from flask import Flask, jsonify, request
from flask_cors import CORS
from flask.logging import default_handler
from logging.handlers import RotatingFileHandler
from datetime import datetime
import threading
import time
import logging
import blpapi
# import sys

class FlaskApp(Flask):
    def __init__(self, *args, **kwargs):
        super(FlaskApp, self).__init__(*args, **kwargs)
        self.market_data = {}
        self.visitor_ips = {}
        self._activate_background_job()
    
    def _activate_background_job(self):
        def bloomberg_subscribe():
            # Bloomberg API runs on localhost:8194
            options = {'host': 'localhost', 'port': 8194}

            # securities you want data of
            securities = [
                "XAU Curncy",
                "COA Comdty",
                "GI1 Index",
                "SPX INDEX",
                "FSSTI Index",
                "HSI Index",
                "BDIY Index"
            ]

            # fields which you want to get the data of  
            fields = ["LAST_PRICE", "RT_PX_CHG_NET_1D", "RT_PX_CHG_PCT_1D"]
            
            # Fill SessionOptions
            sessionOptions = blpapi.SessionOptions()
            sessionOptions.setServerHost(options['host'])
            sessionOptions.setServerPort(options['port'])

            self.logger.info("Bloomberg API - Connecting to %s:%d" % (options['host'], options['port']))

            # Create a Session
            session = blpapi.Session(sessionOptions)

            # Start a Session
            if not session.start():
                event = session.nextEvent()
                for msg in event:
                    self.logger.error(msg)
                self.logger.error("Failed to start session.")
                return 1
            else:
                self.logger.info("Bloomberg session started")

            if not session.openService("//blp/mktdata"):
                self.logger.error("Failed to open //blp/mktdata")
                return 1
            else: 
                self.logger.info("Opening service to //blp/mktdata")

            # Creating subscriptions to required Bloomberg data
            subscriptions = blpapi.SubscriptionList()
            for sec in securities:
                subscriptions.add(sec,
                                fields,
                                "",
                                blpapi.CorrelationId(sec))

            session.subscribe(subscriptions)

            try:
                # Handle received events in an event loop
                eventCount = 4950
                self.logger.info("Subscription begins")
                while(True):
                    event = session.nextEvent(500)
                    eventCount = (eventCount + 1) % 5001
                    # self.market_data.append({"TIME_RETRIEVED": str(datetime.now())})

                    for msg in event:
                        # NOTE: To print the entire message for debugging:
                        # print("%s - %s" % (msg.correlationIds()[0].value(), msg))
                        if event.eventType() == blpapi.Event.SUBSCRIPTION_DATA and msg.hasElement(fields[0]):
                            
                            d = {}
                            d["TITLE"] = msg.correlationIds()[0].value()
                            d['TIME_RETRIEVED'] = str(datetime.now())

                            for field_name in fields:
                                if (msg.asElement().hasElement(field_name)):
                                    field_value = msg.asElement().getElementAsString(field_name)
                                else:
                                    field_value = 0
                                        
                                if field_name == "LAST_PRICE":
                                    d['LAST_PRICE'] = field_value
                                elif field_name == "RT_PX_CHG_NET_1D":
                                    d['NET_CHANGE'] = field_value
                                elif field_name == "RT_PX_CHG_PCT_1D":
                                    d['PCT_CHANGE'] = field_value
                                else:
                                    d[field_name] = field_value

                            self.market_data[msg.correlationIds()[0].value()] = d

                    # roughly every 10 minutes, otherwise we'll be logging too much
                    if eventCount == 5000:
                        self.logger.info("Data From Bloomberg: " + str(self.market_data))
            except Exception as e:
                self.logger.error(e)
            finally:
                # Stop the session
                self.logger.info("Terminating Bloomberg session")
                session.stop()
                return 1
                
        # Start subscription background task in a new thread
        self.thread = threading.Thread(target=bloomberg_subscribe)
        self.thread.start()

app = FlaskApp(__name__)
CORS(app) #enable CORS

# Set up logging
logger = app.logger
logger.setLevel(logging.DEBUG)

# create 30MB log files, up to 100 log files
file_handler = RotatingFileHandler("log.log", maxBytes=1024 * 1024 * 30, backupCount=100)
file_formatter = logging.Formatter('[%(asctime)s] %(levelname)s in %(module)s: %(message)s')
file_handler.setFormatter(file_formatter)
logger.addHandler(file_handler)

@app.before_request
def before_request(): 
    ip = request.remote_addr

    # logs new IPs visit, and logs when existing IPs visits every 60 times
    if ip in app.visitor_ips:
        app.visitor_ips.update({ip: app.visitor_ips[ip] + 1})
        if app.visitor_ips[ip] == 60:
            app.logger.info("{ip} has requested {method} {path} for 60 times".format(
                method = request.method,
                path = request.path,
                ip = ip))
    else:
        app.visitor_ips.update({ip: 1})
        app.logger.info("A new unique IP has visited: {ip} has requested {method} {path}".format(
            method = request.method,
            path = request.path,
            ip = ip))
    

# Routes
@app.route("/")
def home():
    if not app.thread.isAlive():
        app._activate_background_job()
        return jsonify({"message": "Error, can't retrieve data from Bloomberg API!"+ \
                    " Attempting to re-subscribe, please try again."}), 500 
    if app.market_data:
        return jsonify(app.market_data)
    return jsonify({"message": "An error has been encountered " + \
                "while retrieving data from Bloomberg API!", }), 500