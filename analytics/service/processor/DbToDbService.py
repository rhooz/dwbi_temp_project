import tornado.ioloop
import tornado.web
from tornado import gen
import uuid
from socket import gethostname
import logging
import json
import os
import sys

class_path = os.environ['I2AP_LIB_DIRECTORY']
sys.path.append(class_path)

from SDHelper import SDHelper
from Transform import Transform
from JobState import JobState

class DbToDbService(tornado.web.RequestHandler):
    def initialize(self, config):
        self.jobId = str(uuid.uuid4())
        self.config = config
        self.logger = SDHelper(self.config['project-id'], 'processing-log', jobId=self.jobId)

    def verifyToken(self, clientId, clientSecret):
        if clientId == self.config['client-id'] and clientSecret == self.config['client-secret']:
            return True
        else:
            return False

    def get(self, jobId):
        """get the current state of a given Db-to-db job

        Example curl:
            curl -H "Content-Type: application/json" \
                 -H "Tt-I2ap-Id: i2ap-service@tt-cust-analytics.iam.gserviceaccount.com" \
                 -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5" \
                 -X GET
                 http://localhost:3101/DbToDb/97607159-5238-4051-9ca3-1503156e3319
        """
        response = {}
        try:
            clientId = self.request.headers['Tt-I2ap-Id']
            clientSecret = self.request.headers['Tt-I2ap-Sec']
            if self.verifyToken(clientId, clientSecret):
                # token is good, run the flow
                try:
                    # retrieve the current status
                    js = JobState(self.config, 'processor', jobId)
                    # assess the state... NEW means this is the first time job_id has ever been seen
                    if js.state["status"] == 'NEW':
                        response = {"status-message": "Not Found",
                                    "status": "404 Not Found",
                                    "pod": gethostname(),
                                    "job-id": jobId,
                                    "service": js.service}
                        self.set_status(404)
                    else:
                        response = {"status-message": "Ok",
                                    "status": "200 OK",
                                    "pod": gethostname(),
                                    "job-id": jobId,
                                    "service": js.service,
                                    "state-file-path": js.stateFilePath,
                                    "job-status": js.state["status"],
                                    "start-time": js.state["start-time"],
                                    "end-time": js.state["end-time"]}
                        self.set_status(200)
                except Exception as checkError:
                    msg = 'i2ap/Processor.process: Failure in pulling job status. Details: ' + str(checkError)
                    logging.error(msg)
                    response['status-message'] = msg
                    response['status'] = '500 Internal Server Error'
                    self.set_status(500)
            else:
                msg = 'i2ap/Processor: Failure; Invalid origination'
                logging.error(msg)
                response['status-message'] = msg
                response['status'] = '500 Internal Server Error'
                self.set_status(500)
        except Exception:
            msg = 'i2ap/Processor: Failure; Invalid origination'
            logging.error(msg)
            response['status-message'] = msg
            response['status'] = '500 Internal Server Error'
            self.set_status(500)

        self.set_header("Content-type", "application/json")
        self.write(json.dumps(response))
        self.finish()

    @gen.coroutine
    def post(self):
        """
        Performs an action on a customer input file.

        Example curl:
            curl -H "Content-Type: application/json" \
                 -H "Tt-I2ap-Id: i2ap-service@tt-cust-analytics.iam.gserviceaccount.com" \
                 -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5" \
                 -X POST
                 -d '{"object-name":"test","query-params":[{"key":"CUSTOMER_GROUP","value":"17"},{"key":"FROM_DATE","value":"2017-11-01"}]}' \
                 http://localhost:3101/DbToDb
        """
        # some dictionaries to hold the response messages
        response = {'status-message': "unknown",
                    'pod': gethostname(),
                    "job-id": self.jobId}

        # pull in the query parameters
        params = json.loads(self.request.body)
        objectName = params['object-name']
        try:
            disposition = params['disposition']
        except:
            disposition = 'WRITE_APPEND'

        msg = 'i2ap/Processor: Received request to process query: ' + str(objectName) + ' on pod: ' + gethostname()
        logging.info(msg)
        self.logger.logEvent(msg, severity='INFO', jobstatus='START')
        # check for well-formed header
        try:
            clientId = self.request.headers['Tt-I2ap-Id']
            clientSecret = self.request.headers['Tt-I2ap-Sec']
            if self.verifyToken(clientId, clientSecret):
                # token is good, run the flow
                try:
                    # instantiate the Doubleclick helper class
                    js = JobState(self.config, 'processor', self.jobId)
                    etl = Transform(self.config, objectName, params=params['query-params'], disposition=disposition, jobId=self.jobId, maintainState=True)
                except Exception as dataErr:
                    msg = 'i2ap/Processor: Failure in Transform declaration on pod: ' + gethostname() + ' Details: ' + str(dataErr)
                    logging.error(msg)
                    self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                    response['status-message'] = msg
                    response['status'] = '500 Internal Server Error'
                    self.set_status(500)
                else:
                    logging.info('Executing query to import for: ' + objectName)
                    try:
                        js.changeStatus('INPROGRESS')
                        tornado.ioloop.IOLoop.current().spawn_callback(etl.dbToDb)
                        msg = 'i2ap/Processor.process: Accepted query load request for: ' + objectName + ' on pod: ' + gethostname() + '.'
                        logging.info(msg)
                        self.logger.logEvent(msg, severity='INFO', jobstatus='INPROGRESS')
                        response['status-message'] = msg
                        response['status'] = '202 Accepted'
                        self.set_status(202)
                    except Exception as dataErr:
                        msg = 'i2ap/Processor.process: Failure in pulling data for query: ' + objectName + ' on pod: ' + \
                              gethostname() + '. Details: ' + str(dataErr)
                        logging.error(msg)
                        self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                        response['status-message'] = msg
                        response['status'] = '500 Internal Server Error'
                        self.set_status(500)
            else:
                msg = 'i2ap/Processor: Failure; Invalid origination'
                logging.error(msg)
                self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                response['status-message'] = msg
                response['status'] = '500 Internal Server Error'
                self.set_status(500)
        except Exception:
            msg = 'i2ap/Processor: Failure; Invalid origination'
            logging.error(msg)
            self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
            response['status-message'] = msg
            response['status'] = '500 Internal Server Error'
            self.set_status(500)

        # return a response to the consumer
        self.set_header("Content-type", "application/json")
        self.write(json.dumps(response))
        msg = 'Finishing up query load for: ' + objectName + \
            '. sending response: ' + json.dumps(response) + ' from pod: ' + gethostname()
        logging.info(msg)
        self.logger.logEvent(msg, severity='INFO', jobstatus='END')
        self.finish()
