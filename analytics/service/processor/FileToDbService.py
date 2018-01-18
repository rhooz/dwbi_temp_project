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

class FileToDbService(tornado.web.RequestHandler):
    def initialize(self, config):
        self.jobId = str(uuid.uuid4())
        self.config = config
        self.logger = SDHelper(self.config['project-id'], 'processing-log', jobId=self.jobId)

    def verifyToken(self, clientId, clientSecret):
        if clientId == self.config['client-id'] and clientSecret == self.config['client-secret']:
            return True
        else:
            return False

    @gen.coroutine
    def get(self):
        """
        Performs an action on a customer input file.

        Example curl:
            curl "http://localhost:3101/Process?object-name=test&filename=test-20171101T131313123461.csv&disposition=WRITE_TRUNCATE"
        """
        # some dictionaries to hold the response messages
        response = {'status-message': "unknown", 'pod': gethostname()}
        body = {}
        objectName = self.get_argument('object-name')
        filename = self.get_argument('filename')
        try:
            disposition = self.get_argument('disposition')
        except:
            disposition = 'WRITE_APPEND'

        msg = 'i2ap/Processor: Received request to process file: ' + str(filename) + \
              ' on pod: ' + gethostname()
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
                    etl = Transform(self.config, objectName, fileName=filename, disposition=disposition, jobId=self.jobId)
                except Exception as dataErr:
                    msg = 'i2ap/Processor: Failure in Transform declaration on pod: ' + gethostname() + ' Details: ' + str(dataErr)
                    logging.error(msg)
                    self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                    response['status-message'] = msg
                    response['status'] = '500 Internal Server Error'

                else:
                    logging.info('Executing file import: ' + filename)
                    try:
                        tornado.ioloop.IOLoop.current().spawn_callback(etl.fileToDb)
                        msg = 'i2ap/Processor.process: Accepted file load request for: ' + filename + ' on pod: ' + gethostname() + '.'
                        logging.info(msg)
                        self.logger.logEvent(msg, severity='INFO', jobstatus='INPROGRESS')
                        response['status-message'] = msg
                        response['status'] = '202 Accepted'
                    except Exception as dataErr:
                        msg = 'i2ap/Processor.process: Failure in pulling file: ' + filename + ' on pod: ' + \
                              gethostname() + '. Details: ' + str(dataErr)
                        logging.error(msg)
                        self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                        response['status-message'] = msg
                        response['status'] = '500 Internal Server Error'
            else:
                msg = 'i2ap/Processor: Failure; Invalid origination'
                logging.error(msg)
                self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                response['status-message'] = msg
                response['status'] = '500 Internal Server Error'
        except Exception:
            msg = 'i2ap/Processor: Failure; Invalid origination'
            logging.error(msg)
            self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
            response['status-message'] = msg
            response['status'] = '500 Internal Server Error'

        # return a response to the consumer
        response['body'] = body
        self.write(json.dumps(response))
        msg = 'Finishing up file load for: ' + filename + \
            '. sending response: ' + json.dumps(response) + ' from pod: ' + gethostname()
        logging.info(msg)
        self.logger.logEvent(msg, severity='INFO', jobstatus='END')
        self.finish()
