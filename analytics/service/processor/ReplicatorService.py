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
from Replicate import Replicate

class ReplicatorService(tornado.web.RequestHandler):
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

        curl  -H "Tt-I2ap-Id: LJASFdPAFJ#DSFFSD##" -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5"  "http://localhost:3101/Replicate?object-name=sf_Account&source-project=tectonic-analytics&source-dataset=tectonic_commix&dest-project=tt-cust-analytics&dest-dataset=commix_stage&disposition=WRITE_TRUNCATE"
        curl  -H "Tt-I2ap-Id: i2ap-service@tt-cust-analytics.iam.gserviceaccount.com" -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5"  "https://processor.analytics.tectonic-cloud.com/Replicate?object-name=sf_Account&source-project=tectonic-analytics&source-dataset=tectonic_commix&dest-project=tt-cust-analytics&dest-dataset=commix_stage&disposition=WRITE_TRUNCATE"

        """
        # some dictionaries to hold the response messages
        response = {'status-message': "unknown", 'pod': gethostname()}
        body = {}
        objectName = self.get_argument('object-name')
        sourceProject = self.get_argument('source-project')
        sourceDataset = self.get_argument('source-dataset')
        destProject = self.get_argument('dest-project')
        destDataset = self.get_argument('dest-dataset')

        try:
            disposition = self.get_argument('disposition')
        except:
            disposition = 'WRITE_TRUNCATE'

        msg = 'i2ap/Replicator: Received request to replicate table: ' + str(objectName) + \
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
                    rep = Replicate(self.config, objectName, sourceProject, sourceDataset, destProject, destDataset,
                                    disposition=disposition, jobId=self.jobId)
                except Exception as dataErr:
                    msg = 'i2ap/Replicator: Failure in Transform declaration on pod: ' + gethostname() + ' Details: ' + str(dataErr)
                    logging.error(msg)
                    self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                    response['status-message'] = msg
                    response['status'] = '500 Internal Server Error'

                else:
                    logging.info('Executing table replication: ' + objectName)
                    try:
                        tornado.ioloop.IOLoop.current().spawn_callback(rep.process)
                        msg = 'i2ap/Replicator.process: Table Replication request for: ' + objectName + ' on pod: ' + gethostname() + '.'
                        logging.info(msg)
                        self.logger.logEvent(msg, severity='INFO', jobstatus='INPROGRESS')
                        response['status-message'] = msg
                        response['status'] = '202 Accepted'
                    except Exception as dataErr:
                        msg = 'i2ap/Replicator.process: replicating: ' + objectName + ' on pod: ' + \
                              gethostname() + '. Details: ' + str(dataErr)
                        logging.error(msg)
                        self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                        response['status-message'] = msg
                        response['status'] = '500 Internal Server Error'
            else:
                msg = 'i2ap/Replicator: Failure; Invalid origination'
                logging.error(msg)
                self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                response['status-message'] = msg
                response['status'] = '500 Internal Server Error'
        except Exception as dataErr:
            msg = 'i2ap/Replicator: Failure; Invalid origination'
            logging.error(msg)
            self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
            response['status-message'] = msg
            response['status'] = '500 Internal Server Error'

        # return a response to the consumer
        response['body'] = body
        self.write(json.dumps(response))

        msg = 'Finishing up table replication for: ' + objectName + \
            '. sending response: ' + json.dumps(response) + ' from pod: ' + gethostname()
        logging.info(msg)
        self.logger.logEvent(msg, severity='INFO', jobstatus='END')
        self.finish()
