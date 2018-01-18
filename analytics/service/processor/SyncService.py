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

from SFHelper import SFHelper
from SDHelper import SDHelper
from JobState import JobState

class SyncService(tornado.web.RequestHandler):
    def initialize(self, config):
        self.jobId = str(uuid.uuid4())
        self.config = config
        self.logger = SDHelper(self.config['project-id'], os.environ['I2AP_LOG_NAME'], jobId=self.jobId)

    def verifyToken(self, clientId, clientSecret):
        if clientId == self.config['client-id'] and clientSecret == self.config['client-secret']:
            return True
        else:
            return False

    def get(self, jobId):
        """get the current state of a given Salesforce sync job job
        Example curl:
            curl -H "Content-Type: application/json" \
                 -H "Tt-I2ap-Id: i2ap-service@tt-cust-analytics.iam.gserviceaccount.com" \
                 -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5" \
                 -X GET \
                 http://localhost:3101/Sync/97607159-5238-4051-9ca3-1503156e3319
        """
        response = {}
        try:
            clientId = self.request.headers['Tt-I2ap-Id']
            clientSecret = self.request.headers['Tt-I2ap-Sec']
            if self.verifyToken(clientId, clientSecret):
                # token is good, run the flow
                try:
                    # retrieve the current status
                    js = JobState(self.config, 'salesforce', jobId)
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
        Performs an bulk sync action on a salesforce object.

        Example curl:
            curl -H "Content-Type: application/json" \
                 -H "Tt-I2ap-Id: i2ap-service@tt-cust-analytics.iam.gserviceaccount.com" \
                 -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5" \
                 -X POST \
                 -d '{"object-name":"Account", "load-type":"TRUNCRELOAD"}' \
                 http://localhost:3101/Sync

        and incremental:
            curl -H "Content-Type: application/json" \
                 -H "Tt-I2ap-Id: i2ap-service@tt-cust-analytics.iam.gserviceaccount.com" \
                 -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5" \
                 -X POST \
                 -d '{"object-name":"Project__c", "load-type":"INCREMENTAL"}' \
                 http://localhost:3101/Sync

        and all sources:
            curl -H "Content-Type: application/json" \
                 -H "Tt-I2ap-Id: i2ap-service@tt-cust-analytics.iam.gserviceaccount.com" \
                 -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5" \
                 -X POST \
                 -d '{"load-type":"TRUNCRELOAD"}' \
                 http://localhost:3101/Sync

        and all sources, dropping and applying schema:
            curl -H "Content-Type: application/json" \
                 -H "Tt-I2ap-Id: i2ap-service@tt-cust-analytics.iam.gserviceaccount.com" \
                 -H "Tt-I2ap-Sec: E8OLhEWWihzdpIz5" \
                 -X POST \
                 -d '{"load-type":"DROPREPLACE"}' \
                 http://localhost:3101/Sync
        """

        # some dictionaries to hold the response messages
        response = {'status-message': "unknown",
                    'pod': gethostname(),
                    "job-id": self.jobId}

        # pull in the query parameters
        try:
            params = json.loads(self.request.body)
        except:
            params = {}
        # pull in the object name
        try:
            objectName = params['object-name']
            allObjects = False
        except:
            objectName = ''
            allObjects = True
        # pull in the load type
        try:
            loadType = params['load-type']
        except:
            loadType = 'TRUNCRELOAD'

        if loadType == 'INCREMENTAL':
            incremental = True
        else:
            incremental = False

        if allObjects:
            msg = 'i2ap/Processor: Received request to process object sync for all objects on pod: ' + gethostname()
        else:
            msg = 'i2ap/Processor: Received request to process object sync for: ' + str(objectName) + ' on pod: ' + gethostname()
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
                    js = JobState(self.config, 'salesforce', self.jobId, create=True)
                    sf = SFHelper(self.config, objectName, jobId=self.jobId, maintainState=True, incremental=incremental)
                except Exception as dataErr:
                    msg = 'i2ap/Processor: Failure in SalesForce declaration on pod: ' + gethostname() + ' Details: ' + str(dataErr)
                    logging.error(msg)
                    self.logger.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                    response['status-message'] = msg
                    response['status'] = '500 Internal Server Error'
                    self.set_status(500)
                else:
                    if allObjects:
                        logging.info('Executing sync for all objects')
                    else:
                        logging.info('Executing sync for: ' + objectName)
                    try:
                        js.changeStatus('INPROGRESS')
                        if allObjects:
                            if loadType == 'DROPREPLACE':
                                tornado.ioloop.IOLoop.current().spawn_callback(sf.updateAllTables)
                            else:
                                tornado.ioloop.IOLoop.current().spawn_callback(sf.syncAllTables)
                            msg = 'i2ap/Processor.sync: Accepted sync request (' + loadType + ') for all objects on pod: ' + gethostname() + '.'
                        else:
                            tornado.ioloop.IOLoop.current().spawn_callback(sf.syncTable)
                            msg = 'i2ap/Processor.sync: Accepted sync request for: ' + objectName + ' on pod: ' + gethostname() + '.'
                        logging.info(msg)
                        self.logger.logEvent(msg, severity='INFO', jobstatus='INPROGRESS')
                        response['status-message'] = msg
                        response['status'] = '202 Accepted'
                        self.set_status(202)
                    except Exception as dataErr:
                        msg = 'i2ap/Processor.sync: Failure in syncing salesforce object: ' + objectName + ' on pod: ' + \
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
        msg = 'Finishing up sync request for: ' + objectName + \
            '. sending response: ' + json.dumps(response) + ' from pod: ' + gethostname()
        logging.info(msg)
        self.logger.logEvent(msg, severity='INFO', jobstatus='END')
        self.finish()
