import json
import datetime
import os
from StorageHelper import StorageHelper

class JobState:
    """maintain state for web services"""

    def __init__(self, config, service, jobId, startTime=None, create=False):
        """constructor"""
        self.service = service
        self.config = config
        self.jobId = jobId
        self.state = {}
        # evaluate the start time
        if startTime is None:
            self.startTime = str(datetime.datetime.now())
        else:
            self.startTime = startTime
        self.gs = StorageHelper.factory(self.config, jobId=self.jobId)
        self.gs.setBucket(config["state-bucket"])
        self.stateFile = str(self.jobId) + ".json"
        self.stateFilePath = self.service + "/" + self.stateFile
        self.__create = create
        self.exists = False
        self.__pullRecord__()

    def __createRecord__(self):
        """create a new web service state record"""
        self.state["status"] = "NEW"
        self.state["start-time"] = self.startTime
        self.state["end-time"] = ""
        self.state["children"] = []
        self.state['child-status'] = 'NA'
        self.state['child-service'] = ''
        # if the intent is to create a record and it is not there, push the "NEW" state
        if self.__create and not self.exists:
            self.__pushRecord__()

    def __pushRecord__(self):
        """push the revised state to google storage for persistence"""
        # persist and upload the state
        with open(self.stateFile, "w") as outfile:
            json.dump(self.state, outfile)
        self.gs.uploadFile(self.stateFile, self.stateFilePath)
        # cleanup
        os.remove(self.stateFile)

    def __pullRecord__(self):
        """pull the status/log record from storage"""
        try:
            self.state = json.loads(self.gs.streamFile(self.stateFilePath))
            self.exists = True
        except:
            # this is a new job, nothing is out there, so create a record
            self.__createRecord__()

        if len(self.state['children']) > 0:
            # default to COMPLETE
            self.state['child-status'] = 'COMPLETE'
            # loop through and get the status until ERROR or a NOT COMPLETE status is found
            if self.state['child-service'] == '':
                childService = self.service
            else:
                childService = self.state['child-service']

            for childId in self.state['children']:
                childJs = JobState(self.config, childService, childId)
                if childJs.state['status'] == 'ERROR':
                    self.state['child-status'] = 'ERROR'
                    break
                if childJs.state['status'] == 'NEW' or childJs.state['status'] == 'INPROGRESS':
                    self.state['child-status'] = 'INPROGRESS'
                    break

    def updateStatus(self):
        """for long persisting object, allow the caller to update the state"""
        self.__pullRecord__()

    def changeStatus(self, status, endTime=None):
        """change the status to something new"""
        self.state["status"] = status
        if endTime is not None:
            self.state["end-time"] = endTime
        else:
            if status == "COMPLETE" or status == "ERROR":
                self.state['end-time'] = str(datetime.datetime.now())
        # persist the new state
        self.__pushRecord__()

    def addChild(self, jobId):
        """
        add/assoicate another job id
        :param jobId: i2ap unique job identifier (uuid)
        """
        self.state['children'].append(jobId)

    def setChildService(self, service):
        """
        set the child service type as it may differ from the parent's
        :param service: name of i2ap job/service
        """
        self.state['child-service'] = service

    def status(self):
        """
        returned combined job and child status
        :return: i2ap job status
        """
        if self.state['status'] == 'COMPLETE' and self.state['child-status'] == 'COMPLETE':
            return 'COMPLETE'
        elif self.state['status'] == 'ERROR' or self.state['child-status'] == 'ERROR':
            return 'ERROR'
        else:
            return 'INPROGRESS'