import json
import datetime
import os
#from GSHelper import GSHelper

class JobState:
    """maintain state for web services"""

    def __init__(self, config, service, jobId, startTime=None, create=False):
        """constructor"""
        self.service = service
        self.jobId = jobId
        self.state = {}
        # evaluate the start time
        if startTime is None:
            self.startTime = str(datetime.datetime.now())
        else:
            self.startTime = startTime
        self.gs = GSHelper(config["project-id"])
        self.stateFile = str(self.jobId) + ".json"
        self.stateFilePath = config["state-bucket"] + "/" + self.service + "/" + self.stateFile
        self.__create = create
        self.exists = False
        self.__pullRecord__()

    def __createRecord__(self):
        """create a new web service state record"""
        self.state["status"] = "NEW"
        self.state["start-time"] = self.startTime
        self.state["end-time"] = ""
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
