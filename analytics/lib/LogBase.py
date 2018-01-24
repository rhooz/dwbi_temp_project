import uuid
import datetime
import abc
import logging
#from socket import gethostname

class LogBase():
    """
    handle logging activities
    """

    def __init__(self, projectId, loggerName, jobId=''):

        self.projectId = projectId
        self.logId = loggerName
        if jobId == '':
            self.jobId = str(uuid.uuid4())
        else:
            self.jobId = jobId
        self.startTime = self.getUTCNow()

    def getUTCNow(self):
        "return the current UTC time"
        return datetime.datetime.now()
        # return datetime.utcnow().strftime("%Y-%m-%dT%H:%M:%S.%f")[:-4]+'Z'

    def setJobId(self, jobId):
        if jobId != '':
            self.jobId = jobId


    @abc.abstractmethod
    def _logEvent(self, message, severity=None, jobstatus=None, job_id=None, bqJobId=''):
        bodyData = None
        self._logEvent(bodyData, severity=severity)
