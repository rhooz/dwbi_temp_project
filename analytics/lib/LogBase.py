import uuid
import datetime
import abc
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
    def logEvent(self, message, severity=None, jobstatus=None, job_id=None, bqJobId=''):
        # Note:  Caller should override the body data in this function and supply the method for logging
        bodyData = None
        self._logEvent(bodyData, severity=severity)

    @abc.abstractmethod
    def _logEvent(self, bodyData, severity=None):
        # Note:  Caller should supply the method for logging
        bodyData = None
        print bodyData

    @abc.abstractmethod
    def get_entries(self, filterStr, page_size=100):
        """Returns the most recent entries for a given logger subject to the filterString."""
        entries = []
        return entries

    def list_entries(self, filterStr):
        """Prints the most recent entries for a given logger."""
        entries = self.get_entries(filterStr)
        for entry in entries:
           print entry
        return entries
