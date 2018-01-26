from LogBase import LogBase
from google.cloud import logging
from socket import gethostname
import uuid

class SDHelper(LogBase):
    "methods to simplify interaction with the Stackdriver logging service"

    def __init__(self, projectId, loggerName, jobId=''):
        """constructor to supply Google cloud project id and the name of the logging entity (loggerName) to log events"""
        # constrain to the GCP project
        self.projectId = projectId
        self.logId = loggerName
        # Construct the service object for interacting with the BigQuery API.
        self.client = logging.Client()
        if jobId == '':
            self.jobId = str(uuid.uuid4())
        else:
            self.jobId = jobId
        self.startTime = self.getUTCNow()
        self.logger = self.client.logger(self.logId)

    def logEvent(self, message, severity=None, jobstatus=None, job_id=None, bqJobId=''):
        "generate a logging event"
        if severity not in ('DEFAULT', 'NOTICE', 'ALERT', 'EMERGENCY', 'ERROR', 'WARNING', 'CRITICAL', 'INFO', 'DEBUG'):
            severity = 'DEFAULT'
        if job_id is None:
            jobProcessId = str(self.jobId)
        else:
            jobProcessId = str(job_id)
        try:
            podName = gethostname()
        except:
            podName = ''
        logname = 'projects/' + self.projectId + '/logs/' + self.logId
        endTime = self.getUTCNow()
        duration = str(endTime - self.startTime)

        # generate a new event message to send to the service
        bodyData = {
            'logName': logname,
            'textPayload': message,
            'severity': severity,
            'processid': jobProcessId,
            'processStartDateTime': str(self.startTime),
            'processEndDateTime': str(self.getUTCNow()),
            'jobstatus': jobstatus,
            'duration': duration,
            'bqJobId': bqJobId,
            'podId': podName
        }
        """
        This structure will be used for Error Reporting
        bodyErrorData = {
                         "serviceContext": {
                         "service": string,     // Required.
                                            },
                         "message": bodyData,       // Required. Should contain the full exception
                                                 // message, including the stack trace.
                         "context": {
                         "httpRequest": {
                                     "method": string,
                                     "url": string,
                                     "userAgent": string,
                                     "referrer": string,
                                     "responseStatusCode": number,
                                     "remoteIp": string
                                    },
                         "user": string,
                         "reportLocation": {    // Required if no stack trace in 'message'.
                         "filePath": string,
                         "lineNumber": number,
                         "functionName": string
                          }
                         }
                        }
        """
        self.logger.log_struct(bodyData, severity=severity)
        return "entry logged"

    def get_entries(self, filterStr, page_size=100):
        """Returns the most recent entries for a given logger."""
        self.logger = self.client.logger(self.logId)
        entries = []
        page_token = self._collectEntries(entries, filterStr, page_size)
        while page_token is not None:
            page_token = self._collectEntries(entries, filterStr, page_size, page_token)
        return entries

    def _collectEntries(self, entries, filterStr, page_size, page_token=None):
        """Retrieves entries from a given logger subject to the specified filter."""
        list_entries = self.logger.list_entries(order_by=logging.DESCENDING, filter_=filterStr, page_token=page_token, page_size=page_size)
        for entry in list_entries:
            entries.append(entry)
        return list_entries.next_page_token

    def list_entries(self, filterStr):
        """Prints the most recent entries for a given logger."""
        entries = self.get_entries(filterStr)
        for entry in entries:
            timestamp = entry.timestamp.isoformat()
            print('* {}: {}'.format
                  (timestamp, entry.payload))
        return entries
