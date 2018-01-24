from LogBase import LogBase
import bunyan
import logging
#import sys


class FLHelper(LogBase):

    def __init__(self, projectId, loggerName, jobId, destfile):
        self.projectId = projectId
        self.loggerName = loggerName

        if jobId is None:
            self.jobId = str(self.jobId)
        else:
            self.jobId = str(jobId)

        self.startTime = self.getUTCNow()

        self.logger = logging.getLogger()
        self.filehandler = logging.FileHandler(destfile)
        #logHandler = logging.StreamHandler(stream=sys.stdout)
        #logHandler = logging.StreamHandler(stream=outfile)
        formatter = bunyan.BunyanFormatter()
        self.filehandler.setFormatter(formatter)
        self.logger.addHandler(self.filehandler)


    def logEvent(self, message, severity=None, jobstatus=None, job_id=None, bqJobId=''):

        endTime = self.getUTCNow()
        duration = str(endTime - self.startTime)

        if severity not in ('CRITICAL','ERROR','WARNING','INFO','DEBUG'):
            severity = 'DEBUG'

        self.logger.setLevel(severity)

        otherinfo = dict(jobstatus=jobstatus, job_id=self.jobId, bqJobId=bqJobId, projectId=self.projectId,
                         startTime=self.startTime, endTime=endTime, duration=duration, severity=severity)
        if severity == 'CRITICAL':
            self.logger.critical(message, extra = otherinfo)
        elif severity == 'ERROR':
            self.logger.error(message, extra = otherinfo)
        elif severity == 'WARNING':
            self.logger.warn(message, extra = otherinfo)
        elif severity == 'INFO':
            self.logger.info(message, extra = otherinfo)
        else:
            self.logger.debug(message, extra = otherinfo)

        self.filehandler.close()
        return "entry logged"



