from FLHelper import FLHelper
from SDHelper import SDHelper

class LogHelper(object):
    """
    factory class for working with various Logging Methods
    """

    def factory(projectId, type, jobId='',destfile=''):
        """
        connect to the applicable logger by type
        send in logger type, the appropriate helper object will be instantiated/returned

        usage example: s = LogHelper.factory(projectId=20180125, type="filelog",jobId=1,destfile=destfile)

        :param projectId: the project type
        :param type: the logger type (filelog or
        :param jobId: the job id
        :param destfile: the destination location for the log file

        :return: I2AP Helper object
        """
        if type == "filelog":
            return(FLHelper(projectId,"filelog",jobId,destfile))
        elif type == "stackdriver":
            return(SDHelper(projectId,"stackdriver",jobId))
        else:
            raise TypeError('Invalid logging type')
    factory = staticmethod(factory)