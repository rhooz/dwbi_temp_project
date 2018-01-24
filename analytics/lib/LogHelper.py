from FLHelper import FLHelper

class LogHelper(object):
    """
    factory class for working with various Logging Methods
    """

    def factory(projectId, type, jobId='',destfile=''):
        """
        connect to the applicable database by type
        send in i2ap configuration and database type, the appropriate helper object will be instantiated/returned

        usage example: s = SQLHelper.factory(myConfig, 'postgres')

        :param type: the database type
        :param jobId: the I2AP job Id
        :return: I2AP Helper object
        """
        if type == "filelog":
            return(FLHelper(projectId,"filelog",jobId,destfile))
        else:
            raise TypeError('Invalid logging type')
    factory = staticmethod(factory)