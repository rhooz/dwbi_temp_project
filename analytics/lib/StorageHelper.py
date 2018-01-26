#!/usr/bin/env python
from GSHelper import GSHelper
from MIHelper import MIHelper
from FTPHelper import FTPHelper

class StorageHelper(object):
    """
    factory class for working with various object storage technologies
    """

    def factory(config, type='', jobId=''):
        """
        connect to the applicable object store by type
        send in i2ap configuration and storage type, the appropriate helper object will be instantiated/returned

        usage example: s = StorageHelper.factory(myConfig, 'googlestorage')

        :param config: i2ap configuration object
        :param type: the object storage type
        :param jobId: the I2AP job Id
        :return: I2AP Helper object
        """
        if type == '':
            type = config['storage-type']

        if type == "googlestorage":
            return(GSHelper(config, jobId))
        elif type == "minio":
            return(MIHelper(config, jobId))
        elif type == "sftp":
            return (FTPHelper(config, jobId))
        else:
            raise TypeError('Invalid storage type')
    factory = staticmethod(factory)

