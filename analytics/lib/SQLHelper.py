#!/usr/bin/env python
from BQHelper import BQHelper
from ORHelper import PGHelper

class SQLHelper(object):
    """
    factory class for working with various SQL oriented databases
    """

    def factory(config, type, jobId=''):
        """
        connect to the applicable database by type
        send in i2ap configuration and database type, the appropriate helper object will be instantiated/returned

        usage example: s = SQLHelper.factory(myConfig, 'postgres')

        :param config: i2ap configuration object
        :param type: the database type
        :param jobId: the I2AP job Id
        :return: I2AP Helper object
        """
        if type == "bigquery":
            return(BQHelper(config, jobId))
        elif type == "postgres":
            return(PGHelper(config, jobId))
        else:
            raise TypeError('Invalid datatabse type')
    factory = staticmethod(factory)

