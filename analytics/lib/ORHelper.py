#!/usr/bin/env python

from SQLBase import SQLBase

class ORHelper(SQLBase):
    """
    handle database activities with Postgres
    """
    def convertType(self, genericType):
        """
        Convert the generic I2AP type into a database specific type
        :param genericType: the generic I2AP type
        :return: the database specifc type
        """
        if genericType.upper() == 'STRING':
            returnType = 'NVARCHAR2(4000)'
        elif genericType.upper() == 'INTEGER':
            returnType = 'INTEGER'
        elif genericType.upper() == 'FLOAT':
            returnType = 'FLOAT'
        elif genericType.upper() == 'DATE':
            returnType = 'DATE'
        elif genericType.upper() == 'DATETIME':
            returnType = 'TIMESTAMP'
        elif genericType.upper() == 'TIMESTAMP':
            returnType = 'TIMESTAMP'
        else:
            returnType = 'NVARCHAR2(4000)'
        return returnType

    # def createDataset(self, datasetId):
    #     """
    #     Create a schema in the existing database
    #     :param datasetId: the name of the schema to be created
    #     """
    #     sql = 'CREATE SCHEMA ' + self.dataset + ';'
    #     self._createDataset(datasetId, sql)

    # def getDataset(self, datasetId):
    #     """
    #     Pull metadata and other information about a given schema
    #     :param datasetId: the name of the schema to query
    #     :return: dictionary of dataset attributes
    #     """
    #     sql = 'SELECT catalog_name AS database, schema_name AS name, schema_owner AS owner ' + \
    #           'FROM information_schema.schemata ' + \
    #           'WHERE schema_name = \'' + self.dataset + '\''
    #     return self._getDataset(datasetId, sql)

    # def deleteDataset(self, datasetId):
    #     """
    #     Delete the specified schema (and all contained tables) from the database
    #     :param datasetId: the name of the schema to delete
    #     """
    #     sql = 'DROP SCHEMA IF EXISTS ' + self.dataset + ' CASCADE;'
    #     self._deleteDataset(sql)

    # def deleteTable(self, tableName):
    #     """
    #     Delete a table that exists in the database
    #     :param tableName: the name of the table to delete
    #     :param optional: datasetId: the name of the schema containing the table to delete
    #     """
    #     sql = 'DROP TABLE IF EXISTS ' + self.dataset + '.' + tableName + ' CASCADE;'
    #     self._deleteTable(tableName, sql)
    #
    # def getTable(self, tableName):
    #     """
    #     get metadata information on a given table
    #     :param tableName: the name of the table to delete
    #     :param optional: datasetId: the name of the schema containing the table to delete
    #     :return: dictionary
    #     """
    #     sql = 'SELECT info.database, info.schema, info.name, info.type, count.row_count ' + \
    #           'FROM ' + \
    #           '(SELECT table_catalog AS database, table_schema AS schema, table_type AS type, table_name AS name ' + \
    #           'FROM information_schema.tables ' + \
    #           'WHERE table_name = \'' + tableName + '\' and table_schema = \'' + self.dataset + '\') AS info ' + \
    #           'CROSS JOIN ' + \
    #           '(SELECT count(*) row_count FROM ' + self.dataset + '.' + tableName + ') AS count'
    #
    #     return self._getTable(sql)
    #
    # def copyTable(self, sourceTableName, destTableName):
    #     """
    #     Copy the contents of one table into another (of the same schema)
    #     :param sourceTableName: The source table to be copied
    #     :param destTableName: The destination table (to be pasted)
    #     :return:
    #     """
    #     sql = 'SELECT * INTO ' + self.dataset + '.' + destTableName + ' FROM ' + self.dataset + '.' + sourceTableName + ';'
    #     self._copyTable(sourceTableName, destTableName, sql)
    #
    # def truncateTable(self, tableName):
    #     """
    #     Remove the current contents of a table in the database
    #     :param tableName: the name of the table to delete
    #     :param optional: datasetId: the name of the schema containing the table to delete
    #     """
    #     sql = 'TRUNCATE TABLE ' + self.dataset + '.' + tableName + ' CASCADE;'
    #     self._truncateTable(tableName, sql)
    #
    # def deleteView(self, tableName):
    #     """
    #     Delete a view that exists in the database
    #     :param tableName: the name of the table to delete
    #     :return:
    #     """
    #     sql = 'DROP VIEW IF EXISTS ' + self.dataset + '.' + tableName + ' CASCADE;'
    #     self._deleteView(tableName, sql)
