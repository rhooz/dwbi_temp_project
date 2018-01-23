#!/usr/bin/env python

import uuid
import logging
import os
import pandas as pd
import json
import abc

from Connection import Connection
# from GSHelper import GSHelper
# from SDHelper import SDHelper

class SQLBase:
    """
    handle activities in relational databases
    """

    def __init__(self, config, jobId=''):
        """
        constructor
        :param config: i2ap config object
        :param jobId: optional: i2ap job id for logging consistency

        config = { 'project-id': '',
                   'database-type': '',
                   'sql-directory': '',
                   'ddl-directory': '',
                   'dataset': '',
                   'log-name': '',
                   'stage-bucket': '',
                   'database-server': '',
                   'database-user': '',
                   'database-password': '',
                   'database': '',
                   'database-sid': '' }
        """

        self.config = config
        self.projectId = config['project-id'] if 'project-id' in config.keys() else None
        self.dbType = config['database-type'] if 'database-type' in config.keys() else None
        self.sqlDir = config['sql-directory'] if 'sql-directory' in config.keys() else None
        self.ddlDir = config['ddl-directory'] if 'ddl-directory' in config.keys() else None

        # TODO: need to override createDataset (schema)
        # we currently don't have schemas in our Oracle DB but I assume that will change soon.
        # need to ensure the dataset works Oracle in every use case
        self.dataset = config['dataset'] if 'dataset' in config.keys() else ''
        self.logName = config['log-name'] if 'log-name' in config.keys() else None
        self.bucket =  config['stage-bucket'] if 'stage-bucket' in config.keys() else None
        self._dbConfig()

        if jobId == '':
            self.jobId = uuid.uuid4()
        else:
            self.jobId = jobId

        if self.projectId:
            self.gs = GSHelper(self.projectId)
            self.sd = SDHelper(self.projectId, 'salesforce-log', self.jobId)
        else:
            # TODO: add integration for any bucket system
            self.gs = None
            # TODO: add integration for any logging system
            self.sd = None

    @abc.abstractmethod
    def _dbConfig(self):
        """
        database or type specific connections
        """
        self.connection = Connection.factory(self.config, self.dbType)

    def _execDML(self, sql, caller=None):
        """
        execute a DML statement to act on a database
        :param sql: DML statement to execute
        :param caller: the calling function for error messages
        """
        if sql > '':
            try:
                cur = self.connection.cursor()
                cur.execute(sql)
                cur.close()
                self.connection.commit()
            except Exception as dbErr:
                if caller is None:
                    caller = '_execDML'
                msg = 'SQLBase.' + caller + ': Error executing the DML statement \'' + sql + '\'. Details: ' + str(dbErr)
                logging.error(msg)
                raise StandardError(msg)
        else:
            raise StandardError('SQLBase._execDML: No SQL was passed to execute')

    @abc.abstractmethod
    def execDML(self, sql):
        """
        execute a DML statement to act on a database
        :param sql: DML statement to execute
        """
        self._execDML(sql, 'execDML')

    @abc.abstractmethod
    def convertType(self, genericType):
        """
        Convert the generic I2AP type into a database specific type
        :param genericType: the generic I2AP type
        :return: the database specifc type
        """
        if genericType.upper() == 'STRING':
            returnType = 'STRING'
        elif genericType.upper() == 'INTEGER':
            returnType = 'INTEGER'
        elif genericType.upper() == 'FLOAT':
            returnType = 'FLOAT'
        elif genericType.upper() == 'DATE':
            returnType = 'DATE'
        elif genericType.upper() == 'TIMESTAMP':
            returnType = 'TIMESTAMP'
        else:
            returnType = 'STRING'
        return returnType

    def _createDataset(self, datasetId, sql):
        """
        Create a schema in the existing database
        :param datasetId: the name of the schema to be created
        :param sql: the database specific SQL to run to create the dataset
        """
        if datasetId is not None:
            self.dataset = datasetId

        self._execDML(sql, 'createDataset')
        logging.info('BaseSQL.createDataset: Successfully created dataset: ' + self.dataset)

    @abc.abstractmethod
    def createDataset(self, datasetId):
        """
        Create a schema in the existing database
        :param datasetId: the name of the schema to be created
        """
        # Note caller should override the sql in this function
        sql = None
        self._createDataset(datasetId, sql)

    def _deleteDataset(self, sql):
        """
        Delete the specified schema (and all contained tables) from the database
        :param sql: statement to delete the dataset
        """
        if sql is None:
            raise TypeError('SQLBase.deleteDataset: Invalid method override, sql must be provided')

        self._execDML(sql, 'deleteDataset')
        logging.info('BaseSQL.deleteDataset: Successfully deleted dataset: ' + self.dataset)

    @abc.abstractmethod
    def deleteDataset(self, datasetId):
        """
        Delete the specified schema (and all contained tables) from the database
        :param datasetId: the name of the schema to delete
        """
        # Note caller should override the sql in this function
        sql = None
        self._deleteDataset(sql)

    def _getDataset(self, datasetId, sql):
        """
        Pull metadata and other information about a given schema
        :param datasetId: the name of the schema to query
        :param sql: the database specific sql to retrieve the dataset metadata
        :return: dictionary of dataset attributes
        """

        if datasetId is not None:
            self.dataset = datasetId

        if sql is None:
            raise TypeError('SQLBase.deleteDataset: Invalid method override, sql must be provided')

        try:
            d = self.getQueryAsDict(sql)
            if d == []:
                datasetInfo = {}
                datasetInfo['found'] = 0
            else:
                datasetInfo = d[0]
                datasetInfo['found'] = 1
            return datasetInfo
        except Exception as dbErr:
            if 'does not exist' in str(dbErr):
                datasetInfo = {}
                datasetInfo['found'] = 0
                return datasetInfo
            else:
                raise StandardError(str(dbErr))

    @abc.abstractmethod
    def getDataset(self, datasetId):
        """
        Pull metadata and other information about a given schema
        :param datasetId: the name of the schema to query
        :return: dictionary of dataset attributes
        """
        # Note caller should override the sql in this function
        # The expected result should return database, name, and owner attributes
        sql = None
        return self._getDataset(datasetId, sql)

    def checkDatasetExist(self, datasetId):
        """
        Check if the schema exists in the database
        :param datasetId: the name of the schema to check
        :param destProject: deprecate
        :return: boolean
        """
        info = self.getDataset(datasetId)
        if info['found'] == 0:
            return False
        else:
            return True

    def _createTable(self, tableName, sql, shard=False, recreate=False):
        """
        Create a new table in a db schema
        :param tableName: the name of the table to delete
        :param schema: the ddl or "schema" definition of the table
        :param sql: the sql dml statement describing how to create the table
        :param shard: boolean: should the table be partitioned?
        :param recreate: boolean: should the table be dropped and recreated?
        """

        # dump the table if we are recreating
        if recreate:
            self.deleteTable(tableName)

        # execute the statement against the database
        self._execDML(sql, 'createTable')
        logging.info('BaseSQL.createTable: Successfully created table: ' + tableName)

    @abc.abstractmethod
    def createTable(self, tableName, schema, shard=False, recreate=False):
        """
        Create a new table in a db schema
        :param tableName: the name of the table to delete
        :param schema: the ddl or "schema" definition of the table
        :param sql: the sql dml statement describing how to create the table
        :param shard: boolean: should the table be partitioned?
        :param recreate: boolean: should the table be dropped and recreated?
        """
        # Note caller MAY need override the sql in this function; ANSI standard provided here
        sql = 'CREATE TABLE ' + self.dataset + '.' + tableName + ' ('
        # build a DML statement to create the table by looping through the i2ap schema list dictionary
        needComma = False
        for column in schema:
            if needComma:
                sql += ', '
            sql += column['name'] + ' ' + self.convertType(column['type'])
            needComma = True
        sql += ')'
        self._createTable(tableName, sql, shard=shard, recreate=recreate)

    @abc.abstractmethod
    def createTableAs(self, tableName, query=None, fileName=None, recreate=False):
        """
        Create a new table from a query
        :param tableName: the name of the view to create
        :param query: optional: sql statement to establish as a view
        :param fileName: optional: file (in SQL library) containing sql statement to execute
        :param recreate: optional: replace the existing view
        """

        # dump the view if we are recreating
        if recreate:
            self.deleteTable(tableName)

        if query is None and fileName is None:
            raise ValueError('SQLBase.createTableAs(): You must provide either a query or a query file')

        # read the SQL statement from the file
        if fileName is not None:
            with open(self.sqlDir + '/' + fileName, 'r') as sqlFile:
                query = sqlFile.read().replace('\n', ' ')

        query = 'CREATE TABLE ' + self.dataset + '.' + tableName + ' AS ' + query
        if not query.endswith(';'):
            query += ';'

        self._execDML(query, 'createTableAs')
        logging.info('BaseSQL.createTableAs: Successfully created table: ' + tableName)

    def _copyTable(self, sourceTableName, destTableName, sql):
        """
        Copy the contents of one table into another (of the same schema)
        :param sourceTableName: The source table to be copied
        :param destTableName: The destination table (to be pasted)
        :param sql: the sql dml statement describing how to copy the table
        """
        self._execDML(sql, 'copyTable')
        logging.info('BaseSQL.copyTable: Successfully copied table: ' + sourceTableName + ' into ' + destTableName)

    def copyTable(self, sourceTableName, destTableName):
        """
        Copy the contents of one table into another (of the same schema)
        :param sourceTableName: The source table to be copied
        :param destTableName: The destination table (to be pasted)
        :return:
        """
        sql = None
        self._copyTable(sourceTableName, destTableName, sql)

    def _deleteTable(self, tableName, sql):
        """
        Delete a table that exists in the database
        :param tableName: the name of the table to delete
        :param sql: the sql that describes how to delete the table
        """

        self._execDML(sql, 'deleteTable')
        logging.info('BaseSQL.deleteTable: Successfully deleted table: ' + tableName)

    @abc.abstractmethod
    def deleteTable(self, tableName):
        """
        Delete a table that exists in the database
        :param tableName: the name of the table to delete
        """
        # Note caller should override the sql in this function
        sql = None
        self._deleteTable(tableName, sql)

    def _getTable(self, sql):
        """
        get metadata information on a given table
        :param sql: the sql to pull the table metadata
        :return: dictionary
        """
        try:
            d = self.getQueryAsDict(sql)
            tableInfo = d[0]
            if tableInfo == {}:
                tableInfo['found'] = 0
            else:
                tableInfo['found'] = 1
            return tableInfo
        except Exception as dbErr:
            if 'does not exist' in str(dbErr):
                tableInfo = {}
                tableInfo['found'] = 0
                return tableInfo
            else:
                raise StandardError(str(dbErr))

    @abc.abstractmethod
    def getTable(self, tableName):
        """
        get metadata information on a given table
        :param tableName: the name of the table to delete
        :return: dictionary
        """
        # Note caller should override the sql in this function
        # The expected result should return database, schema, type and name attributes
        sql = None
        return self._getTable(sql)

    def getRowCount(self, tableName):
        """
        Retrive the number of rows in a table
        :param tableName: the name of the table to count
        :return: integer
        """
        return self.getTable(tableName)['row_count']

    def checkExist(self, tableName):
        """
        verifies if the requested table exists
        :param tableName: the name of the table to delete
        :return:
        """
        info = self.getTable(tableName)
        if info['found'] == 0:
            return False
        else:
            return True

    def _truncateTable(self, tableName, sql):
        """
        Remove the current contents of a table in the database
        :param tableName: the name of the table to delete
        :param sql: the sql to support truncating or emptying the table
        """

        self._execDML(sql, 'truncateTable')
        logging.info('BaseSQL.truncateTable: Successfully emptied table: ' + tableName)

    def truncateTable(self, tableName):
        """
        Remove the current contents of a table in the database
        :param tableName: the name of the table to delete
        """
        # Note caller should override the sql in this function
        sql = None
        self._truncateTable(tableName, sql)

    def _updateTable(self, tableName, sql):
        """
        Update every value in a currently existing table with data from a new table by key
        :param tableName: The name of the table to be updated
        :param sql: the sql to support truncating or emptying the table
        """
        # execute the statement against the database
        self._execDML(sql, 'updateTable')
        logging.info('BaseSQL.updatedTable: Successfully updated table: ' + tableName)

    @abc.abstractmethod
    def updateTable(self, tableName, schema, newTable, keyFieldList):
        """
        Update every value in a currently existing table with data from a new table by key
        Addtionally, insert every row from new table not already present in the old
        :param tableName: The name of the table to be updated
        :param schema: the ddl or "schema" definition of the table
        :param newTable: The name of the temporary table containing the new values
        :param keyFieldList: The list of fields that uniquely identify the record to be updated
        """
        # Note caller MAY need override the sql in this function; ANSI standard provided here
        sql = 'UPDATE ' + self.dataset + '.' + tableName + ' curr SET '
        # Add the set statement for non-key fields
        needComma = False
        for column in schema:
            if column['name'] not in keyFieldList:
                if needComma:
                    sql += ', '
                sql += column['name'] + ' = new.' + column['name']
                needComma = True
        # add in the source of the updates
        sql += ' FROM ' + self.dataset + '.' + newTable + ' new WHERE '
        # add the primary key part of the join clause
        needAnd = False
        for key in keyFieldList:
            if needAnd:
                sql += 'AND '
            sql += 'curr.' + key + ' = new.' + key + ' '
            needAnd = True
        # update the matching fields
        self._updateTable(tableName, sql)

        # grab the full set of comma separated fields
        fieldsPlain = ''
        fieldsNew = ''
        needComma = False
        for column in schema:
            if needComma:
                fieldsPlain += ', '
                fieldsNew += ', '
            fieldsPlain += column['name']
            fieldsNew += 'new.' + column['name']
            needComma = True

        sql = 'INSERT INTO ' + self.dataset + '.' + tableName + ' (' + fieldsPlain + ') ' + \
              'SELECT ' + fieldsNew + ' ' + \
              'FROM ' + self.dataset + '.' + tableName + ' curr ' + \
              'RIGHT OUTER JOIN ' + self.dataset + '.' + newTable + ' new ON '
        # add the primary key part of the join clause
        # put together the where clause at the same time
        where = 'WHERE '
        needAnd = False
        for key in keyFieldList:
            if needAnd:
                sql += 'AND '
            sql += 'curr.' + key + ' = new.' + key + ' '
            where += 'curr.' + key + ' IS NULL '
            needAnd = True
        # add in the where clause
        sql += where
        # insert the new data
        self._execDML(sql, 'updateTable')

    @abc.abstractmethod
    def createView(self, viewName, query=None, fileName=None, recreate=True, legacy=True):
        """
        Create a view in the destination schema
        :param viewName: the name of the view to create
        :param query: optional: sql statement to establish as a view
        :param fileName: optional: file (in SQL library) containing sql statement to execute
        :param recreate: optional: replace the existing view
        :param legacy: deprecate
        """

        # dump the view if we are recreating
        if recreate:
            self.deleteView(viewName)

        if query is None and fileName is None:
            raise ValueError('SQLBase.createView(): You must provide either a query or a query file')

        # read the SQL statement from the file
        if fileName is not None:
            with open(self.sqlDir + '/' + fileName, 'r') as sqlFile:
                query = sqlFile.read().replace('\n', ' ')

        query = 'CREATE VIEW ' + self.dataset + '.' + viewName + ' AS ' + query
        if not query.endswith(';'):
            query += ';'

        self._execDML(query, 'createView')
        logging.info('BaseSQL.createView: Successfully created view: ' + viewName)

    def _deleteView(self, viewName, sql):
        """
        Delete a view that exists in the database
        :param tableName: the name of the table to delete
        :param sql: the sql containing the statement for removing the view
        :return:
        """

        self._execDML(sql, 'deleteView')
        logging.info('BaseSQL.deleteView: Successfully deleted view: ' + viewName)

    @abc.abstractmethod
    def deleteView(self, viewName):
        """
        Delete a view that exists in the database
        :param tableName: the name of the table to delete
        """
        # Note caller should override the sql in this function
        sql = None
        self._deleteView(viewName, sql)

    @abc.abstractmethod
    def loadDataFromGCS(self, tableName, source, skipRows=1, writeDisp='WRITE_APPEND', fileFormat='csv'):
        """
        Load a database table from a file in GCS
        :param tableName: the name of the table to load
        :param source: Full GCS source path for the file
        :param writeDisp: optional how to load the file (append, or overwrite) (default: append)
        """

        gs = GSHelper(self.projectId)
        fileName = tableName + '.' + fileFormat
        gs.downloadFile(source, fileName)
        self.loadDataFromFile(tableName, fileName, skipRows=skipRows, writeDisp=writeDisp, fileFormat=fileFormat)
        os.remove(fileName)

    @abc.abstractmethod
    def loadDataFromFile(self, tableName, fileName, skipRows=1, writeDisp='WRITE_APPEND', fileFormat='csv'):
        """
        Load a database table from a data file
        :param tableName: the name of the table to load
        :param fileName: the name of the local file to load into the table
        :param skipRows: optional: the number of rows to skip at the top of a file (default: 0)
        :param writeDisp: optional how to load the file (append, or overwrite) (default: append)
        :param fileFormat: optional: the format of the input file (default: csv)
        """
        # set the file separator
        if fileFormat == 'csv':
            sep = ','

        if writeDisp == 'WRITE_TRUNCATE':
            self.truncateTable(tableName)

        # trim header rows: 1 by default
        if skipRows >= 1:
            with open(fileName, 'r') as fileIn:
                allrows = fileIn.read().splitlines(True)
            # split up the filename and create temp version
            fileName, extension = os.path.splitext(fileName)
            fileName = fileName + '-noheader' + extension
            # stip out the top line
            with open(fileName, 'w') as fileOut:
                fileOut.writelines(allrows[skipRows:])

        # load the file
        try:
            f = open(fileName, 'r')
            cur = self.connection.cursor()
            # cur.copy_from(f, self.dataset + '.' + tableName, sep)
            sql = 'COPY ' + self.dataset + '.' + tableName + ' FROM STDIN CSV DELIMITER \',\' QUOTE \'"\''
            cur.copy_expert(sql, f)
            cur.close()
            self.connection.commit()
            f.close()
        except Exception as dbErr:
            msg = 'SQLBase.loadDataFromFile: Error loading file ' + fileName + ' into table ' + tableName + '. Details: ' + str(dbErr)
            logging.error(msg)
            raise StandardError(msg)

        # clean up temp table
        if skipRows >= 1:
            os.remove(fileName)

    @abc.abstractmethod
    def getQueryAsDataframe(self, query):
        """
        Pull the data from query into a dataframe
        :param query: the SQL statement representing the query
        :return: dataframe
        """
        try:
            df = pd.read_sql_query(query, con=self.connection)
        except Exception as dbErr:
            msg = 'SQLBase.getQueryAsDataframe: Error running SQL query against the database. Query: ' + query + '. Details: ' + str(dbErr)
            logging.error(msg)
            raise StandardError(msg)

        return df

    def getQueryAsDict(self, query):
        """
        Pull the data from query into a list of records/dictionay
        :param query: the SQL statement representing the query
        :return: dictionary
        """
        return self.getQueryAsDataframe(query).to_dict(orient='records')

    def getSchemaFromFile(self, tableName):
        """
        Extract the schema from the i2ap ddl
        :param tableName: the name of the table
        :return: dictionary
        """
        with open(self.ddlDir + '/' + tableName + '.json') as json_data:
            json_content = json.load(json_data)
        return json_content['fields']

    @abc.abstractmethod
    def exportDataToFile(self, tableName):
        """
        Extract data from a table and place it into a csv file in GCS
        :param tableName: the name of the table to load
        """
        query = 'SELECT * FROM ' + self.dataset + '.' + tableName + ';'
        df = self.getQueryAsDataframe(query)
        df.to_csv(tableName + '.csv', index=False, header=True)

    @abc.abstractmethod
    def exportDataToGCS(self, tableName, destination):
        """
        Extract data from a table and place it into a csv file in GCS
        :param tableName: the name of the table to load
        :param destination: the GS path
        """
        self.exportDataToFile(tableName)
        gs = GSHelper(self.projectId)
        gs.uploadFile(tableName + '.csv', destination)
        os.remove(tableName + '.csv')
