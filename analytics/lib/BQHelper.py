#!/usr/bin/env python

from googleapiclient.discovery import build
from googleapiclient.errors import HttpError
from oauth2client.client import GoogleCredentials
from google.cloud import bigquery
# from bigquery.client import JOB_WRITE_TRUNCATE
import uuid
import time
import json
import os
import petl as etl
import numpy as np
import pandas as pd
import logging
from SQLBase import SQLBase

class BQHelper(SQLBase):
    """
    API Reference: https://googlecloudplatform.github.io/google-cloud-python/latest/bigquery/reference.html
    """
    def _dbConfig(self):
        """
        database or type specific connections
        """
        # grab the application default credentials from application environment
        self.credentials = GoogleCredentials.get_application_default()
        # Construct the service object for interacting with BigQuery APIs
        self.bigquery_service = build('bigquery', 'v2', credentials=self.credentials)
        # Establish the bigquery client
        self.client = bigquery.Client(project=self.projectId)

    # BigQuery specific utility funcions

    def _waitForJob(self, job):
        """
        Wait for the job to complete within BigQuery
        :param job: the key/identifier for the BQ job
        :return: exit
        """
        bqjobId = ''
        while True:
            try:
                job.reload()
                try:
                    bqjobId = job._properties['jobReference']['jobId']
                except:
                    bqjobId = ''
            except Exception as reloaderr:
                self.logger.logEvent('BQHelper.waitForJobReload: ' + str(reloaderr), severity='WARNING',
                                     jobstatus='INPROGRESS', bqJobId=bqjobId)

            if job.state == 'DONE':
                if job.error_result:
                    self.logger.logEvent('BQHelper.waitForJob: ' + str(job.errors), severity='ERROR', jobstatus='DONE',
                                         bqJobId=bqjobId)
                    raise RuntimeError(str(job.errors) + "\n bqJobId: " + bqjobId)
                return
            time.sleep(10)

    def _waitForJobCountRows(self, job, tableName, startCnt, completeCnt):
        """
        Wait for the job to complete with count verification
        :param job: the key/identifier for the BQ job
        :param tableName: the name of the table the job is impacting
        :param startCnt: the base count for the impacted table
        :param completeCnt: the expected completion count for the impacted table
        :return:
        """
        while True:
            job.reload()
            if job.state == 'DONE':
                if job.error_result:
                    raise RuntimeError(str(job.errors))
                else:
                    # pull row count
                    currCnt = self.getRowCount(tableName)
                    if startCnt < currCnt < completeCnt:
                        raise RuntimeError(str(job.errors))
                    elif currCnt == completeCnt:
                        return
            time.sleep(1)

    def execDML(self, sql):
        """
        execute a DML statement to act on a database
        :param sql: DML statement to execute
        """
        retry = 0
        while retry < 3:
            try:
                job = self.client.query(sql)
                # wait for the job to complete
                job.result()
                break
            except Exception as bqErr:
                msg = 'BQHelper.execDML: Attempt ' + str(retry) + ' failed. Details:' + str(bqErr)
                self.logger.logEvent(msg, severity='WARNING', jobstatus='INPROGRESS')
                retry += 1
                time.sleep(2)

    def createDataset(self, datasetId):
        """
        Create a schema in the existing database
        :param datasetId: the name of the schema to be created
        """
        if datasetId is not None:
            self.dataset = datasetId

        try:
            ref = self.client.dataset(self.dataset)
            dataset = bigquery.Dataset(ref)
            res = self.client.create_dataset(dataset)
        except Exception as createErr:
            msg = "BQHelper.createDataset: Error creating dataset. Details: " + str(createErr)
            logging.error(msg)

        if not self.checkDatasetExist(datasetId):
            raise Exception('Error creating dataset')

    def deleteDataset(self, datasetId):
        """
        Delete the specified schema (and all contained tables) from the database
        :param datasetId: the name of the schema to delete
        """
        if datasetId is not None:
            self.dataset = datasetId

        try:
            ref = self.client.dataset(self.dataset)
            dataset = bigquery.Dataset(ref)
            self.client.delete_dataset(dataset)
        except Exception as deleteErr:
            msg = "BQHelper.deleteDataset: Some issue in deleting the dataset. Details: " + str(deleteErr)
            logging.error(msg)

        if self.checkDatasetExist(datasetId):
            raise Exception('Error deleting dataset')

    def getDataset(self, datasetId):
        """
        Pull metadata and other information about a given schema
        :param datasetId: the name of the schema to query
        :return: dictionary of dataset attributes
        """
        if datasetId is not None:
            self.dataset = datasetId

        datasetInfo = {}
        try:
            ref = self.client.dataset(self.dataset)
            dataset = bigquery.Dataset(ref)
            res = self.client.get_dataset(dataset)
            datasetInfo['found'] = 1
            datasetInfo['database'] = dataset.project
            datasetInfo['name'] = dataset.dataset_id
        except:
            datasetInfo['found'] = 0

        return datasetInfo

    def _createSchemaObject(self, schema):
        """
        take and i2ap schema dictionary and convert it into a schema collection
        :param schema: the ddl or "schema" definition of the table
        :return: list of bigquery.SchemaField
        """
        fields = []
        for field in schema:
            if field['type'].upper() == 'RECORD':
                # add the parent record field, but include the children from a recursive call
                fields.append(bigquery.SchemaField(field['name'], field['type'], mode=field['mode'].upper(), fields=self._createSchemaObject(field['fields'])))
            else:
                fields.append(bigquery.SchemaField(field['name'], field['type'], mode=field['mode'].upper()))
        return tuple(fields)

    def createTable(self, tableName, schema, shard=False, recreate=False):
        """
        Create a new table in a db schema
        :param tableName: the name of the table to delete
        :param schema: the ddl or "schema" definition of the table
        :param sql: the sql dml statement describing how to create the table
        :param shard: boolean: should the table be partitioned?
        :param recreate: boolean: should the table be dropped and recreated?
        """
        dataset = bigquery.Dataset(self.client.dataset(self.dataset))
        ref = dataset.table(tableName)

        if recreate:
            self.deleteTable(tableName)

        if not self.checkExist(tableName):
            table = bigquery.Table(ref, schema=self._createSchemaObject(schema))
            table_out = self.client.create_table(table)

        if not self.checkExist(tableName):
            raise Exception('Error creating table')

    def createTableAs(self, tableName, query=None, fileName=None, recreate=False):
        """
        Create a new table from a query
        :param tableName: the name of the view to create
        :param query: optional: sql statement to establish as a view
        :param fileName: optional: file (in SQL library) containing sql statement to execute
        :param recreate: optional: replace the existing view
        """
        dataset = bigquery.Dataset(self.client.dataset(self.dataset))
        ref = dataset.table(tableName)

        if query is None and fileName is None:
            raise ValueError('SQLBase.createView(): You must provide either a query or a query file')

        # read the SQL statement from the file
        if fileName is not None:
            with open(self.sqlDir + '/' + fileName, 'r') as sqlFile:
                query = sqlFile.read().replace('\n', ' ')

        config = bigquery.QueryJobConfig()
        config.create_disposition = 'CREATE_IF_NEEDED'
        config.destination = ref
        config.disposition = 'WRITE_TRUNCATE'
        config.maximum_billing_tier = 100
        # block to trap and respond to 500 errors back from the API
        retry = 0
        while retry < 3:
            try:
                job = self.client.query(query, job_config=config)
                # wait for the job to complete
                job.result()
                break
            except Exception as bqErr:
                msg = 'BQHelper.createTableAs: Attempt ' + str(retry) + ' failed. Details:' + str(bqErr)
                self.logger.logEvent(msg, severity='WARNING', jobstatus='INPROGRESS')
                retry += 1
                time.sleep(2)

    def copyTable(self, sourceTableName, destTableName):
        """
        Copy the contents of one table into another (of the same schema)
        :param sourceTableName: The source table to be copied
        :param destTableName: The destination table (to be pasted)
        :return:
        """
        dataset = bigquery.Dataset(self.client.dataset(self.dataset))
        srcTbl = dataset.table(sourceTableName)
        destTbl = dataset.table(destTableName)
        # counts
        srcCnt = self.getRowCount(sourceTableName)
        destCnt = self.getRowCount(destTableName)
        targetCnt = srcCnt + destCnt

        config = bigquery.CopyJobConfig()
        config.write_disposition = 'WRITE_APPEND'
        job = self.client.copy_table(srcTbl, destTbl, job_config=config)

        # wait for the results
        job.results()

    def deleteTable(self, tableName):
        """
        Delete a table that exists in the database
        :param tableName: the name of the table to delete
        """
        dataset = bigquery.Dataset(self.client.dataset(self.dataset))
        table = dataset.table(tableName)

        try:
            self.client.delete_table(table)
        except Exception as deleteErr:
            msg = "BQHelper.deleteTable: Some issue in deleting the table. Details: " + str(deleteErr)
            logging.error(msg)

        if self.checkExist(tableName):
            raise Exception('Error deleting table')

    def getTable(self, tableName):
        """
        get metadata information on a given table
        :param tableName: the name of the table to delete
        :return: dictionary
        """
        dataset = bigquery.Dataset(self.client.dataset(self.dataset))
        table_ref = dataset.table(tableName)
        tableInfo = {}
        try:
            table = self.client.get_table(table_ref)
            tableInfo['found'] = 1
            tableInfo['row_count'] = table.num_rows
            tableInfo['schema'] = table.dataset_id
            tableInfo['name'] = table.table_id
            tableInfo['type'] = table.table_type
            tableInfo['database'] = table.project
        except:
            tableInfo['found'] = 0
        return tableInfo

    def createView(self, viewName, query=None, fileName=None, recreate=True, legacy=True):
        """
        Create a view in the destination schema
        :param viewName: the name of the view to create
        :param query: optional: sql statement to establish as a view
        :param recreate: optional: replace the existing view
        :param legacy: deprecate
        :return:
        """
        if query is None and fileName is None:
            raise ValueError('SQLBase.createView(): You must provide either a query or a query file')

        # read the SQL statement from the file
        if fileName is not None:
            with open(self.sqlDir + '/' + fileName, 'r') as sqlFile:
                query = sqlFile.read().replace('\n', ' ')

        dataset = bigquery.Dataset(self.client.dataset(self.dataset))
        ref = dataset.table(viewName)

        if recreate:
            self.deleteTable(viewName)


        table = bigquery.Table(ref)
        table_out = self.client.create_table(table)

        if not self.checkExist(tableName):
            raise Exception('Error creating table')

    def deleteView(self, viewName):
        """
        Delete a view that exists in the database
        :param tableName: the name of the table to delete
        """
        self.deleteTable(viewName)

    def loadDataFromGCS(self, tableName, source, skipRows=1, writeDisp='WRITE_APPEND', fileFormat='csv'):
        """
        Load a database table from a file in GCS
        :param tableName: the name of the table to load
        :param source: Full GCS source path for the file
        :param writeDisp: optional how to load the file (append, or overwrite) (default: append)
        """
        dataset = self.client.dataset(self.dataset)
        table = dataset.table(tableName)

        config = bigquery.LoadJobConfig()
        if fileFormat == 'csv':
            config.source_format = 'CSV'
            config.skip_leading_rows = skipRows
        elif fileFormat == 'json':
            config.source_format = 'NEWLINE_DELIMITED_JSON'
        config.write_disposition = writeDisp

        # block to trap and respond to 500 errors back from the API
        retry = 1
        while retry <= 3:
            try:
                job = self.client.load_table_from_uri('gs://' + source, table, job_config=config, job_id_prefix='i2ap')
                # wait for completion
                job.result()
                break
            except Exception as bqErr:
                msg = 'BQHelper.loadDataFromGCS: Attempt ' + str(retry) + ' failed. Details:' + str(bqErr)
                print(msg)
                logging.info(msg)
                self.logger.logEvent(msg, severity='WARNING', jobstatus='INPROGRESS')
                retry += 1
                time.sleep(10)

        if retry == 4:
            msg = 'BQHelper.loadDataFromGCS: Retry attempts exceeded. Load consistently fails. Aborting.'
            logging.error(msg)
            self.logger.logEvent(msg, severity='ERROR', jobstatus='END')
            raise StandardError(msg)

    def loadDataFromFile(self, tableName, fileName, skipRows=1, writeDisp='WRITE_APPEND', fileFormat='csv'):
        """
        Load a database table from a data file
        :param tableName: the name of the table to load
        :param fileName: the name of the local file to load into the table
        :param skipRows: optional: the number of rows to skip at the top of a file (default: 0)
        :param writeDisp: optional how to load the file (append, or overwrite) (default: append)
        :param fileFormat: optional: the format of the input file (default: csv)
        """
        dataset = self.client.dataset(self.dataset)
        table = dataset.table(tableName)

        config = bigquery.job.LoadJobConfig()
        if fileFormat == 'csv':
            config.source_format = 'CSV'
            config.skip_leading_rows = skipRows
        elif fileFormat == 'json':
            config.source_format = 'NEWLINE_DELIMITED_JSON'
        config.write_disposition = writeDisp
        # This example uses CSV, but you can use other formats.
        # block to trap and respond to 500 errors back from the API
        retry = 1
        while retry <= 3:
            try:
                with open(fileName, 'rb') as source_file:
                    job = self.client.load_table_from_file(source_file, table, job_config=config)
                    # wait for completion
                    job.result()
                break
            except Exception as bqErr:
                msg = 'BQHelper.loadDataFromFile: Attempt ' + str(retry) + ' failed. Details:' + str(bqErr)
                logging.info(msg)
                self.logger.logEvent(msg, severity='WARNING', jobstatus='INPROGRESS')
                retry += 1
                time.sleep(5)

        if retry == 4:
            msg = 'BQHelper.loadDataFromFile: Retry attempts exceeded. Load consistently fails. Aborting.'
            logging.error(msg)
            self.logger.logEvent(msg, severity='ERROR', jobstatus='END')
            raise StandardError(msg)

    def getQueryAsDataframe(self, query):
        """
        Pull the data from query into a dataframe
        :param query: the SQL statement representing the query
        :return: dataframe
        """
        # block to trap and respond to 500 errors back from the API
        retry = 1
        while retry <= 3:
            try:
                job = self.client.query(query)
                # wait for the job to complete
                iterator = job.result()
                # for some reason to_dataframe cannot be found, so do it here
                headers = [field.name for field in iterator.schema]
                rows = [row.values() for row in iterator]
                return pd.DataFrame(rows, columns=headers)
            except Exception as bqErr:
                msg = 'BQHelper.getQueryAsDataframe: Attempt ' + str(retry) + ' failed. Details:' + str(bqErr)
                logging.warning(msg)
                self.logger.logEvent(msg, severity='WARNING', jobstatus='INPROGRESS')
                retry += 1
                time.sleep(5)
        if retry == 4:
            msg = 'BQHelper.getQueryAsDataframe: Retry attempts exceeded. Query consistently fails. Aborting.'
            logging.error(msg)
            self.logger.logEvent(msg, severity='ERROR', jobstatus='END')
            raise StandardError(msg)

    def exportDataToFile(self, tableName):
        """
        Extract data from a table and place it into a csv file in GCS
        :param tableName: the name of the table to load
        """
        destination = 'temp/' + tableName + '.csv'
        self.exportDataToStorage(tableName, destination)
        self.gs.downloadFile(destination, tableName + '.csv')
        self.gs.deleteFile('temp/' + tableName + '.csv')

    def exportDataToStorage(self, tableName, destination):
        """
        Extract data from a table and place it into a csv file in GCS
        :param tableName: the name of the table to load
        :param destination: the GS path
        """
        dataset = bigquery.Dataset(self.client.dataset(self.dataset))
        table = dataset.table(tableName)

        try:
            job = self.client.extract_table(table,'gs://' + destination)
            # wait for the return
            job.result()
        except Exception as exportErr:
            msg = 'BQHelper.exportDataToStorage: Export of table data failed. Details:' + str(exportErr)
            logging.error(msg)
            self.logger.logEvent(msg, severity='ERROR', jobstatus='INPROGRESS')
            raise exportErr
    #
    #
    # TODO move the BQ only functions over to the base class
    #
    #

    def _waitForServiceJobs(self, OutName):
        # TODO: Why can't the other waitForJob methods be used here
        while True:

            try:

                status = self.bigquery_service.jobs().get(
                    projectId=self.projectId, jobId=OutName['jobReference']['jobId']).execute()
                if 'DONE' == status['status']['state']:
                    break
            except Exception as bqreaderr:
                self.logger.logEvent('BQHelper._waitForServiceJobs: ' + str(bqreaderr), severity='WARNING',
                                     jobstatus='INPROGRESS', bqJobId=OutName['jobReference']['jobId'])

            print('waiting for 10 secs')
            time.sleep(10)

        if 'errors' in status['status']:
            raise RuntimeError('Error in creating table from query. Details: ' + str(status))

    def getTableData(self, datasetId, tableName):
        tableRef = {'projectId': self.projectId,
                    'datasetId': datasetId,
                    'tableId': tableName}
        return self.bigquery_service.tabledata().list(**tableRef).execute()

    def getNpArray(self, rowArray, dtype):
        '''
        convert an table array into a petl table
        return the petl table
        '''
        # use numpy to comform the arrays into something readable by PETL

        return np.array(rowArray, dtype)

    def getQueryAsTable(self, query, dtype, legacy=True):
        '''
        execute a query against BigQuery
        return a PETL table
        '''
        return etl.fromarray(self.getQueryAsArray(query, dtype, legacy))


    def getQueryAsDataframewSchema(self, query, legacy=True):
        '''
        execute a query against BigQuery
        return a dataframe
        '''
        results = self.client.run_sync_query(query)
        results.use_legacy_sql = legacy
        results.timeout_ms = 10000000

        results.run()
        job = results.job
        retry_count = 1000

        while retry_count > 0 and job.state != 'DONE':
            retry_count -= 1
            print "retrying: " + str(retry_count)
            time.sleep(2)
            job.reload()

        # run the query and return the results - synchronously

        schema = [field.name for field in results.schema]
        # pull 10k row blocks or "pages"
        page_token = None
        tableDf = None

        rows = list(results.fetch_data())

        # add to the table
        if tableDf is None:
            tableDf = pd.DataFrame(rows)
        else:
            tableDf = tableDf.append(pd.DataFrame(rows))

        tableDf.columns = schema
        return tableDf

    def getQueryAsArray(self, query, dtype, legacy=True):
        '''
        execute a query against BigQuery
        return an array of rows
        '''
        while True:
            try:
                results = self.client.run_sync_query(query)
                results.use_legacy_sql = legacy
                # run the query and return the results - synchronously
                results.run()
                break
            except Exception as bqErr:
                self.logger.logEvent('BQHelper.getQueryAsArray, run_sync_query: ' + str(bqErr), severity='WARNING',
                                     jobstatus='INPROGRESS')
                time.sleep(10)

        # pull 10k row blocks or "pages"
        page_token = None
        tableArray = None
        pageTokenHold = None

        try:
            rows = list(results.fetch_data())

            # back up the tokens in case the next loop fails
            pageTokenHold = page_token

            # add to the table
            if tableArray is None:
                tableArray = self.getNpArray(rows, dtype)
            else:
                tableArray = np.append(tableArray, self.getNpArray(rows, dtype))



        except Exception as bqErr:
            print "didn't work out with page fetch"
            self.logger.logEvent('BQHelper.getQueryAsArray, page fetch: ' + str(bqErr), severity='WARNING',
                                 jobstatus='INPROGRESS')
            page_token = pageTokenHold
            time.sleep(10)

        return tableArray

    def getTableSchema(self, datasetId, tableName):
        tableCollection = self.bigquery_service.tables()
        tablemetadata = tableCollection.get(projectId=self.projectId,
                                            datasetId=datasetId,
                                            tableId=tableName).execute()
        self.tableColumns = []
        for i in xrange(len(tablemetadata['schema']['fields'])):
            self.tableColumns.append(tablemetadata['schema']['fields'][i]['name'])
        return self.tableColumns

    def stream_data(self, datasetId, tableName, data, schema):
        # first checks if table already exists. If it doesn't, then create it
        if not self.checkExist(tableName):
            self.createTable(tableName, schema, shard=False, recreate=False)

        body = {
            'rows': [
                {
                    'json': data,
                    'insertId': str(uuid.uuid4())
                }
            ]
        }
        self.bigquery_service.tabledata().insertAll(projectId=self.projectId, datasetId=datasetId, tableId=tableName,
                                                    body=body).execute(num_retries=5)


