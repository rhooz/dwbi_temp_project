#!/usr/bin/env python
import collections
import logging
import uuid
import os
import json
import csv
import datetime
from salesforce_bulkipy import SalesforceBulkipy
from simple_salesforce import Salesforce
# from SDHelper import SDHelper
# from GSHelper import GSHelper
# from SQLHelper import SQLHelper
# from JobState import JobState
from ETLHelper import ETLHelper
from RemoteDebug import *

class SFHelper:
    """
    Simplify interactions with Salesforce in a i2ap-y way

    Test Script:

        sf = SFHelper('tt-cust-wynn-data', 'tt-cust-wynn-data-stage', 'tt-cust-wynn-data-archive', 'account', 'commix.admin@gettectonic.com.wynn', 'tectonic123', '3emEdghKnT3xFkAyy5gRtwRt', sandbox=True)
        sf.start()
        sf.insert('account-test.csv', batchSize=5)
        sf.waitForAll()
        sf.finish()
    """

    def __init__(self, config, objectName, splitSize=None, jobId='', maintainState=False, incremental=False):
        """constructor
        config = { 'project-id': '',
                   'stage-bucket': '',
                   'archive-bucket': '',
                   'ddl-directory': '',
                   'salesforce-user': '',
                   'salesforce-password': '',
                   'salesforce-token': '',
                   'salesforce-sandbox': '' }

        objectName = 'Salesforce_table_name',
        splitSize = int() # number of records per batch
        """

        self.project = config['project-id'] if 'project-id' in config.keys() else None
        self.bucket = config['stage-bucket'] if 'stage-bucket' in config.keys() else None
        self.archive = config['archive-bucket'] if 'archive-bucket' in config.keys() else None
        self.ddlDir = config['ddl-directory'] if 'ddl-directory' in config.keys() else None
        self.config = config
        self.maintainState = maintainState
        self.incremental = incremental

        # resolve job id
        if jobId == '':
            self.jobId = str(uuid.uuid4())
        else:
            self.jobId = jobId

        # bring in the full object list
        if self.ddlDir:
            with open(self.ddlDir + '/salesforce-manifest.json') as json_data:
                self.manifest = json.load(json_data)
        else:
            # TODO: How do we want to handle this in the future?
            self.ddlDir = None

        if self.project:
            self.gs = GSHelper(self.project)
            self.sd = SDHelper(self.project, self.logName, self.jobId)
        else:
            # TODO: add integration for any bucket system
            self.gs = None
            # TODO: add integration for any logging system
            self.sd = None

        # authenticate against salesforce
        try:
            self.sf = SalesforceBulkipy(username=config['salesforce-user'], password=config['salesforce-password'],
                                        security_token=config['salesforce-token'], sandbox=config['salesforce-sandbox'])
            self.ssf = Salesforce(username=config['salesforce-user'], password=config['salesforce-password'],
                                  security_token=config['salesforce-token'], sandbox=config['salesforce-sandbox'])
        except Exception as sfErr:
            msg = 'SFHelper: Authentication against salesforce failed. Details: ' + str(sfErr)
            logging.error(msg)
            if self.sd:
                self.sd.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
            raise StandardError(msg)

        self.setObjectName(objectName)
        # establish an information object to pull info about the salesforce object... objectName
        self.isStarted = False
        self.columns = None

    def setObjectName(self, objectName, incremental=None):
        """setter for the objectName used for looping calls"""
        self.objectName = objectName
        if incremental is not None:
            self.incremental = incremental
        if objectName != '':
            self.sfInfo = self.ssf.__getattr__(objectName)

    def convertType(self, sfType):
        """
        Convert the Salesforce type into the generic I2AP type
        :param sfType: the salesforce data type
        :return: the generic I2AP type
        """
        if sfType.upper() == 'ID':
            returnType = 'STRING'
        elif sfType.upper() == 'BOOLEAN':
            returnType = 'BOOLEAN'
        elif sfType.upper() == 'REFERENCE':
            returnType = 'STRING'
        elif sfType.upper() == 'PICKLIST':
            returnType = 'STRING'
        elif sfType.upper() == 'MULTIPICKLIST':
            returnType = 'STRING'
        elif sfType.upper() == 'TEXTAREA':
            returnType = 'STRING'
        elif sfType.upper() == 'PHONE':
            returnType = 'STRING'
        elif sfType.upper() == 'STRING':
            returnType = 'STRING'
        elif sfType.upper() == 'DOUBLE':
            returnType = 'FLOAT'
        elif sfType.upper() == 'CURRENCY':
            returnType = 'FLOAT'
        elif sfType.upper() == 'INT':
            returnType = 'INTEGER'
        elif sfType.upper() == 'DATETIME':
            returnType = 'DATETIME'
        elif sfType.upper() == 'DATE':
            returnType = 'DATE'
        else:
            returnType = 'STRING'
        return returnType

    def getSchema(self, persist=False):
        """
        Build an I2AP schema from salesforce schema information
        :param persist: Indicates if the caller wants the i2ap ddl/schema persisted to disk
        :return: list of column dictionaries
        """
        # update the column set
        if persist:
            self.columns = self._describe(source='salesforce')
        else:
            self.columns = self._describe(source='i2ap')
        # convert the sf schema into an i2ap one
        schema = []
        for column in self.columns:
            attr = {}
            attr['name'] = column['name']
            attr['type'] = self.convertType(column['type'])
            attr['mode'] = 'nullable'
            schema.append(attr)

        # save the ddl to a file if persist is set to true
        if persist:
            etl = ETLHelper(self.config)
            etl.schemaToFile(self.objectName.lower(), schema)

        # return the schema to the caller
        return schema

    def _describe(self, source='salesforce'):
        """return a list of fields and information FROM Salesforce about an object"""
        if source == 'salesforce':
            desc = self.sfInfo.describe()
            columns = []
            # sort the fields by name
            sortedFields = sorted(desc['fields'], key=lambda k: k['name'])
            for x in sortedFields:
                if x['type'] != 'address' and x['type'] != 'url' and 'Geocode' not in x['name'] \
                        and x['name'] != 'HasOpenActivity' and x['name'] != 'HasOverdueTask' \
                        and x['name'] != 'BadgeText' and x['name'] != 'IsProfilePhotoActive' \
                        and x['name'] != 'IsHighPriority' and x['name'] != 'RecurrenceRegeneratedType' and x['name'] != 'TaskSubtype':
                    attr = {"name": str(x['name']),
                            "type": str(x['type']),
                            "length": str(x['length']),
                            "custom": str(x['custom'])}
                    columns.append(attr)
        else:
            with open(self.ddlDir + '/' + self.objectName.lower() + '.json', 'r') as inFile:
                schema = json.load(inFile)
            columns = schema['fields']
        return columns

    def describe(self, refresh=False):
        """return a list of fields and information about an object"""
        if refresh:
            # if the caller wants the data from Salesforce, re-pull it
            self.columns = self._describe(source='salesforce')
        else:
            # pull it from the object ddl
            try:
                self.columns = self._describe(source='i2ap')
            except:
                # if there is no local ddl, then pull from salesforce (i.e. refresh)
                self.getSchema(persist=True)

    def checkForChange(self):
        """
        check to see if there are changes between i2ap and salesforce
        :return: True if there is a change, False if not
        """

        # pull the salesforce columns (and sort them
        sfSchema = self.columnList(self._describe(source='salesforce')).sort()
        i2apSchema = self.columnList(self._describe(source='i2ap')).sort()
        # compare function will return 0 if they are identical
        if cmp(sfSchema, i2apSchema) == 0:
            return False
        else:
            return True

    def columnList(self, columns=[]):
        """return a list of column names"""
        # if passed in, use that list instead of the object list
        if columns == []:
            columns = self.columns

        columnNames = []
        for x in columns:
            columnNames.append(x['name'])
        return columns

    def columnString(self, columns=[]):
        """return a string of column names"""
        # if passed in, use that list instead of the object list
        if columns == []:
            columns = self.columns

        columnNames = ''
        needComma = False
        for x in columns:
            if needComma:
                columnNames += ', '
            columnNames += x['name']
            needComma = True
        return columnNames

    def saveResults(self, rows, failed, remaining):
        self.batchResults = rows

    def start(self, bulkType):
        """begin a batch job process"""
        if not self.isStarted:
            try:
                if bulkType == "insert":
                    # start a batch that supports concurrent insert jobs
                    self.job = self.sf.create_insert_job(self.objectName, contentType='CSV', concurrency='Parallel')
                    self.isStarted = True
                    logging.info('SFHelper.start: Owning salesforce job for insert: ' + str(self.job))
                if bulkType == "query":
                    # start a batch that facilitates querying data
                    self.job = self.sf.create_query_job(self.objectName)
                    self.isStarted = True
                    logging.info('SFHelper.start: Owning salesforce job for query: ' + str(self.job))
            except Exception as sfErr:
                msg = 'SFHelper.start: Unable to open up job for concurrent loads. Details: ' + str(sfErr)
                logging.error(msg)
                if self.sd:
                    self.sd.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                raise StandardError(msg)

            # build a dictionary to track batch status within
            self.batchIds = []

    def insert(self, fileName, batchSize=2500):
        """insert a single batch"""
        # make sure a job is open for processing
        if not self.isStarted:
            self.start('insert')

        # pull the file into the pod
        self.gs.downloadFile(self.bucket+'/'+fileName, fileName)

        # pull in the csv object
        try:
            sfContent = open(fileName).read()
        except Exception as sfErr:
            msg = 'SFHelper.insert: Error likely in data content; unable to split csv file. Details: ' + str(sfErr)
            logging.error(msg)
            if self.sd:
                self.sd.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
            raise StandardError(msg)

        # bulkipy takes care of splitting for us - 3rd arg is the split size
        try:
            batchIds = self.sf.bulk_csv_upload(self.job, sfContent, batchSize)
        except Exception as sfErr:
            msg = 'SFHelper.insert: Failure in submitting batches. Details: ' + str(sfErr)
            logging.error(msg)
            if self.sd:
                self.sd.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
            raise StandardError(msg)
        else:
            # make sure the batch ids are legit
            for batch in batchIds:
                if not batch:
                    msg = 'SFHelper.insert: Did not receive valid batch id from service.'
                    logging.error(msg)
                    if self.sd:
                        self.sd.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
                    raise StandardError(msg)
            self.batchIds.extend(batchIds)

        # clean up the file and archive
        os.remove(fileName)
        self.gs.moveFile(self.bucket, destBucket=self.archive, fileName=fileName)

    def _wait(self, batchId):
        """wait for a single salesforce batch to complete"""
        self.sf.wait_for_batch(self.job, batchId, timeout=120)

    def waitForAll(self):
        """with the batch submitted, wait for them all to finish"""
        if self.batchIds:
            for batch in self.batchIds:
                logging.info('SFHelper.wait: Setting wait for batch: ' + str(batch))
                self._wait(batch)

    def _getLastUpdateRow(self):
        """
        get the object sync metadata row
        :return array of dict:
        """
        s = SQLHelper.factory(self.config, self.config['database-type'])
        # make sure the object metadata table exists
        if not s.checkExist('i2ap_incremental'):
            s.createTable('i2ap_incremental', s.getSchemaFromFile('i2ap_incremental'))

        sql = 'SELECT dataset, table_name, last_modified_date ' + \
              'FROM ' + self.config['dataset'] + '.i2ap_incremental ' + \
              'WHERE dataset = \'' + self.config['dataset'] + '\' ' + \
              'AND table_name = \'' + self.objectName.lower() + '\''
        return s.getQueryAsDict(sql)

    def getLastUpdate(self):
        """
        get the last date/time the object was changed
        :return: datetime
        """
        try:
            result = self._getLastUpdateRow()
        except:
            result = []
        # if nothing is returned, then the full table is needed
        if result == []:
            return None
        else:
            # convert the timestamp to string and the salesforce standard
            return str(result[0]['last_modified_date']).replace(' ', 'T') + 'Z'

    def setLastUpdate(self, lastUpdateField='systemmodstamp'):
        """
        set the last date/time the object was synced
        """
        # see if this record has been synced before
        result = self._getLastUpdateRow()
        logging.info('out of last update metadata check')
        # get the last modified date
        sql = 'SELECT max(' + lastUpdateField + ') last_modified_date ' + \
              'FROM ' + self.config['dataset'] + '.' + self.objectName.lower()
        s = SQLHelper.factory(self.config, self.config['database-type'])
        lastUpdate = s.getQueryAsDict(sql)[0]['last_modified_date']
        # store the updated date
        if result == []:
            # if not, then insert a new record
            sql = 'INSERT INTO ' + self.config['dataset'] + '.i2ap_incremental (dataset, table_name, last_modified_date) ' + \
                  'VALUES (\'' + self.config['dataset'] + '\', \'' + self.objectName.lower() + '\', \'' + str(lastUpdate) + '\')'
        else:
            # if it has been synced, then update the record
            sql = 'UPDATE ' + self.config['dataset'] + '.i2ap_incremental ' + \
                  'SET last_modified_date = \'' + str(lastUpdate) + '\' ' + \
                  'WHERE dataset = \'' + self.config['dataset'] + '\' ' + \
                  'AND table_name = \'' + self.objectName.lower() + '\''
        s.execDML(sql)

    def query(self, lastUpdateField='systemmodstamp'):
        """query the contents of a single table into a csv"""
        # TODO: Set up last updated

        # make sure a job is open for processing
        if not self.isStarted:
            self.start('query')

        # make sure we have details about the object
        if self.columns is None:
            # TODO: originally -> self.describe()
            # When working with Saleforce this will throw an exception.  Need
            # some logic to alter the "refresh" parameter on the fly.
            self.describe(refresh=True)

        # bulkipy takes care of the API interactions
        try:
            query = 'SELECT ' + self.columnString() + ' FROM ' + self.objectName
            if self.incremental:
                lastUpdate = self.getLastUpdate()
                if lastUpdate is None:
                    self.incremental = False
                else:
                    query += ' WHERE ' + lastUpdateField + ' > ' + str(self.getLastUpdate())
            batchId = self.sf.query(self.job, query)
        except Exception as sfErr:
            msg = 'SFHelper.query: Failure in executing query batch. Details: ' + str(sfErr)
            logging.error(msg)
            if self.sd:
                self.sd.logEvent(msg, severity='ERROR', jobstatus='STOPPED')
            raise StandardError(msg)

        # wait for the job to complete
        self._wait(batchId)

        # pull all of the results back
        results = self.sf.get_all_results_for_batch(batch_id=batchId, job_id=self.job, parse_csv=True)
        results = list(list(x) for x in results)

        # export to a csv file
        with open(self.objectName + '.csv', 'wb') as csvfile:
            wr = csv.writer(csvfile, quoting=csv.QUOTE_MINIMAL)
            for x in results[0]:
                wr.writerow(x)

    def status(self):
        """grab the status of the batches within the finished job"""
        count = 0
        f = open('Errors.txt', "a+")
        f.write('Header Row\n')
        if self.batchIds:
            for batch in self.batchIds:
                logging.info('SFHelper.status: Retrieving status for batch: ' + str(batch))
                flag = self.sf.get_upload_results(self.job, batch, callback=self.saveResults)
                details = []
                error = False
                for row in self.batchResults:
                    status = {"batchId": row.id,
                              "error": row.error}
                    if row.created != 'Created':
                        count += 1
                    if row.created == 'false':
                        error = True
                        details.append(status)
                        f.write(str(count) + ',' + str(row.error))
                        f.write('\n')
        f.close()

        if error:
            raise StandardError('SFHelper.status: Call was successful, but some rows failed. Details: ' + json.dumps(details))

    def finish(self):
        """close out/cleanup a batch job process"""
        if self.isStarted:
            self.sf.close_job(self.job)
            self.isStarted = False

    def syncTable(self, objectName='', keyFields=["id"], lastUpdateField='systemmodstamp'):
        """
        sync the salesforce object to the destination mart table
        """
        if self.maintainState:
            js = JobState(self.config, 'salesforce', self.jobId)

        if objectName == '':
            # we're not passing in an object, so use the one defined in the constructor
            objectName = self.objectName
        else:
            # replace the constructor defined name with a new one
            self.setObjectName(objectName)
            self.describe()
        try:
            s = SQLHelper.factory(self.config, self.config['database-type'])
            tableName = objectName.lower()
            # make sure the table is there first
            if not s.checkExist(tableName):
                s.createTable(tableName, self.getSchema())
                # regardless of caller, if the table did not exist, it's not incremental
                self.incremental = False
            else:
                # empty the table if it's not an incremental load
                if not self.incremental:
                    # truncate the table for now
                    s.truncateTable(tableName)
            # pull the data
            self.start('query')
            self.query()
            self.finish()
            if self.incremental:
                # define a unique temp table name
                tempTable = tableName + '_' + str(self.jobId).replace('-', '_')
                # create the temp table
                s.createTable(tempTable, self.getSchema())
                # load the pulled data into the temp table
                s.loadDataFromFile(tempTable, objectName + '.csv')
                # update the current table with the temp table data
                s.updateTable(tableName, self.getSchema(), tempTable, keyFields)
                # delete the temp table
                s.deleteTable(tempTable)
            else:
                # load the data
                s.loadDataFromFile(tableName, objectName + '.csv')
            # update the metadata
            self.setLastUpdate(lastUpdateField=lastUpdateField)
            # cleanup locally
            os.remove(objectName + '.csv')
        except Exception as dbErr:
            msg = 'SFHelper.syncTable: Error/Issue moving \'' + objectName + '\' into the mart. Details: ' + str(dbErr)
            logging.error(msg)
            if self.sd:
                self.sd.logEvent(msg, severity='ERROR', jobstatus='END')
            if self.maintainState:
                js.changeStatus('ERROR')
            raise StandardError(msg)

        # notify of completion
        if self.maintainState and objectName != '':
            js.changeStatus('COMPLETE')

        msg = 'SFHelper.syncTable(): Completed salesforce sync for \'' + objectName + '\' (async follow-up)'
        logging.info(msg)
        if self.sd:
            self.sd.logEvent(msg, severity='INFO', jobstatus='END')

    def syncAllTables(self):
        """
        loop through the manifest and load all of the tables
        """
        for row in self.manifest:
            # call sync table
            self.syncTable(objectName=row['object-name'], keyFields=row['key-fields'], lastUpdateField=row['last-update-field'])

        # notify of completion
        if self.maintainState:
            js = JobState(self.config, 'salesforce', self.jobId)
            js.changeStatus('COMPLETE')

        msg = 'SFHelper.syncAllTables(): Completed salesforce sync for all requested objects (async follow-up)'
        logging.info(msg)
        if self.sd:
            self.sd.logEvent(msg, severity='INFO', jobstatus='END')

    def updateAllTables(self):
        """
        loop through the manifest and update the table layouts if they have changed
        then perform the sync action from syncAllTables
        """
        etl = ETLHelper(self.config)
        s = SQLHelper.factory(self.config, self.config['database-type'])
        for row in self.manifest:
            drop = False
            # replace the constructor defined name with a new one
            self.setObjectName(row['object-name'])

            # make sure there is a local schema first
            if etl.checkSchemaExist(row['object-name'].lower()):
                # if there is a change, drop and recreate the table
                if self.checkForChange():
                    drop = True
            else:
                drop = True

            # update the schema file/definition
            self.describe(refresh=True)

            try:
                # drop if needed
                if drop:
                    if s.checkExist(row['object-name'].lower()):
                        s.deleteTable(row['object-name'].lower())
            except Exception as i2apErr:
                msg = 'SFHelper.updateAllTables: Error/Issue dropping ' + objectName + '\' for replace. Details: ' + str(i2apErr)
                logging.error(msg)
                if self.sd:
                    self.sd.logEvent(msg, severity='ERROR', jobstatus='INPROGRESS')
                if self.maintainState:
                    js.changeStatus('ERROR')
                raise StandardError(msg)

            # sync will take care of the create table
            self.syncTable(objectName=row['object-name'], keyFields=row['key-fields'], lastUpdateField=row['last-update-field'])

        # notify of completion
        if self.maintainState:
            js = JobState(self.config, 'salesforce', self.jobId)
            js.changeStatus('COMPLETE')

        msg = 'SFHelper.updateAllTables(): Completed salesforce drop/recreate for all requested objects (async follow-up)'
        logging.info(msg)
        if self.sd:
            self.sd.logEvent(msg, severity='INFO', jobstatus='END')
