#!/usr/bin/env python
import pandas as pd
import uuid
import os
import io

from StorageHelper import StorageHelper
from SQLHelper import SQLHelper
from LogHelper import LogHelper
from JobState import JobState

class Transform:
    """Process and transform source data exports into bigquery"""

    def __init__(self, config, objectName, fileName='', params=[], disposition='WRITE_APPEND', jobId='', dbType='bigquery', maintainState=False):
        """constructor"""
        self.project = config['project-id']
        self.bucket = config['stage-bucket']
        self.archiveBucket = config['archive-bucket']
        self.logfile = config['logfile']
        self.fileName = fileName
        if fileName == '':
            self.type = 'SQL'
        else:
            self.type = 'FILE'
        self.params = params
        self.dataset = config['dataset']
        self.objectName = objectName
        self.disposition = disposition
        if jobId == '':
            self.jobId = uuid.uuid4()
        else:
            self.jobId = jobId

        self.config = config
        self.maintainState = maintainState
        self.gs = StorageHelper.factory(self.config, jobId=self.jobId)
        self.bq = SQLHelper.factory(self.config, dbType, jobId=self.jobId)
        self.logger = LogHelper.factory(self.config['project-id'], type="filelog", jobId=self.jobId,
                                   destfile=logfile)
        self.ddlDir = config['ddl-directory']
        self.sqlDir = config['sql-directory']

    def pullFile(self):
        """pull the source file into a dataframe for transform"""
        # pull the file from google storage into an in memory dataframe
        self.df = pd.read_csv(io.StringIO(self.gs.streamFile(self.fileName).decode('ISO-8859-1')), dtype=object, keep_default_na=False)

    def archiveFile(self):
        """archive the processed file"""
        self.gs.moveFile(self.fileName, destBucket=self.archiveBucket, destFile=self.fileName)

    def addTransformations(self):
        """create new columns, etc to prepare the data for salesforce load"""
        # set the identifier for this job - MAKE SURE THIS IS IN YOUR DDL
        self.df['job_id'] = str(self.jobId)

        # TODO - add some transformations. Do something with the data.
        # EXAMPLE BASE TRANSFORMATION: set the categoy hash for eventual pull and load
        # NOTE: EACH DESIRED FIELD WILL NEED TO BE ADDED TO THE OBJECT'S DDL
        # self.df['category_id'] = 'A-1'

        # more transformations to be coded here
        # ...

    def loadFile(self):
        """take the dataframe and load it to big query"""
        # write the data from memory to disk
        self.df.to_csv(self.fileName + '-out.csv', index=False, header=False)
        # create the bigquery table from the salesforce schema (if table is already there, this will do nothing)
        self.bq.createTable(self.dataset, self.objectName, self.bq.getSchemaFromFile(self.objectName))
        # load the data to bigquery
        self.bq.loadDataFromFile(self.dataset, self.objectName, self.fileName + '-out.csv', writeDisp=self.disposition)
        # clean up the local version of the file
        os.remove(self.fileName + '-out.csv')

    def extractAsFile(self, category):
        """create a file from big query by category for load to salesforce"""
        # build the category query
        # TODO - replace * below with actual sf field names, list
        sql = 'SELECT * FROM ' + self.dataset + '.' + self.objectName\
        # TODO - and probably want some reduction condition...
        # + ' WHERE category_id = \'' + category + '\''
        # create a temp table containing the category only data for the object
        self.bq.createTableAs('temp_' + category, sql)
        # save that file out to GCS
        self.bq.exportDataToStorage('temp_' + category, 'gs://' + self.bucket + '/' + category + '.csv')

    def pullSQL(self):
        """pull the SQL file and apply the parameters"""
        with open(self.sqlDir + '/' + self.objectName + '.sql') as sqlFile:
            self.sourceSQL = sqlFile.read().replace('\n', ' ')
        # replace the dataset
        self.sourceSQL = self.sourceSQL.replace('{{DATASET}}', self.dataset)
        # loop through the parameters and apply
        for parm in self.params:
            self.sourceSQL = self.sourceSQL.replace('{{' + parm['key'].upper() + '}}', str(parm['value']))
        # check to see if the template was filled
        if self.sourceSQL.find('{') != -1 or self.sourceSQL.find('}') != -1:
            raise StandardError("Transform.pullSQL(): At least one parameter was not supplied in the call")

        # execute the query into a dataframe
        self.df = self.bq.getQueryAsDataframe(self.sourceSQL)

    def dbToDb(self):
        """move data from a database query into another table at frequency"""
        # grab the needed SQL statement
        self.pullSQL()
        # change it up as needed
        self.addTransformations()
        # push the data into bigQuery
        self.loadFile()
        # notify of completion
        if self.maintainState:
            js = JobState(self.config, 'processor', self.jobId)
            js.changeStatus('COMPLETE')

        msg = 'Transform.dbToDb(): Completed transformation processing (async follow-up)'
        self.logger.logEvent(msg, severity='INFO', jobstatus='END')

    def fileToDb(self):
        """run the ETL flow"""
        # grab the file for processing
        self.pullFile()
        # change it up as needed
        self.addTransformations()
        # push the data into bigQuery
        self.loadFile()
        # archive the remote version of the file
        self.archiveFile()
        # notify of completion
        msg = 'Transform.process(): Completed transformation processing (async follow-up)'
        self.logger.logEvent(msg, severity='INFO', jobstatus='END')
