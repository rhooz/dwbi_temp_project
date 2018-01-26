from messytables import CSVTableSet, type_guess, types
import re
import shutil
import datetime
import codecs
import json
import logging
import os

class ETLHelper:
    """some tectonic help functions for the Python ETL stack"""
    def __init__(self, config):
        self.ddlDir = config['ddl-directory']

    def fetchTypes(self, localCSVFile):
        """derive the column types for the passed in csv file"""

        # open the file
        fileHandle = open(localCSVFile, 'rb')
        # convert file into messytables rowset
        rowSet = CSVTableSet(fileHandle).tables[0]
        # return the ordered list of types
        return type_guess(rowSet.sample,
                [types.DecimalType, types.IntegerType, types.DateUtilType, types.StringType])

    def convertType(self, messyType):
        """convert from messytables type string to BigQuery"""
        bigQType = ''
        if str(messyType) == 'String':
            bigQType = 'STRING'
        elif str(messyType) == 'Integer':
            bigQType = 'INTEGER'
        elif str(messyType) == 'Decimal':
            bigQType = 'FLOAT'
        elif str(messyType) == 'Float':
            bigQType = 'FLOAT'
        elif str(messyType) == 'DateUtil':
            bigQType = 'TIMESTAMP'
        return bigQType

    def conformHeaderRow(self,localCSVFile):
        """"replace special characters in the header with underscores (_)"""
        # open up the csv
        inFile=open(localCSVFile,'rb')
        # open up the temp csv
        outFile=open('temp-'+localCSVFile,'wb')
        # pull off the header row
        header=inFile.readline()
        # transform it
        header=str.lower(re.sub(r'[^\x00-\x7F]+|\r','_',header))
        columns=header
        # write the header
        outFile.write(header)
        # write the rest of the file
        shutil.copyfileobj(inFile, outFile)
        # clean them up
        inFile.close()
        outFile.close()
        # rename the file
        shutil.move('temp-'+localCSVFile,localCSVFile)
        return columns

    def schemaStringToDict (self, columns, columnTypes, overrideTypes):
        """convert a PETL column string into a dictionary of columns: RETURNS dict"""
        #target schema "bigQ" or empty
        # create a list to house the GCP fields
        fields = []
        # counter to march through the types list
        columnCnt = 0
        # split the column srting by field and build out the dictionary
        for column in columns:
            # determine a type was specified in Trifacta, if not, use the messyType
            if column in overrideTypes:
                newType = overrideTypes[column]
            else:
                newType = columnTypes[columnCnt]
            field = {'mode': 'NULLABLE',
                     'name': column,
                     'type': self.convertType(newType)}
            fields.append(field)
            columnCnt += 1
        # create a dictiorary to hold the schema
        schema = {'fields': fields}
        return schema

    def csvStringtoFile(self, csvString, fileName):
        """place a csv string into a file"""
        with codecs.open(fileName, 'w', 'utf-8') as csvFile:
            csvFile.write(csvString)

    def jsonToJsonLines(self, inFileName, outFileName='', forAllRows=None):
        """convert a json file into BQ loadable jsonlines file"""
        # derive the output filename
        if outFileName == '':
            outFileName = inFileName.split('.')[0] + '.jsonl'

        # pull in the standard json source
        with open(inFileName, 'r') as inFile:
            jsonData = json.load(inFile)

        # export just records with new lines
        with open(outFileName, 'w') as outFile:
            for each in jsonData:
                # if there are additional fields to add/denormalize, add them here
                if forAllRows is not None:
                    each.update(forAllRows)
                json.dump(each, outFile)
                outFile.write('\n')

    def checkSchemaExist(self, tableName):
        """
        check if a schema files exists in this i2ap environment
        :param tableName: The name of the i2ap object to check
        :return: boolean
        """
        if os.path.isfile(self.ddlDir + '/' + tableName + '.json'):
            return True
        else:
            return False

    def schemaToFile(self, tableName, fieldSchema):
        """
        take the fields section of an i2ap schema file and persist into a ddl file
        :param tableName: the name of the table/name of the field schema
        :param fieldSchema: dictionary containing i2ap fields from a table
        """
        # assemble the wider schema
        schema = {}
        partition = [
                        { "shard": False }
                    ]
        schema['fields'] = fieldSchema
        schema['partition'] = partition

        try:
            # persist the schema in a ddl file
            with open(self.ddlDir + '/' + tableName + '.json', 'w') as outfile:
                json.dump(schema, outfile)
        except Exception as jsonErr:
            msg = 'ETLHelper.schemaToFile: Error exporting schema dictionaty into DDL file. Details: ' + str(jsonErr)
            logging.info(msg)
            raise StandardError(msg)
