#!/usr/bin/env python

from SQLBase import SQLBase
import os
import logging

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
            returnType = 'NVARCHAR2(2000)'
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
            returnType = 'NVARCHAR2(2000)'
        return returnType

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
        # TODO: set up createDataset for schema
        sql = 'CREATE TABLE ' + tableName + ' ('
        # build a DML statement to create the table by looping through the i2ap schema list dictionary
        needComma = False
        for column in schema:
            if needComma:
                sql += ', '
            sql += column['name'] + ' ' + self.convertType(column['type'])
            needComma = True
        sql += ')'
        self._createTable(tableName, sql, shard=shard, recreate=recreate)

    def loadDataFromFile(self, tableName, fileName, schema, writeDisp='WRITE_APPEND', fileFormat='csv'):
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

        # TODO: test truncate (does it work with Oracle?)
        if writeDisp == 'WRITE_TRUNCATE':
            self.truncateTable(tableName)

        # load the file
        try:
            # 1. open csv
            import csv

            with open("Contact.csv","r") as csv_file:
                reader = csv.reader(csv_file)
                lines=[]
                for line in reader:
                    lines.append(line)

            # 2. get column types
            col_types = { col['name']: self.convertType(col['type']) for col in schema }

            # 3. generate sql
            cols = lines[0]
            for row_number in range(1, len(lines)):
                temp_row = []
                for item_number in range(len(lines[row_number])):
                    col_type = col_types[cols[item_number]]
                    temp_val = lines[row_number][item_number]
                    if temp_val == '':
                        temp_val = 'NULL'
                    elif col_type == 'DATE' or col_type == 'DATETIME' or col_type == 'TIMESTAMP':
                        temp_val = temp_val[:19].replace('T', ' ')
                        temp_val = "TO_DATE('" + temp_val + "', 'yyyy/mm/dd hh24:mi:ss')"
                    elif col_type == 'NVARCHAR2(2000)':
                        temp_val = "'" + temp_val + "'"
                    lines[row_number][item_number] = temp_val

            inserts = []
            for line in lines[1:]:
                sql = 'INSERT INTO ' + tableName + ' (' + ','.join(lines[0]) + ') VALUES ('
                sql += ','.join(lines[1]) + ')'
                inserts.append(sql)

            cur = self.connection.cursor()
            for i in inserts:
                cur.execute(i)
                self.connection.commit()

            cur.close()
            # do you want to insert every column or drop columns with no data?

            # '''
            # take columns from line 1 of csv...
            # '''
            # sql = 'INSERT INTO ' + tableName + ' (' + ','.join(lines[0][:9]) + ') VALUES ('
            # sql = 'INSERT INTO ' + tableName + ' (' + lines[0][8] + ') VALUES (
            # sql += "TO_DATE('2003/05/03 21:02:44', 'yyyy/mm/dd hh24:mi:ss') )"
            #
            # for n in range(1, len(cols.split(',')[10]) + 1):
            #     sql += ':' + str(n) + ','
            #
            # for n in range(1, len(cols.split(',')) + 1):
            #     sql += ':' + str(n) + ','
            #
            # sql = sql[:-1] + ')'
            #
            # # sql = sql.replace(',,', "''")
            #
            # # cur.execute(sql, lines[1][:17])
            # insert = []
            # for n, val in enumerate(lines[0][:9]):
            #     q = lines[1][n]
            #     if type(q) == str and 'date' in lines[0][n].lower():
            #         if q == '':
            #             q = "TO_DATE('2003/05/03 21:02:44', 'yyyy/mm/dd hh24:mi:ss')""
            #         else:
            #             q = q.replace('T', ' ').replace('-', '/')[:10]
            #     elif q == '':
            #         q = None
            #     insert.append(q)
            #
            # cur.execute(sql, insert)
            #
            # cur.execute(sql, [ lines[0][n] if x != '' else None for n, val in enumerate(lines[0]])
            # cur.close()
            # self.connection.commit()
            # f.close()
        except Exception as dbErr:
            # TODO: Does throwing an exception work?
            msg = 'SQLBase.loadDataFromFile: Error loading file ' + fileName + ' into table ' + tableName + '. Details: ' + str(dbErr)
            logging.error(msg)
            raise StandardError(msg)

        # clean up temp table
        # if skipRows >= 1:
        #     os.remove(fileName)

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
