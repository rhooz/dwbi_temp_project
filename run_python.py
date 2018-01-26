# create temp table
# TODO: set up env variables
from lib import *
import os

config = { 'salesforce-user': 'tommy.martin@gettectonic.com.devwynn',
           'salesforce-password': "1 is the loneliest...",
           'salesforce-token': 'bnNBltsTe0QgNoCXzb5WzaOj1',
           'salesforce-sandbox': True,
           'database-type': 'oracle',
           'database-server': '172.18.0.1',
           'database-user': 'readonlyuser',
           'database-password': 'MyPassword',
           'database-sid': 'RHTestDB',
           'database-port': 1521 }


objectName = 'Contact'

sfh = SFHelper(config=config, objectName=objectName, incremental=False)

schema = sfh.getSchema(persist=True)

orh = ORHelper(config=config)

# TODO: what are we going to do with the temp table? truncate every time? create and delete every time?
new_table = 'tempContact'

if False: # create new table
    orh.createTable(new_table, schema)

# get salesforce data
sfh.query()

# insert from csv
fileName = 'Contact.csv'
orh.loadDataFromFile(tableName=new_table, fileName=fileName, schema=schema)
# TODO: Is the data in Oracle???

# Run update
# TODO: Nothing has been done after this point

# TODO: create new oracle Contact table (this is the main table we will be reporting from)

tableName = 'testTable'
keyFieldList = []

# TODO: check that the updateTable function works
updateTable(tableName, schema, newTable, keyFieldList)

# TODO: look in SFHelper and ORHelper to see any other TODOs
