from behave import *
import sys
import os
import csv
class_path = os.environ['I2AP_LIB_DIRECTORY']
sys.path.append(class_path)

from SFHelper import SFHelper
from SQLHelper import SQLHelper
from RemoteDebug import *

#
# Scenario: Pull object from salesforce into a csv file
#
@Given("there is a salesforce object")
def prepare(context):
    # initialize
    context.objectName = 'Account'
    context.sf = SFHelper(context.config, context.objectName)

@When("a salesforce export call is initiated")
def runthrough(context):
    context.sf.start('query')
    context.sf.query()
    context.sf.finish()

@Then("a csv file containing the object is produced")
def evaluate(context):
    # TODO how to independently count rows in an SF object?
    with open(context.objectName + '.csv', 'r') as csvfile:
        rd = csv.reader(csvfile)
        count = sum(1 for row in rd)
    assert count > 1
    os.remove(context.objectName + '.csv')

#
# Scenario: Place pulled object into database
#
@Given("there is a salesforce csv")
def prepare(context):
    # initialize
    context.objectName = 'Account'
    context.config['dataset'] = context.featurePrefix
    context.sf = SFHelper(context.config, context.objectName)
    context.s = SQLHelper.factory(context.config, context.config['database-type'])
    if not context.s.checkDatasetExist(context.featurePrefix):
        context.s.createDataset(context.featurePrefix)
    if context.s.checkExist(context.objectName):
        context.s.deleteTable(context.objectName)

@When("a sync call is initiated")
def runthrough(context):
    context.sf.syncTable()

@Then("a table containing the data from the csv is produced")
def evaluate(context):
    sql = 'select count(*) cnt from ' + context.featurePrefix + '.' + context.objectName
    d = context.s.getQueryAsDict(sql)
    assert int(d[0]['cnt']) > 1
