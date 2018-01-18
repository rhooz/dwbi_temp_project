from behave import *
import sys
import os
import csv
class_path = os.environ['I2AP_LIB_DIRECTORY']
sys.path.append(class_path)

from BQHelper import BQHelper
from GSHelper import GSHelper
from RemoteDebug import *

#
# Scenario: Load data from a file into bigquery
#
@Given("there is a table definition with a file")
def prepare(context):
    # initialize
    context.bq = BQHelper(context.config)
    context.config['dataset'] = context.featurePrefix
    # cleanup
    if not context.bq.checkDatasetExist(context.config['dataset']):
        context.bq.createDataset(context.config['dataset'])
    context.bq.deleteTable('bq_test')
    context.bq.createTable('bq_test', context.bq.getSchemaFromFile('test'))

@When("a request is placed to upload the file")
def runthrough(context):
    context.bq.loadDataFromFile('bq_test', context.dataDir + '/bq_test.csv')

@Then("the file data is present in bigquery")
def evaluate(context):
    sql = 'SELECT email_id FROM ' + context.featurePrefix + '.bq_test'
    d = context.bq.getQueryAsDataframe(sql)
    assert len(d) == 3

#
# Scenario: Load data from GCS into bigquery
#
@Given("there is a table definition with a GCS file")
def prepare(context):
    # initialize
    context.bq = BQHelper(context.config)
    context.config['dataset'] = context.featurePrefix
    # setup / move file to GCS
    context.gs = GSHelper(context.config)
    context.gs.uploadFile(context.dataDir + '/bq_test_2.csv', context.bucket + '/bg_test_2.csv')

@When("a request is made to load from GCS")
def runthrough(context):
    context.bq.loadDataFromGCS('bq_test', context.bucket + '/bg_test_2.csv')

@Then("the GCS file data is present in bigquery")
def evaluate(context):
    sql = 'SELECT email_id FROM ' + context.featurePrefix + '.bq_test'
    d = context.bq.getQueryAsDataframe(sql)
    print(d)
    assert len(d) == 6
    context.gs.deleteFile(context.bucket, 'bg_test_2.csv')

#
# Scenario: Place data from bigquery into a file
#
@Given("there is a table in bigquery")
def prepare(context):
    # initialize
    context.bq = BQHelper(context.config)
    context.config['dataset'] = context.featurePrefix
    # setup / move file to GCS
    context.gs = GSHelper(context.config)
    try:
        os.remove('bq_test.csv')
        os.remove('bq_test_gcs.csv')
    except:
        pass

@When("a request to download table into a file")
def runthrough(context):
    context.bq.exportDataToFile('bq_test')

@When("a request to download table into GCS")
def runthrough_2(context):
    context.bq.exportDataToGCS('bq_test', context.bucket + '/bq_test_gcs.csv')

@Then("the file is present locally")
def evaluate(context):
    with open('bq_test.csv', 'r') as csvfile:
        rd = csv.reader(csvfile)
        count = sum(1 for row in rd)
    assert count == 7
    os.remove('bq_test.csv')

@Then("the file is present in GCS")
def evaluate_2(context):
    context.gs.downloadFile(context.bucket + '/bq_test_gcs.csv', 'bq_test_gcs.csv')
    with open('bq_test_gcs.csv', 'r') as csvfile:
        rd = csv.reader(csvfile)
        count = sum(1 for row in rd)
    assert count == 7
    os.remove('bq_test_gcs.csv')
    context.gs.deleteFile(context.bucket, 'bq_test_gcs.csv')