from behave import *
import sys
import os
import requests
import json
import time

class_path = os.environ['I2AP_LIB_DIRECTORY']
sys.path.append(class_path)

from BQHelper import BQHelper

@Given("I have email object source data")
def prepare(context):
    # initialize
    context.bq = BQHelper(context.project)
    # cleanup
    context.bq.deleteTable(context.dataset, 'test')
    # pull in the header configuration information
    context.url = 'http://localhost:3101/DbToDb'
    # set the query parameters
    context.payload = {'object-name': 'test',
                       'query_params':
                            [{'key':'EMAIL_GROUP_ID',
                            'value': '100002'}]
                       }
    context.headers = {'Content-Type': 'application/json',
               'Tt-I2ap-Id': os.environ['I2AP_CLIENT_ID'],
               'Tt-I2ap-Sec': os.environ['I2AP_CLIENT_SECRET']}

@When("I execute a call to transform that data")
def runthrough(context):
    resp = requests.post(context.url, data=json.dumps(context.payload), params=context.params, headers=context.headers)
    # TO-DO need a status method since its an async call, sleep for now
    time.sleep(10)

@Then("A table is created with those results")
def evaluate(context):
    cnt = context.bq.getRowCount(context.dataset, 'test')
    assert cnt == 1
