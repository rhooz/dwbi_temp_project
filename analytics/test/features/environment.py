import sys
import os

class_path = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../lib'))
sys.path.append(class_path)
#from GSHelper import *
#from BQHelper import *


# from ddlFlow import *

def before_all(context):
    context.project = os.environ['I2AP_PROJECT_ID']
    context.stageBucket = os.environ['I2AP_STAGE_BUCKET']
    context.bucket=context.stageBucket
    context.archiveBucket = os.environ['I2AP_ARCHIVE_BUCKET']
    context.dataset = os.environ['I2AP_DATASET']
    context.ddlDir = os.environ['I2AP_DDL_DIRECTORY']
    context.libDir = os.environ['I2AP_LIB_DIRECTORY']
    context.featurePrefix = os.environ['I2AP_FEATURE_PREFIX']
    context.dataDir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../data'))
    context.ddlDir = os.path.abspath(os.path.join(os.path.dirname(os.path.abspath(__file__)), '../../ddl'))

    config = dict()
    config['project-id'] = context.project
    config['stage-bucket'] = context.bucket
    config['archive-bucket'] = context.archiveBucket
    config['dataset'] = context.dataset
    config['ddl-directory'] = os.environ['I2AP_DDL_DIRECTORY']
    config['sql-directory'] = os.environ['I2AP_SQL_DIRECTORY']
    config['lib-directory'] = os.environ['I2AP_LIB_DIRECTORY']
    config['database-type'] = os.environ['I2AP_MART_TYPE']
    config['database-server'] = os.environ['I2AP_MART_SERVER']
    config['database-port'] = os.environ['I2AP_MART_PORT']
    config['database'] = os.environ['I2AP_MART_NAME']
    config['database-user'] = os.environ['I2AP_MART_USER']
    config['database-password'] = os.environ['I2AP_MART_PASSWORD']
    config['log-name'] = os.environ['I2AP_LOG_NAME']
    config['salesforce-user'] = os.environ['I2AP_SALESFORCE_USER']
    config['salesforce-password'] = os.environ['I2AP_SALESFORCE_PASSWORD']
    config['salesforce-token'] = os.environ['I2AP_SALESFORCE_TOKEN']
    if os.environ['I2AP_SALESFORCE_SANDBOX'] == "True":
        config['salesforce-sandbox'] = True
    else:
        config['salesforce-sandbox'] = False
    context.config = config
    #context.gs = GSHelper(context.project)
    #context.bq = BQHelper(context.config)
