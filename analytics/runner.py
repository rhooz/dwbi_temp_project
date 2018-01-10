from Replicate import Replicate
import os

config = {}
config['port'] = os.environ['I2AP_PORT']
config['override-port'] = os.environ['I2AP_OVERRIDE_PORT']
config['project-id'] = os.environ['I2AP_PROJECT_ID']
config['stage-bucket'] = os.environ['I2AP_STAGE_BUCKET']
config['archive-bucket'] = os.environ['I2AP_ARCHIVE_BUCKET']
config['graph-bucket'] = os.environ['I2AP_GRAPH_BUCKET']
config['dataset'] = os.environ['I2AP_DATASET']
config['ddl-directory'] = os.environ['I2AP_DDL_DIRECTORY']
config['lib-directory'] = os.environ['I2AP_LIB_DIRECTORY']
config['client-secret'] = os.environ['I2AP_CLIENT_SECRET']
config['client-id'] = os.environ['I2AP_CLIENT_ID']
config['data-service'] = os.environ['I2AP_DATA_SERVICE']
config['email-id'] = os.environ['I2AP_EMAIL_ID']
config['email-name'] = os.environ['I2AP_EMAIL_NAME']
config['email-password'] = os.environ['I2AP_EMAIL_PASSWORD']

objectName = 'sf_Account'
sourceProject = 'tectonic-analytics'
sourceDataset = 'tectonic_commix'
destProject = 'tt-cust-analytics'
destDataset = 'commix_stage'

rep = Replicate(config, objectName, sourceProject, sourceDataset, destProject, destDataset)
rep.process()
