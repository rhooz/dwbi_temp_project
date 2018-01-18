import tornado.ioloop
import tornado.web
from tornado.options import options, parse_command_line
import logging
import os

from WelcomeService import WelcomeService
from FileToDbService import FileToDbService
from DbToDbService import DbToDbService
from EmailService import EmailService
from GraphicService import GraphicService
from LinkService import LinkService
from ReplicatorService import ReplicatorService
from SyncService import SyncService

from RemoteDebug import *

def pullConfig():
    """Pull the environment configuration variables into a dictionary"""
    config={}
    config['port'] = os.environ['I2AP_PORT']
    config['override-port'] = os.environ['I2AP_OVERRIDE_PORT']
    config['project-id'] = os.environ['I2AP_PROJECT_ID']
    config['stage-bucket'] = os.environ['I2AP_STAGE_BUCKET']
    config['archive-bucket'] = os.environ['I2AP_ARCHIVE_BUCKET']
    config['state-bucket'] = os.environ['I2AP_STATE_BUCKET']
    config['graph-bucket'] = os.environ['I2AP_GRAPH_BUCKET']
    config['dataset'] = os.environ['I2AP_DATASET']
    config['ddl-directory'] = os.environ['I2AP_DDL_DIRECTORY']
    config['sql-directory'] = os.environ['I2AP_SQL_DIRECTORY']
    config['lib-directory'] = os.environ['I2AP_LIB_DIRECTORY']
    config['client-secret'] = os.environ['I2AP_CLIENT_SECRET']
    config['client-id'] = os.environ['I2AP_CLIENT_ID']
    config['data-service'] = os.environ['I2AP_DATA_SERVICE']
    config['email-id'] = os.environ['I2AP_EMAIL_ID']
    config['email-name'] = os.environ['I2AP_EMAIL_NAME']
    config['email-password'] = os.environ['I2AP_EMAIL_PASSWORD']
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
    return config

def main():
    # bring in application configuration parameters
    config = pullConfig()
    # open up a port for to handle data-processing requests
    options.logging="info"
    parse_command_line()
    logging.info('I2AP Processing Service; starting up...')
    app = tornado.web.Application([
        ('/FileToDb', FileToDbService, dict(config=config)),
        (r'/DbToDb/(?P<jobId>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/?', DbToDbService, dict(config=config)),
        ('/DbToDb', DbToDbService, dict(config=config)),
        (r'/Sync/(?P<jobId>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/?', SyncService, dict(config=config)),
        ('/Sync', SyncService, dict(config=config)),
        ('/', WelcomeService, dict(config=config))
    ])
    try:
        if config['override-port'] != '':
            logging.info("Processing Port overidden: " + str(config['override-port']))
            listenPort = config['override-port']
        else:
            logging.info("Listening on default port: " + str(config['port']))
            listenPort = config['port']
    except:
        listenPort = config['port']

    # start the server
    app.listen(listenPort)
    tornado.ioloop.IOLoop.current().start()

if __name__ == '__main__':
    main()

