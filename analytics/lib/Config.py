import os

class Config:
    """
    Handles activities around i2ap configuration
    """
    def __init__(self):
        """
        constructor
        """
        self._pull()

    def _pull(self):
        """
        Pull the environment configuration variables into a dictionary
        """
        config = {}
        # try everything and default if its not found in the environment
        try:
            config['port'] = os.environ['I2AP_PORT']
        except:
            config['port'] = ''
        try:
            config['override-port'] = os.environ['I2AP_OVERRIDE_PORT']
        except:
            config['override-port'] = ''
        try:
            config['project-id'] = os.environ['I2AP_PROJECT_ID']
        except:
            config['project-id'] = ''
        try:
            config['stage-bucket'] = os.environ['I2AP_STAGE_BUCKET']
        except:
            config['stage-bucket'] = ''
        try:
            config['archive-bucket'] = os.environ['I2AP_ARCHIVE_BUCKET']
        except:
            config['archive-bucket'] = ''
        try:
            config['state-bucket'] = os.environ['I2AP_STATE_BUCKET']
        except:
            config['state-bucket'] = ''
        try:
            config['graph-bucket'] = os.environ['I2AP_GRAPH_BUCKET']
        except:
            config['graph-bucket'] = ''
        try:
            config['dataset'] = os.environ['I2AP_DATASET']
        except:
            config['dataset'] = ''
        try:
            config['ddl-directory'] = os.environ['I2AP_DDL_DIRECTORY']
        except:
            config['ddl-directory'] = ''
        try:
            config['sql-directory'] = os.environ['I2AP_SQL_DIRECTORY']
        except:
            config['sql-directory'] = ''
        try:
            config['lib-directory'] = os.environ['I2AP_LIB_DIRECTORY']
        except:
            config['lib-directory'] = ''
        try:
            config['client-secret'] = os.environ['I2AP_CLIENT_SECRET']
        except:
            config['client-secret'] = ''
        try:
            config['client-id'] = os.environ['I2AP_CLIENT_ID']
        except:
            config['client-id'] = ''
        try:
            config['data-service'] = os.environ['I2AP_DATA_SERVICE']
        except:
            config['data-service'] = ''
        try:
            config['email-id'] = os.environ['I2AP_EMAIL_ID']
        except:
            config['email-id'] = ''
        try:
            config['email-name'] = os.environ['I2AP_EMAIL_NAME']
        except:
            config['email-name'] = ''
        try:
            config['email-password'] = os.environ['I2AP_EMAIL_PASSWORD']
        except:
            config['email-password'] = ''
        try:
            config['database-type'] = os.environ['I2AP_MART_TYPE']
        except:
            config['database-type'] = ''
        try:
            config['database-server'] = os.environ['I2AP_MART_SERVER']
        except:
            config['database-server'] = ''
        try:
            config['database-port'] = os.environ['I2AP_MART_PORT']
        except:
            config['database-port'] = ''
        try:
            config['database'] = os.environ['I2AP_MART_NAME']
        except:
            config['database'] = ''
        try:
            config['database-user'] = os.environ['I2AP_MART_USER']
        except:
            config['database-user'] = ''
        try:
            config['database-password'] = os.environ['I2AP_MART_PASSWORD']
        except:
            config['database-password'] = ''
        try:
            config['log-name'] = os.environ['I2AP_LOG_NAME']
        except:
            config['log-name'] = ''
        try:
            config['salesforce-user'] = os.environ['I2AP_SALESFORCE_USER']
        except:
            config['salesforce-user'] = ''
        try:
            config['salesforce-password'] = os.environ['I2AP_SALESFORCE_PASSWORD']
        except:
            config['salesforce-password'] = ''
        try:
            config['salesforce-token'] = os.environ['I2AP_SALESFORCE_TOKEN']
        except:
            config['salesforce-token'] = ''
        try:
            config['storage-type'] = os.environ['I2AP_STORAGE_TYPE']
        except:
            config['storage-type'] = ''
        try:
            config['storage-project'] = os.environ['I2AP_STORAGE_PROJECT']
        except:
            config['storage-project'] = ''
        try:
            config['storage-key'] = os.environ['I2AP_STORAGE_KEY']
        except:
            config['storage-key'] = ''
        try:
            config['storage-secret'] = os.environ['I2AP_STORAGE_SECRET']
        except:
            config['storage-secret'] = ''
        try:
            config['storage-user'] = os.environ['I2AP_STORAGE_USER']
        except:
            config['storage-user'] = ''
        try:
            config['storage-password'] = os.environ['I2AP_STORAGE_PASSWORD']
            if os.environ['I2AP_SALESFORCE_SANDBOX'] == "True":
                config['salesforce-sandbox'] = True
            else:
                config['salesforce-sandbox'] = False
        except:
            config['salesforce-sandbox'] = False

        self.config = config

    def get(self):
        """
        return the config dictionary
        :return: config
        """
        return self.config