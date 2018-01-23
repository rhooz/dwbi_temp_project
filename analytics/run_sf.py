import os
from lib.SFHelper import *



if __name__ == '__main__':
    config = { 'salesforce-user': os.environ['SF_USERNAME_SB'],
               'salesforce-password': os.environ['SF_PASSWORD_SB'],
               'salesforce-token': os.environ['SF_SECURITY_TOKEN_SB'],
               'salesforce-sandbox': True }

    objectName = 'Contact'
    sfh = SFHelper(config=config, objectName=objectName, incremental=False)

    # sfh.describe(refresh=True)

    sfh.query()
