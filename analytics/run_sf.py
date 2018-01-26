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

def test(**config):
    print type(config)
    for key, value in config.iteritems():
        print key, value

class Test(object):

    _i = 3
    h = 100

    @property
    def i(self):
        return type(self)._i

    @i.setter
    def i(self,val):
        self.__class__._i = val


class Test(object): pass
t = Test()
print type(t) is t.__class__
print type(t)

class Test(): pass
t = Test()
print type(t) is t.__class__
print type(t)
