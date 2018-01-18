import tornado.web
from socket import gethostname

class WelcomeService(tornado.web.RequestHandler):
    def initialize(self, config):
        self.config = config

    def get(self):
        """
        Verify the service is up and running/responding

        Example curl:
            curl http://localhost:<port>/Welcome
        """
        self.write('Welcome to the Service running on: ' + gethostname() + '\n')
        self.finish()