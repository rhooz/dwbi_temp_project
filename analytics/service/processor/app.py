import tornado.ioloop
import tornado.web
from tornado.options import options, parse_command_line
import logging
import os

from WelcomeService import WelcomeService
from FileToDbService import FileToDbService
from DbToDbService import DbToDbService
#from EmailService import EmailService
#from GraphicService import GraphicService
#from LinkService import LinkService
#from ReplicatorService import ReplicatorService
from SyncService import SyncService
#from ObjectService import ObjectService
from Config import Config

from RemoteDebug import *

def main():
    # bring in application configuration parameters
    c = Config()
    config = c.get()
    # open up a port for to handle data-processing requests
    options.logging="info"
    parse_command_line()
    logging.info('I2AP Processing Service; starting up...')
    app = tornado.web.Application([
        #('/Replicate', ReplicatorService, dict(config=config)),
        ('/FileToDb', FileToDbService, dict(config=config)),
        (r'/DbToDb/(?P<jobId>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/?', DbToDbService, dict(config=config)),
        ('/DbToDb', DbToDbService, dict(config=config)),
        #('/Email', EmailService, dict(config=config)),
        #(r'/Object/(?P<objectName>\w+)/?', ObjectService, dict(config=config)),
        #('/Object', ObjectService, dict(config=config)),
        (r'/Sync/(?P<jobId>[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})/?', SyncService, dict(config=config)),
        ('/Sync', SyncService, dict(config=config)),
        #('/Graphic', GraphicService, dict(config=config)),
        #('/Link', LinkService, dict(config=config)),
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
