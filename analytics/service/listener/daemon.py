#!/usr/bin/env python

"""This application demonstrates polls Pub/Sub for GCS notifications, evaluates
the file type and then invokes the appropriate data processing service.
Note: If specified, it will ensure the message is only processed ONCE.
Note: This daemon processes PULL subscriptions only.

Usage:
  gcs indicates the daemon should process GCS notifications
  --project <google project id>
  --subscription <google pubsub subsription name>
"""

import argparse
import logging
import os
import sys

class_path = os.environ['I2AP_LIB_DIRECTORY']
sys.path.append(class_path)

from PSHelper import PSHelper


def pullConfig():
    """Pull the environment configuration variables into a dictionary"""
    config={}
    config['port'] = os.environ['I2AP_PORT']
    config['override-port'] = os.environ['I2AP_OVERRIDE_PORT']
    config['project-id'] = os.environ['I2AP_PROJECT_ID']
    config['stage-bucket'] = os.environ['I2AP_STAGE_BUCKET']
    config['archive-bucket'] = os.environ['I2AP_ARCHIVE_BUCKET']
    config['dataset'] = os.environ['I2AP_DATASET']
    config['client-id'] = os.environ['I2AP_CLIENT_ID']
    config['client-secret'] = os.environ['I2AP_CLIENT_SECRET']
    config['processor-service'] = os.environ['I2AP_PROCESSOR_SERVICE']
    config['listener-topic'] = os.environ['I2AP_LISTENER_TOPIC']
    config['listener-topic-type'] = os.environ['I2AP_LISTENER_TOPIC_TYPE']
    config['listener-subscription'] = os.environ['I2AP_LISTENER_SUBSCRIPTION']
    config['ddl-directory'] = os.environ['I2AP_DDL_DIRECTORY']
    return config


if __name__ == '__main__':
    """parses the startup arguments for the message processing daemon"""
    parser = argparse.ArgumentParser(
        description=__doc__,
        formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument('--type', help='GCS or STD')
    parser.add_argument('--project', help='The ID of the project that owns the subscription')
    parser.add_argument('--topic', help='Then name of the Pub/Sub topic')
    parser.add_argument('--subscription', help='The name of the Pub/Sub subscription')
    options = parser.parse_args()
    config = pullConfig()

    if options.type:
        config['listener-topic-type'] = options.type
        config['project-id'] = options.project
        config['listener-topic'] = options.topic
        config['listener-subscription'] = options.subscription

    logging.basicConfig(stream=sys.stdout, level=logging.DEBUG,
                        format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logging.info('Starting a listener for topic: ' + config['listener-topic'])
    ps = PSHelper(config)
    ps.listenForMessages()
    logging.info('Stopping pub/sub listener...')
