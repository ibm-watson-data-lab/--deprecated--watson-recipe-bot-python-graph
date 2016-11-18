#!/usr/bin/env python

import logging

class GraphLogger(object):

    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s %(message)s')
        self.logger = logging.getLogger(__name__)

    def log_message_in_pretty(self, msg):
        """ Show the given message in pretty manner"""
        self.logger.info('*'*80)
        self.logger.info(msg)
        self.logger.info('*'*80)

    def info(self, msg):
        self.logger.info(msg)

    def warn(self, msg):
        self.logger.warn(msg)

    def error(self, msg):
        self.logger.error(msg)
