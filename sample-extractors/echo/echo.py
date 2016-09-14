#!/usr/bin/env python

"""Extractor to show check_message"""

import logging

from pyclowder.extractors import Extractor
from pyclowder.connectors import CheckMessage
import pyclowder.files


class Echo(Extractor):
    """Echo Extractor

    Uploads the data from the body back to clowder.
    """
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def check_message(self, connector, parameters):
        """The extractor to not download the file."""
        return CheckMessage.bypass

    def process_message(self, connector, parameters):
        """Acual work is done here"""
        host = parameters['host']
        secret_key = parameters['secretKey']
        file_id = parameters['fileid']

        # store results as metadata
        rabbitmq = {}
        for key, value in parameters.items():
            if key not in ('channel', 'header', 'secretKey'):
                rabbitmq[key] = value

        # store results as metadata
        metadata = self.get_metadata(rabbitmq, 'file', file_id, host)
        logging.getLogger(__name__).debug(metadata)

        # upload metadata
        pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)


if __name__ == "__main__":
    extractor = Echo()
    extractor.start()
