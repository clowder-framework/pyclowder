#!/usr/bin/env python

"""Extractor to show check_message"""

import argparse
import logging
import os

from clowder.extractors import Extractor
from clowder.connectors import CheckMessage
from clowder.utils import setup_logging
import clowder.files


class Echo(Extractor):
    """Echo Extractor

    Uploads the data from the body back to clowder.
    """
    def __init__(self, ssl_verify=False):
        Extractor.__init__(self, "echo", ssl_verify)

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

        # context url
        context_url = 'https://clowder.ncsa.illinois.edu/clowder/contexts/metadata.jsonld'

        # store results as metadata
        metadata = {
            '@context': [
                context_url,
                {
                    'rabbitmq': 'http://clowder.ncsa.illinois.edu/%s#rabbitmq'
                                % self.extractor_name
                }
            ],
            'attachedTo': {
                'resourceType': 'file', 'id': parameters["fileid"]
            },
            'agent': {
                '@type': 'cat:extractor',
                'extractor_id': 'https://clowder.ncsa.illinois.edu/extractors/%s'
                                % self.extractor_name
            },
            'content': {
                'rabbitmq': rabbitmq
            }
        }
        logging.getLogger(__name__).debug(metadata)

        # upload metadata
        clowder.files.upload_file_metadata_jsonld(connector, host, secret_key, file_id, metadata)


def main():
    """main function"""

    # read values from environment variables, otherwise use defaults
    # this is the specific setup for the extractor
    rabbitmq_uri = os.getenv('RABBITMQ_URI', "amqp://guest:guest@127.0.0.1/%2f")
    rabbitmq_exchange = os.getenv('RABBITMQ_EXCHANGE', "clowder")
    registration_endpoints = os.getenv('REGISTRATION_ENDPOINTS', "")
    rabbitmq_key = "*.file.#"

    # parse command line arguments
    parser = argparse.ArgumentParser(description='Echo parameters back as metadata.')
    parser.add_argument('--connector', '-c', type=str, nargs='?', default="RabbitMQ",
                        choices=["RabbitMQ", "HPC"],
                        help='connector to use (default=RabbitMQ)')
    parser.add_argument('--logging', '-l', nargs='?', default=None,
                        help='file or logging coonfiguration (default=None)')
    parser.add_argument('--num', '-n', type=int, nargs='?', default=1,
                        help='number of parallel instances (default=1)')
    parser.add_argument('--pickle', type=file, nargs='*', default=None, action='append',
                        help='pickle file that needs to be processed (only needed for HPC)')
    parser.add_argument('--register', '-r', nargs='?', default=registration_endpoints,
                        help='Clowder registration URL (default=%s)' % registration_endpoints)
    parser.add_argument('--rabbitmqURI', nargs='?', default=rabbitmq_uri,
                        help='rabbitMQ URI (default=%s)' % rabbitmq_uri.replace("%", "%%"))
    parser.add_argument('--rabbitmqExchange', nargs='?', default=rabbitmq_exchange,
                        help='rabbitMQ exchange (default=%s)' % rabbitmq_exchange)
    parser.add_argument('--sslignore', '-s', dest="sslverify", action='store_false',
                        help='should SSL certificates be ignores')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0')
    args = parser.parse_args()

    # setup logging for the exctractor
    setup_logging(args.logging)
    logging.getLogger('clowder').setLevel(logging.DEBUG)
    logging.getLogger('__main__').setLevel(logging.DEBUG)

    # start the extractor
    extractor = Echo(ssl_verify=args.sslverify)
    extractor.start_connector(args.connector, args.num,
                              rabbitmq_uri=args.rabbitmqURI,
                              rabbitmq_exchange=args.rabbitmqExchange,
                              rabbitmq_key=rabbitmq_key,
                              hpc_picklefile=args.pickle,
                              regstration_endpoints=args.register)

if __name__ == "__main__":
    main()
