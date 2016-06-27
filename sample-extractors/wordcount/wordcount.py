#!/usr/bin/env python

import argparse
import logging
import os
import subprocess

from clowder.extractors import Extractor
import clowder.api


class WordCount(Extractor):
    def __init__(self, ssl_verify=False):
        Extractor.__init__(self, 'wordcount', ssl_verify)

    def process_message(self, connector, parameters):
        # Process the file and upload the results

        logger = logging.getLogger(__name__)
        inputfile = parameters['inputfile']
        host = parameters['host']
        secret_key = parameters['secretKey']
        file_id = parameters['fileid']

        # call actual program
        result = subprocess.check_output(['wc', inputfile], stderr=subprocess.STDOUT)
        (lines, words, characters, filename) = result.split()

        # context url
        context_url = 'https://clowder.ncsa.illinois.edu/clowder/contexts/metadata.jsonld'

        # store results as metadata
        metadata = {
            '@context': [
                context_url,
                {
                    'lines': 'http://clowder.ncsa.illinois.edu/' + self.extractor_name + '#lines',
                    'words': 'http://clowder.ncsa.illinois.edu/' + self.extractor_name + '#words',
                    'characters': 'http://clowder.ncsa.illinois.edu/' + self.extractor_name + '#characters'
                }
            ],
            'attachedTo': {
                'resourceType': 'file', 'id': parameters["fileid"]
            },
            'agent': {
                '@type': 'cat:extractor',
                'extractor_id': 'https://clowder.ncsa.illinois.edu/extractors/' + self.extractor_name
            },
            'content': {
                'lines': lines,
                'words': words,
                'characters': characters
            }
        }
        logger.debug(metadata)

        # upload metadata
        clowder.api.upload_file_metadata_jsonld(connector, host, secret_key, file_id, metadata)


if __name__ == "__main__":
    # read values from environment variables, otherwise use defaults
    # this is the specific setup for the extractor
    rabbitmqURI = os.getenv('RABBITMQ_URI', "amqp://guest:guest@127.0.0.1/%2f")
    rabbitmqExchange = os.getenv('RABBITMQ_EXCHANGE', "clowder")
    registrationEndpoints = os.getenv('REGISTRATION_ENDPOINTS', "")
    rabbitmqKey = "*.file.text.#"

    # parse command line arguments
    parser = argparse.ArgumentParser(description='WordCount extractor. Counts the number of characters, words and'
                                                 ' lines in the text file that was uploaded.')
    parser.add_argument('--connector', '-c', type=str, nargs='?', default="RabbitMQ",
                        choices=["RabbitMQ", "HPC"],
                        help='connector to use (default=RabbitMQ)')
    parser.add_argument('--logging', '-l', nargs='?', default=None,
                        help='file or logging coonfiguration (default=None)')
    parser.add_argument('--num', '-n', type=int, nargs='?', default=1,
                        help='number of parallel instances (default=1)')
    parser.add_argument('--pickle', type=file, nargs='*', default=None, action='append',
                        help='pickle file that needs to be processed (only needed for HPC)')
    parser.add_argument('--register', '-r', nargs='?', default=registrationEndpoints,
                        help='Clowder registration URL (default=%s)' % registrationEndpoints)
    parser.add_argument('--rabbitmqURI', nargs='?', default=rabbitmqURI,
                        help='rabbitMQ URI (default=%s)' % rabbitmqURI.replace("%", "%%"))
    parser.add_argument('--rabbitmqExchange', nargs='?', default=rabbitmqExchange,
                        help='rabbitMQ exchange (default=%s)' % rabbitmqExchange)
    parser.add_argument('--sslignore', '-s', dest="sslverify", action='store_false',
                        help='should SSL certificates be ignores')
    parser.add_argument('--version', action='version', version='%(prog)s 1.0')
    args = parser.parse_args()

    # create the extractor
    extractor = WordCount(ssl_verify=args.sslverify)

    # setup logging for the exctractor
    extractor.setup_logging(args.logging)
    logging.getLogger('clowder').setLevel(logging.DEBUG)
    logging.getLogger('__main__').setLevel(logging.DEBUG)

    # start the extractor
    extractor.start_connector(args.connector, args.num,
                              rabbitmq_uri=args.rabbitmqURI,
                              rabbitmq_exchange=args.rabbitmqExchange,
                              rabbitmq_key=rabbitmqKey,
                              hpc_picklefile=args.pickle,
                              regstration_endpoints=args.register)
