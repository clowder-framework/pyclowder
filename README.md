This repository contains the next generation of pyClowder. This library makes it easier to interact with clowder and to create extractors. 

# Extractor creation

One of the most interesting aspects of Clowder is the ability to extract metadata from any file. This ability is created using extractors. To make it easy to create these extractors in python we have created a module called clowder. Besides wrapping often used api calls in convinient python calls, we have also added some code to make it easy to create new extractors. 

## Example Extractor

Following is an example of the WordCount extractor. This example will allow the user to specify from the command line what connector to use, the number of connectors, you can get a list of the options by using the -h flag on the command line. This will also read some environment variables to initialize the defaults allowing for easy use of this extractor in a Docker container.

```
#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import argparse
import logging
import os
import subprocess

from clowder.extractors import Extractor
from clowder.utils import setup_logging
import clowder.files


class WordCount(Extractor):
    """Count the number of characters, words and lines in a text file."""
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
        (lines, words, characters, _) = result.split()

        # context url
        context_url = 'https://clowder.ncsa.illinois.edu/clowder/contexts/metadata.jsonld'

        # store results as metadata
        metadata = {
            '@context': [
                context_url,
                {
                    'lines': 'http://clowder.ncsa.illinois.edu/%s#lines' % self.extractor_name,
                    'words': 'http://clowder.ncsa.illinois.edu/%s#words' % self.extractor_name,
                    'characters': 'http://clowder.ncsa.illinois.edu/%s#characters'
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
                'lines': lines,
                'words': words,
                'characters': characters
            }
        }
        logger.debug(metadata)

        # upload metadata
        clowder.files.upload_file_metadata_jsonld(connector, host, secret_key, file_id, metadata)


def main():
    """main function"""
    # read values from environment variables, otherwise use defaults
    # this is the specific setup for the extractor
    rabbitmq_uri = os.getenv('RABBITMQ_URI', "amqp://guest:guest@127.0.0.1/%2f")
    rabbitmq_exchange = os.getenv('RABBITMQ_EXCHANGE', "clowder")
    registration_endpoints = os.getenv('REGISTRATION_ENDPOINTS', "")
    rabbitmq_key = "*.file.text.#"

    # parse command line arguments
    parser = argparse.ArgumentParser(description='WordCount extractor. Counts the number of'
                                                 ' characters, words and lines in the text'
                                                 ' file that was uploaded.')
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
    extractor = WordCount(ssl_verify=args.sslverify)
    extractor.start_connector(args.connector, args.num,
                              rabbitmq_uri=args.rabbitmqURI,
                              rabbitmq_exchange=args.rabbitmqExchange,
                              rabbitmq_key=rabbitmq_key,
                              hpc_picklefile=args.pickle,
                              regstration_endpoints=args.register)

if __name__ == "__main__":
    main()
```

## Initialization

To create a new extracor you should create a new class based on clowder.Extractor. The extractor should call the super method from the constructor with the name of the extractor. In the case of the RabbitMQ connector this will also be the name of the queue:

```
class WordCount(Extractor):
    def __init__(self, ssl_verify=False):
        Extractor.__init__(self, 'wordcount', ssl_verify)
```

## Message Processing

Next the extractor should implement one or both of the check_message and process message functions. The check_message should return one of the values in CheckMessage.

```
    def check_message(self, connector, parameters):
        logging.getLogger(__name__).debug("default check message : " + str(parameters))
        return CheckMessage.download

    def process_message(self, debug, parameters):
        logging.getLogger(__name__).debug("default process message : " + str(parameters))
        pass
```


## Starting the Extractor

The extractor base class has convenient functions for starting a connector. The function that starts the extractor will take an argument that is the connector to use and the options needed to setup the connector. Once the start_connector function is called the code will not return until the connector is finished, or is interrupted.

```
    # setup logging for the exctractor
    setup_logging(args.logging)
    logging.getLogger('clowder').setLevel(logging.DEBUG)
    logging.getLogger('__main__').setLevel(logging.DEBUG)

    # start the extractor
    extractor = WordCount(ssl_verify=args.sslverify)
    extractor.start_connector(args.connector, args.num,
                              rabbitmq_uri=args.rabbitmqURI,
                              rabbitmq_exchange=args.rabbitmqExchange,
                              rabbitmq_key=rabbitmqKey,
                              hpc_picklefile=args.pickle,
                              regstration_endpoints=args.register)
```

# Connectors

The system has two connectors defined by default. The connectors are used to star the extractors. The connector will look for messages and call the check_message and process_message of the extractor. The two connectors are RabbitMQConnector and HPCConnector. Both of these will call the check_message first, and based on the result of this will ignore the message, download the file and call process_message or bypass the download and just call the process_message. 

## RabbitMQConnector

The RabbitMQ connector connects to a RabbitMQ instance, creates a queue and binds itself to that queue. Any message in the queue will be fetched and passed to the check_message and process_message. This connector takes three parameters:

* rabbitmq_uri [REQUIRED] : the uri of the RabbitMQ server
* rabbitmq_exchange [OPTIONAL] : the exchange to which to bind the queue
* rabbitmq_key [OPTIONAL] : the key that binds the queue to the exchange

## HPCConnector

The HPC connector will run extractions based on the pickle files that are passed in to the constructor as an argument. Once all pickle files are processed the extractor will stop. The pickle file is assumed to have one additional argument, the logfile that is being monitored to send feedback back to clowder. This connector takes a single argument (which can be list):

* picklefile [REQUIRED] : a single file, or list of files that are the pickled messsages to be processed.


# Clowder API wrappers

Besides code to create extractors there are also functins that wrap the clowder API. They are broken up into modules that map to the routes endpoint of clowder, for example /api/files/:id/download will be in the clowder.files package.

## utils

The clowder.utils package contains some utility functions that should make it easier to create new code that works as an extractor or code that interacts with clowder. One of these functions is setup_logging, which will initalize the logging system for you. The logging function takes a single argument that can be None. The argument is either a pointer to a file that is read with the configuration options.

# files


