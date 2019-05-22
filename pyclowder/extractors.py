"""Clowder Extractors

One of the most interesting aspects of Clowder is the ability to extract
metadata from any file. This ability is created using extractors. To make it
easier to create these extractors in python we have created a class called
Extractor that can be used as the base class. The extractor should implement
one or both of the check_message and process message functions. The
check_message should return one of the values in CheckMessage.
"""

import argparse
import json
import logging
import logging.config
import os
import sys
import threading
import traceback
import re
import time

from pyclowder.connectors import RabbitMQConnector, HPCConnector, LocalConnector
from pyclowder.utils import CheckMessage, setup_logging
import pyclowder.files


class Extractor(object):
    """Basic extractor.

    Most extractors will want to override at least the process_message
    function. To control if the file should be downloaded (and if the
    process_message function should be called) the check_message
    function can be used.
    """

    def __init__(self):
        self.extractor_info = None
        self.args = None
        self.ssl_verify = False

        # load extractor_info.json
        filename = 'extractor_info.json'
        if not os.path.isfile(filename):
            pathname = os.path.abspath(os.path.dirname(sys.argv[0]))
            filename = os.path.join(pathname, 'extractor_info.json')

        if not os.path.isfile(filename):
            print("Could not find extractor_info.json")
            sys.exit(-1)
        try:
            with open(filename) as info_file:
                self.extractor_info = json.load(info_file)
        except Exception:  # pylint: disable=broad-except
            print("Error loading extractor_info.json")
            traceback.print_exc()
            sys.exit(-1)

        # read values from environment variables, otherwise use defaults
        # this is the specific setup for the extractor
        # use RABBITMQ_QUEUE env to overwrite extractor's queue name
        rabbitmq_queuename = os.getenv('RABBITMQ_QUEUE')
        if not rabbitmq_queuename:
            rabbitmq_queuename = self.extractor_info['name']
        rabbitmq_uri = os.getenv('RABBITMQ_URI', "amqp://guest:guest@127.0.0.1/%2f")
        rabbitmq_exchange = os.getenv('RABBITMQ_EXCHANGE', "clowder")
        registration_endpoints = os.getenv('REGISTRATION_ENDPOINTS', "")
        logging_config = os.getenv("LOGGING")
        mounted_paths = os.getenv("MOUNTED_PATHS", "{}")
        input_file_path = os.getenv("INPUT_FILE_PATH")
        output_file_path = os.getenv("OUTPUT_FILE_PATH")
        connector_default = "RabbitMQ"
        if os.getenv('LOCAL_PROCESSING', "False").lower() == "true":
            connector_default = "Local"

        # create the actual extractor
        self.parser = argparse.ArgumentParser(description=self.extractor_info['description'])
        self.parser.add_argument('--connector', '-c', type=str, nargs='?', default=connector_default,
                                 choices=["RabbitMQ", "HPC", "Local"],
                                 help='connector to use (default=RabbitMQ)')
        self.parser.add_argument('--logging', '-l', nargs='?', default=logging_config,
                                 help='file or url or logging coonfiguration (default=None)')
        self.parser.add_argument('--num', '-n', type=int, nargs='?', default=1,
                                 help='number of parallel instances (default=1)')
        self.parser.add_argument('--pickle', nargs='*', dest="hpc_picklefile",
                                 default=None, action='append',
                                 help='pickle file that needs to be processed (only needed for HPC)')
        self.parser.add_argument('--register', '-r', nargs='?', dest="registration_endpoints",
                                 default=registration_endpoints,
                                 help='Clowder registration URL (default=%s)' % registration_endpoints)
        self.parser.add_argument('--rabbitmqURI', nargs='?', dest='rabbitmq_uri', default=rabbitmq_uri,
                                 help='rabbitMQ URI (default=%s)' % rabbitmq_uri.replace("%", "%%"))
        self.parser.add_argument('--rabbitmqQUEUE', nargs='?', dest='rabbitmq_queuename',
                                 default=rabbitmq_queuename,
                                 help='rabbitMQ queue name (default=%s)' % rabbitmq_queuename)
        self.parser.add_argument('--rabbitmqExchange', nargs='?', dest="rabbitmq_exchange", default=rabbitmq_exchange,
                                 help='rabbitMQ exchange (default=%s)' % rabbitmq_exchange)
        self.parser.add_argument('--mounts', '-m', dest="mounted_paths", default=mounted_paths,
                                 help="dictionary of {'remote path':'local path'} mount mappings")
        self.parser.add_argument('--input-file-path', '-ifp', dest="input_file_path", default=input_file_path,
                                 help="Full path to local input file to be processed (used by Big Data feature)")
        self.parser.add_argument('--output-file-path', '-ofp', dest="output_file_path", default=output_file_path,
                                 help="Full path to local output JSON file to store metadata "
                                      "(used by Big Data feature)")
        self.parser.add_argument('--sslignore', '-s', dest="sslverify", action='store_false',
                                 help='should SSL certificates be ignores')
        self.parser.add_argument('--version', action='version', version='%(prog)s 1.0')
        self.parser.add_argument('--no-bind', dest="nobind", action='store_true',
                                 help='instance will bind itself to RabbitMQ by name but NOT file type')

    def setup(self):
        """Parse command line arguments and so some setup

        This will parse any command line arguments, update some variables based on these command line arguments and
        initialize the logging system.
        """
        self.args = self.parser.parse_args()

        # use command line option for ssl_verify
        if 'sslverify' in self.args:
            self.ssl_verify = self.args.sslverify

        # start logging system
        setup_logging(self.args.logging)

    def start(self):
        """Create the connector and start listening.

        Based on the num command line argument this will start multiple instances of a connector and run each of them
        in their own thread. Once the connector(s) are created this function will go into a endless loop until either
        all connectors have stopped or the user kills the program.
        """
        logger = logging.getLogger(__name__)
        connectors = list()
        for connum in range(self.args.num):
            if self.args.connector == "RabbitMQ":
                if 'rabbitmq_uri' not in self.args:
                    logger.error("Missing URI for RabbitMQ")
                else:
                    rabbitmq_key = []
                    if not self.args.nobind:
                        for key, value in self.extractor_info['process'].items():
                            for mt in value:
                                # Replace trailing '*' with '#'
                                mt = re.sub('(\*$)', '#', mt)
                                if mt.find('*') > -1:
                                    logger.error("Invalid '*' found in rabbitmq_key: %s" % mt)
                                else:
                                    if mt == "":
                                        rabbitmq_key.append("*.%s.#" % key)
                                    else:
                                        rabbitmq_key.append("*.%s.%s" % (key, mt.replace("/", ".")))

                    rconn = RabbitMQConnector(self.args.rabbitmq_queuename,
                                              self.extractor_info,
                                              check_message=self.check_message,
                                              process_message=self.process_message,
                                              rabbitmq_uri=self.args.rabbitmq_uri,
                                              rabbitmq_exchange=self.args.rabbitmq_exchange,
                                              rabbitmq_key=rabbitmq_key,
                                              rabbitmq_queue=self.args.rabbitmq_queuename,
                                              mounted_paths=json.loads(self.args.mounted_paths))
                    rconn.connect()
                    rconn.register_extractor(self.args.registration_endpoints)
                    connectors.append(rconn)
                    threading.Thread(target=rconn.listen, name="Connector-" + str(connum)).start()
            elif self.args.connector == "HPC":
                if 'hpc_picklefile' not in self.args:
                    logger.error("Missing hpc_picklefile for HPCExtractor")
                else:
                    hconn = HPCConnector(self.extractor_info['name'],
                                         self.extractor_info,
                                         check_message=self.check_message,
                                         process_message=self.process_message,
                                         picklefile=self.args.hpc_picklefile,
                                         mounted_paths=json.loads(self.args.mounted_paths))
                    hconn.register_extractor(self.args.registration_endpoints)
                    connectors.append(hconn)
                    threading.Thread(target=hconn.listen, name="Connector-" + str(connum)).start()
            elif self.args.connector == "Local":

                if self.args.input_file_path is None:
                    logger.error("Environment variable INPUT_FILE_PATH or parameter --input-file-path is not set. "
                                 "Please try again after setting one of these")
                elif not os.path.isfile(self.args.input_file_path):
                    logger.error("Local input file is not a regular file. Please check the path.")
                else:
                    local_connector = LocalConnector(self.extractor_info['name'],
                                                     self.extractor_info,
                                                     self.args.input_file_path,
                                                     process_message=self.process_message,
                                                     output_file_path=self.args.output_file_path)
                    connectors.append(local_connector)
                    threading.Thread(target=local_connector.listen, name="Connector-" + str(connum)).start()
            else:
                logger.error("Could not create instance of %s connector.", self.args.connector)
                sys.exit(-1)

        logger.info("Waiting for messages. To exit press CTRL+C")
        try:
            while connectors:
                time.sleep(1)
                connectors = filter(lambda x: x.alive(), connectors)
        except KeyboardInterrupt:
            pass
        except BaseException:
            logger.exception("Error while consuming messages.")

        for c in connectors:
            c.stop()

    def get_metadata(self, content, resource_type, resource_id, server=None):
        """Generate a metadata field.

        This will return a metadata dict that is valid JSON-LD. This will use the results as well as the information
        in extractor_info.json to create the metadata record.

        This does a simple check for validity, and prints to debug any issues it finds (i.e. a key in conent is not
        defined in the context).

        Args:
            content (dict): the data that is in the content
            resource_type (string); type of resource such as file, dataset, etc
            resource_id (string): id of the resource the metadata is associated with
            server (string): clowder url, used for extractor_id, if None it will use
                             https://clowder.ncsa.illinois.edu/extractors
        """
        logger = logging.getLogger(__name__)
        context_url = 'https://clowder.ncsa.illinois.edu/contexts/metadata.jsonld'

        # simple check to see if content is in context
        if logger.isEnabledFor(logging.DEBUG):
            for k in content:
                if not self._check_key(k, self.extractor_info['contexts']):
                    logger.debug("Simple check could not find %s in contexts" % k)

        return {
            '@context': [context_url] + self.extractor_info['contexts'],
            'attachedTo': {
                'resourceType': resource_type,
                'id': resource_id
            },
            'agent': {
                '@type': 'cat:extractor',
                'extractor_id': '%sextractors/%s/%s' %
                                (server, self.extractor_info['name'], self.extractor_info['version']),
                'version': self.extractor_info['version'],
                'name': self.extractor_info['name']
            },
            'content': content
        }

    def _check_key(self, key, obj):
        if key in obj:
            return True

        if isinstance(obj, dict):
            for x in obj.values():
                if self._check_key(key, x):
                    return True
        elif isinstance(obj, list):
            for x in obj:
                if self._check_key(key, x):
                    return True
        return False

    # pylint: disable=no-self-use,unused-argument
    def check_message(self, connector, host, secret_key, resource, parameters):
        """Checks to see if the message needs to be processed.

        This will return one of the values from CheckMessage:
        - ignore : the process_message function is not called
        - download : the input file will be downloaded and process_message is called
        - bypass : the file is NOT downloaded but process_message is still called

        Args:
            connector (Connector): the connector that received the message
            parameters (dict): the message received
        """

        logging.getLogger(__name__).debug("default check message : " + str(parameters))
        return CheckMessage.download

    # pylint: disable=no-self-use,unused-argument
    def process_message(self, connector, host, secret_key, resource, parameters):
        """Process the message and send results back to clowder.

        Args:
            connector (Connector): the connector that received the message
            parameters (dict): the message received
        """
        logging.getLogger(__name__).debug("default process message : " + str(parameters))


class SimpleExtractor(Extractor):
    """
    Simple extractor. All that is needed to be done is extend the process_file function.
    """

    def __init__(self):
        """
        Initialize the extractor and setup the logger.
        """
        Extractor.__init__(self)
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.INFO)
        self.logger = logging.getLogger('__main__')
        self.logger.setLevel(logging.INFO)

    def process_message(self, connector, host, secret_key, resource, parameters):
        """
        Process a clowder message. This will download the file to local disk and call the
        process_file to do the actual processing of the file. The resulting dict is then
        parsed and based on the keys in the dict it will upload the results to the right
        location in clowder.
        """
        input_file = resource["local_paths"][0]
        file_id = resource['id']

        # call the actual function that processes the file
        if file_id and input_file:
            result = self.process_file(input_file)
        else:
            result = dict()

        # return information to clowder
        try:
            if 'metadata' in result.keys():
                metadata = self.get_metadata(result.get('metadata'), 'file', file_id, host)
                self.logger.info("upload metadata")
                self.logger.debug(metadata)
                pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)
            if 'previews' in result.keys():
                self.logger.info("upload previews")
                for preview in result['previews']:
                    if os.path.exists(str(preview)):
                        preview = {'file': preview}
                        self.logger.info("upload preview")
                        pyclowder.files.upload_preview(connector, host, secret_key, file_id, str(preview))
        finally:
            self.cleanup_data(result)

    def process_file(self, input_file):
        """
        This function will process the file and return a dict that contains the result. This
        dict can have the following keys:
            - metadata: the metadata to be associated with the file
            - previews: files on disk with the preview to be uploaded
        :param input_file: the file to be processed.
        :return: the specially formatted dict.
        """
        return dict()

    def cleanup_data(self, result):
        """
        Once the information is uploaded to clowder this function is called for cleanup. This
        will enable the extractor to remove any preview images or other cleanup other resources
        that were opened. This is the same dict as returned by process_file.

        The default behaviour is to remove all the files in previews.

        :param result: the result returned from process_file.
        """

        for preview in result.get("previews", []):
            if os.path.exists(preview):
                os.remove(preview)
