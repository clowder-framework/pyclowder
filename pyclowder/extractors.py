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
import pyclowder.datasets
from functools import reduce

clowder_version = int(os.getenv('CLOWDER_VERSION', '1'))

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
                new_info = self._get_extractor_info_v2()
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
        clowder_url = os.getenv("CLOWDER_URL", "")
        registration_endpoints = os.getenv('REGISTRATION_ENDPOINTS', "")
        logging_config = os.getenv("LOGGING")
        mounted_paths = os.getenv("MOUNTED_PATHS", "{}")
        input_file_path = os.getenv("INPUT_FILE_PATH")
        output_file_path = os.getenv("OUTPUT_FILE_PATH")
        connector_default = "RabbitMQ"
        if os.getenv('LOCAL_PROCESSING', "False").lower() == "true":
            connector_default = "Local"
        max_retry = int(os.getenv('MAX_RETRY', 10))
        heartbeat = int(os.getenv('HEARTBEAT', 5*60))

        # create the actual extractor
        self.parser = argparse.ArgumentParser(description=self.extractor_info['description'])
        self.parser.add_argument('--connector', '-c', type=str, nargs='?', default=connector_default,
                                 choices=["RabbitMQ", "HPC", "Local"],
                                 help='connector to use (default=RabbitMQ)')
        self.parser.add_argument('--logging', '-l', nargs='?', default=logging_config,
                                 help='file or url or logging coonfiguration (default=None)')
        self.parser.add_argument('--pickle', nargs='*', dest="hpc_picklefile",
                                 default=None, action='append',
                                 help='pickle file that needs to be processed (only needed for HPC)')
        self.parser.add_argument('--clowderURL', nargs='?', dest='clowder_url', default=clowder_url,
                                 help='Clowder host URL')
        self.parser.add_argument('--rabbitmqURI', nargs='?', dest='rabbitmq_uri', default=rabbitmq_uri,
                                 help='rabbitMQ URI (default=%s)' % rabbitmq_uri.replace("%", "%%"))
        self.parser.add_argument('--rabbitmqQUEUE', nargs='?', dest='rabbitmq_queuename',
                                 default=rabbitmq_queuename,
                                 help='rabbitMQ queue name (default=%s)' % rabbitmq_queuename)
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
        self.parser.add_argument('--max-retry', dest='max_retry', default=max_retry,
                                 help='Maximum number of retries if an error happens in the extractor (default=%d)' % max_retry)
        self.parser.add_argument('--heartbeat', dest='heartbeat', default=heartbeat,
                                 help='Time in seconds between extractor heartbeats (default=%d)' % heartbeat)

    def setup(self):
        """Parse command line arguments and so some setup

        This will parse any command line arguments, update some variables based on these command line arguments and
        initialize the logging system.
        """
        self.args = self.parser.parse_args()

        # fix extractor_info based on the queue name
        if self.args.rabbitmq_queuename and self.extractor_info['name'] != self.args.rabbitmq_queuename:
            self.extractor_info['name'] = self.args.rabbitmq_queuename

        # use command line option for ssl_verify
        if 'sslverify' in self.args:
            self.ssl_verify = self.args.sslverify

        # start logging system
        setup_logging(self.args.logging)

    def start(self):
        """Create the connector and start listening.

        Start a single instance of a connector and run it in their own thread.
        Once the connector(s) are created this function will go into a endless loop until either
        all connectors have stopped or the user kills the program.
        """
        logger = logging.getLogger(__name__)
        connector = None

        if self.args.connector == "RabbitMQ":
            if 'rabbitmq_uri' not in self.args:
                logger.error("Missing URI for RabbitMQ")
            else:
                rabbitmq_key = []
                if not self.args.nobind:
                    for key, value in self.extractor_info['process'].items():
                        for mt in value:
                            # Replace trailing '*' with '#'
                            mt = re.sub(r'(\*$)', '#', mt)
                            if mt.find('*') > -1:
                                logger.error("Invalid '*' found in rabbitmq_key: %s" % mt)
                            else:
                                if mt == "":
                                    rabbitmq_key.append("*.%s.#" % key)
                                else:
                                    rabbitmq_key.append("*.%s.%s" % (key, mt.replace("/", ".")))

                connector = RabbitMQConnector(self.args.rabbitmq_queuename,
                                              self.extractor_info,
                                              check_message=self.check_message,
                                              process_message=self.process_message,
                                              rabbitmq_uri=self.args.rabbitmq_uri,
                                              rabbitmq_key=rabbitmq_key,
                                              rabbitmq_queue=self.args.rabbitmq_queuename,
                                              mounted_paths=json.loads(self.args.mounted_paths),
                                              clowder_url=self.args.clowder_url,
                                              max_retry=self.args.max_retry,
                                              heartbeat=self.args.heartbeat)
                connector.connect()
                threading.Thread(target=connector.listen, name="RabbitMQConnector").start()

        elif self.args.connector == "HPC":
            if 'hpc_picklefile' not in self.args:
                logger.error("Missing hpc_picklefile for HPCExtractor")
            else:
                connector = HPCConnector(self.extractor_info['name'],
                                         self.extractor_info,
                                         check_message=self.check_message,
                                         process_message=self.process_message,
                                         picklefile=self.args.hpc_picklefile,
                                         mounted_paths=json.loads(self.args.mounted_paths),
                                         max_retry=self.args.max_retry)
                threading.Thread(target=connector.listen, name="HPCConnector").start()

        elif self.args.connector == "Local":
            if self.args.input_file_path is None:
                logger.error("Environment variable INPUT_FILE_PATH or parameter "
                             "--input-file-path is not set. Please try again after "
                             "setting one of these")
            elif not os.path.isfile(self.args.input_file_path):
                logger.error("Local input file is not a regular file. Please check the path.")
            else:
                connector = LocalConnector(self.extractor_info['name'],
                                           self.extractor_info,
                                           self.args.input_file_path,
                                           process_message=self.process_message,
                                           output_file_path=self.args.output_file_path,
                                           max_retry=self.args.max_retry)
                threading.Thread(target=connector.listen, name="LocalConnector").start()
        else:
            logger.error("Could not create instance of %s connector.", self.args.connector)
            sys.exit(-1)

        logger.info("Waiting for messages. To exit press CTRL+C")
        try:
            while connector.alive():
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        except BaseException:
            logger.exception("Error while consuming messages.")
        connector.stop()

    def _get_extractor_info_v2(self):
        current_extractor_info = self.extractor_info.copy()
        listener_data = dict()
        listener_data['name'] = current_extractor_info['name']
        listener_data['version'] = current_extractor_info['version']
        listener_data['description'] = current_extractor_info['version']
        return listener_data


    def get_metadata(self, content, resource_type, resource_id, server=None, contexts=None):
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
        # TODO generate clowder2.0 extractor info
        if clowder_version == 2.0:
            new_extractor_info = self._get_extractor_info_v2()
            md = dict()
            if contexts is not None:
                md["context"] = [context_url] + contexts
            md["context_url"] = context_url
            md["content"] = content
            md["contents"] = content
            md["listener"] = new_extractor_info
            return md
        else:
            # TODO handle cases where contexts are either not available or are dynamnically generated
            if contexts is not None:
                md = {
                    '@context': [context_url] + contexts,
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
                return md
            else:
                md = {
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
                return md
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
        print(clowder_version)
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

    # TODO: Support check_message() in simple extractors

    def process_message(self, connector, host, secret_key, resource, parameters):
        """
        Process a clowder message. This will download the file(s) to local disk and call
        process_file or process_dataset to do the actual processing. The resulting dict is then
        parsed and based on the keys in the dict it will upload the results to the right
        location in clowder.
        """
        if 'files' in resource:
            type = 'dataset'
            input_files = resource['local_paths']
            dataset_id = resource['id']

        elif 'local_paths' in resource:
            type = 'file'
            input_file = resource['local_paths'][0]
            file_id = resource['id']
            dataset_id = resource['parent']['id']
        else:
            # TODO: Eventually support other messages such as metadata.added
            type = 'unknown'

        # call the actual function that processes the message
        if type == 'file' and file_id and input_file:
            result = self.process_file(input_file)
        elif type == 'dataset' and dataset_id and input_files:
            result = self.process_dataset(input_files)
        else:
            result = dict()

        try:
            # upload metadata to the processed file or dataset
            if 'metadata' in result.keys():
                self.logger.debug("upload metadata")
                if type == 'file':
                    metadata = self.get_metadata(result.get('metadata'), 'file', file_id, host)
                    self.logger.debug(metadata)
                    pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)
                elif type == 'dataset':
                    metadata = self.get_metadata(result.get('metadata'), 'dataset', dataset_id, host)
                    self.logger.debug(metadata)
                    pyclowder.datasets.upload_metadata(connector, host, secret_key, dataset_id, metadata)
                else:
                    self.logger.error("unable to attach metadata to resource type: %s" % type)

            # upload previews to the processed file
            if 'previews' in result.keys():
                if type == 'file':
                    for preview in result['previews']:
                        if os.path.exists(str(preview)):
                            preview = {'file': preview}
                            self.logger.debug("upload preview")
                            pyclowder.files.upload_preview(connector, host, secret_key, file_id, str(preview))
                else:
                    # TODO: Add Clowder endpoint (& pyclowder method) to attach previews to datasets
                    self.logger.error("previews not currently supported for resource type: %s" % type)

            if 'tags' in result.keys():
                self.logger.debug("upload tags")
                tags = {"tags": result["tags"]}
                if type == 'file':
                    pyclowder.files.upload_tags(connector, host, secret_key, file_id, tags)
                else:
                    pyclowder.datasets.upload_tags(connector, host, secret_key, dataset_id, tags)

            # upload output files to the processed file's parent dataset or processed dataset
            if 'outputs' in result.keys():
                self.logger.debug("upload output files")
                if type == 'file' or type == 'dataset':
                    for output in result['outputs']:
                        if os.path.exists(str(output)):
                            pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id, str(output))
                else:
                    self.logger.error("unable to upload outputs to resource type: %s" % type)

            if 'new_dataset' in result.keys():
                if type == 'dataset':
                    nds = result['new_dataset']
                    if 'name' not in nds.keys():
                        self.logger.error("new datasets require a name")
                    else:
                        description = nds['description'] if 'description' in nds.keys() else ""
                        new_dataset_id = pyclowder.datasets.create_empty(connector, host, secret_key, nds['name'],
                                                                         description)
                        self.logger.debug("created new dataset: %s" % new_dataset_id)

                        if 'metadata' in nds.keys():
                            self.logger.debug("upload metadata to new dataset")
                            metadata = self.get_metadata(nds.get('metadata'), 'dataset', new_dataset_id, host)
                            self.logger.debug(metadata)
                            pyclowder.datasets.upload_metadata(connector, host, secret_key, new_dataset_id, metadata)

                        if 'outputs' in nds.keys():
                            self.logger.debug("upload output files to new dataset")
                            for output in nds['outputs']:
                                if os.path.exists(str(output)):
                                    pyclowder.files.upload_to_dataset(connector, host, secret_key, new_dataset_id,
                                                                      str(output))

                        if 'previews' in nds.keys():
                            # TODO: Add Clowder endpoint (& pyclowder method) to attach previews to datasets
                            self.logger.error("previews not currently supported for resource type: %s" % type)

        finally:
            self.cleanup_data(result)

    def process_file(self, input_file):
        """
        This function will process the file and return a dict that contains the result. This
        dict can have the following keys:
            - metadata: the metadata to be associated with the processed file
            - previews: images on disk with the preview to be uploaded to the processed file
            - outputs: files on disk to be added to processed file's parent
        :param input_file: the file to be processed.
        :return: the specially formatted dict.
        """
        return dict()

    def process_dataset(self, input_files):
        """
        This function will process the file list and return a dict that contains the result. This
        dict can have the following keys:
            - metadata: the metadata to be associated with the processed dataset
            - outputs: files on disk to be added to the dataset
            - previews: images to be associated with the dataset
            - new_dataset: a dict describing a new dataset to be created for the outputs, with the following keys:
                - name: the name of the new dataset to be created (including adding the outputs,
                        metadata and previews contained in new_dataset)
                - description: description for the new dataset to be created
                - previews: (see above)
                - metadata: (see above)
                - outputs: (see above)
        :param input_files: the files to be processed.
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
