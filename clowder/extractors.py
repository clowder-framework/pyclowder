"""Clowder Extractors

One of the most interesting aspects of Clowder is the ability to extract
metadata from any file. This ability is created using extractors. To make it
easy to create these extractors in python we have created a module called
clowder. Besides wrapping often used api calls in convinient python calls, we
have also added some code to make it easy to create new extractors.

The extractor should implement one or both of the check_message and process
message functions. The check_message should return one of the values in
CheckMessage.
"""

import json
import logging
import logging.config
import os
import threading

import time
import yaml

from clowder.connectors import RabbitMQConnector, HPCConnector, CheckMessage


class Extractor(object):
    """Basic extractor.

    Most extractors will want to override at least the process_message
    function. To control if the file should be downloaded (and if the
    process_message function should be called) the check_message
    function can be used.
    """
    def __init__(self, extractor_name, ssl_verify=False):
        self.extractor_name = extractor_name
        self.ssl_verify = ssl_verify

    def setup_logging(self, config_info=None):
        """Given config_info setup logging.

        If config_info points to a file it will try to load it, and configure
        the logging with the values from the file. This supports yaml, json
        and ini files.

        If config_info is a string, it will try to parse the string as json
        and configure the logging system using the parsed data.

        Finally if config_info is None it will use a basic configuration for
        the logging.

        Args:
            config_info (string): either a file on disk or a json string that
                has the logging configuration as json.
        """
        if config_info:
            if os.path.isfile(config_info):
                if config_info.endswith('.yml'):
                    with open(config_info, 'r') as configfile:
                        config = yaml.safe_load(configfile)
                        logging.config.dictConfig(config)
                elif config_info.endswith('.json'):
                    with open(config_info, 'r') as configfile:
                        config = json.load(configfile)
                        logging.config.dictConfig(config)
                else:
                    logging.config.fileConfig(config_info)
            else:
                config = json.load(config_info)
                logging.config.dictConfig(config)
        else:
            logging.basicConfig(format='%(asctime)-15s [%(threadName)-15s] %(levelname)-7s :'
                                       ' %(name)s - %(message)s',
                                level=logging.INFO)
            logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARN)

    def start_connector(self, connector, num, **kwargs):
        """Create the connector and start listening.

        Based on the num variable this will start multiple instances of a
        connector and run each of them in their own thread. Once the
        connector(s) are created this function will go into a endless loop
        until either all connectors have stopped or the user kill the
        program.

        Args:
            connector (string): the connector to create
            num (int): the number of connector instaces to create
            **kwargs: arguments used to confgure the connectors
        """
        logger = logging.getLogger(__name__)
        connectors = list()
        for connum in xrange(num):
            if connector == "RabbitMQ":
                if 'rabbitmq_uri' not in kwargs:
                    logger.error("Missing URI for RabbitMQ")
                else:
                    rconn = RabbitMQConnector(self.extractor_name,
                                              check_message=self.check_message,
                                              process_message=self.process_message,
                                              rabbitmq_uri=kwargs.get('rabbitmq_uri'),
                                              rabbitmq_exchange=kwargs.get('rabbitmq_exchange'),
                                              rabbitmq_key=kwargs.get('rabbitmq_key'))
                    rconn.connect()
                    rconn.register_extractor(kwargs.get('regstration_endpoints'))
                    connectors.append(rconn)
                    threading.Thread(target=rconn.listen, name="Connector-" + str(connum)).start()
            elif connector == "HPC":
                if 'hpc_picklefile' not in kwargs:
                    logger.error("Missing hpc_picklefile for HPCExtractor")
                else:
                    hconn = HPCConnector(self.extractor_name,
                                         check_message=self.check_message,
                                         process_message=self.process_message,
                                         picklefile=kwargs.get('hpc_picklefile'))
                    hconn.register_extractor(kwargs.get('regstration_endpoints'))
                    connectors.append(hconn)
                    threading.Thread(target=hconn.listen, name="Connector-" + str(connum)).start()
            else:
                logger.error("Could not create instance of %s connector.", connector)

        logger.info("Waiting for messages. To exit press CTRL+C")
        try:
            while connectors:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        except BaseException:
            logger.exception("Error while consuming messages.")

        while connectors:
            connectors.pop(0).stop()

    def check_message(self, connector, parameters):
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

    def process_message(self, connector, parameters):
        """Process the message and send results back to clowder.

        Args:
            connector (Connector): the connector that received the message
            parameters (dict): the message received
        """
        logging.getLogger(__name__).debug("default process message : " + str(parameters))
