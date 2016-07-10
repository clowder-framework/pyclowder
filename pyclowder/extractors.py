"""Clowder Extractors

One of the most interesting aspects of Clowder is the ability to extract
metadata from any file. This ability is created using extractors. To make it
easier to create these extractors in python we have created a class called
Extractor that can be used as the base class. The extractor should implement
one or both of the check_message and process message functions. The
check_message should return one of the values in CheckMessage.
"""

import logging
import logging.config
import threading

import time

from pyclowder.connectors import RabbitMQConnector, HPCConnector
from pyclowder.utils import CheckMessage


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

    # pylint: disable=no-self-use,unused-argument
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

    # pylint: disable=no-self-use,unused-argument
    def process_message(self, connector, parameters):
        """Process the message and send results back to clowder.

        Args:
            connector (Connector): the connector that received the message
            parameters (dict): the message received
        """
        logging.getLogger(__name__).debug("default process message : " + str(parameters))
