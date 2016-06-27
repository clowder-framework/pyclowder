import json
import logging
import logging.config
import os
import threading

import time
import yaml

from clowder.connectors import RabbitMQConnector, HPCConnector, CheckMessage


class Extractor:
    def __init__(self, extractor_name, ssl_verify=False):
        self.extractor_name = extractor_name
        self.ssl_verify = ssl_verify

        # this will override the process_message and check_message functions
        # if self.check_message:
        #     Extractor.__dict__['_Extractor_check_message'] = self.check_message
        # if self.process_message:
        #     Extractor.__dict__['_Extractor_process_message'] = self.process_message

    def setup_logging(self, config_info=None):
        if config_info:
            if os.path.isfile(config_info):
                if config_info.endswith('.yml'):
                    with open(config_info, 'r') as f:
                        config = yaml.safe_load(f)
                        logging.config.dictConfig(config)
                elif config_info.endswith('.json'):
                    with open(config_info, 'r') as f:
                        config = json.load(f)
                        logging.config.dictConfig(config)
                else:
                    logging.config.fileConfig(config_info)
            else:
                config = json.load(config_info)
                logging.config.dictConfig(config)
        else:
            logging.basicConfig(format='%(asctime)-15s [%(threadName)-15s] %(levelname)-7s : %(name)s - %(message)s',
                                level=logging.INFO)
            logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARN)

    def start_connector(self, connector, num, **kwargs):
        logger = logging.getLogger(__name__)
        connectors = list()
        for x in xrange(num):
            conn = None
            if connector == "RabbitMQ":
                if 'rabbitmq_uri' not in kwargs:
                    logger.error("Missing URI for RabbitMQ")
                else:
                    conn = RabbitMQConnector(self.extractor_name,
                                             check_message=self.check_message,
                                             process_message=self.process_message,
                                             rabbitmq_uri=kwargs.get('rabbitmq_uri'),
                                             rabbitmq_exchange=kwargs.get('rabbitmq_exchange'),
                                             rabbitmq_key=kwargs.get('rabbitmq_key'))
                    conn.connect()
            elif connector == "HPC":
                if 'hpc_picklefile' not in kwargs:
                    logger.error("Missing hpc_picklefile for HPCExtractor")
                else:
                    conn = HPCConnector(self.extractor_name,
                                        check_message=self.check_message,
                                        process_message=self.process_message,
                                        picklefile=kwargs.get('hpc_picklefile'))
            else:
                logger.error("Could not create instance of %s connector." % connector)

            if conn:
                conn.register_extractor(kwargs.get('regstration_endpoints'))
                connectors.append(conn)
                threading.Thread(target=conn.listen, name="Connector-" + str(x)).start()

        logger.info("Waiting for messages. To exit press CTRL+C")
        try:
            while connectors:
                time.sleep(1)
        except KeyboardInterrupt:
            pass
        except:
            logger.exception("Error while consuming messages.")

        while connectors:
            connectors.pop(0).stop()

    def check_message(self, connector, parameters):
        logging.getLogger(__name__).debug("default check message : " + str(parameters))
        return CheckMessage.download

    def process_message(self, debug, parameters):
        logging.getLogger(__name__).debug("default process message : " + str(parameters))
        pass
