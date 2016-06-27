"""Clowder Utils

This module contains utilities that make it easier to work with clowder.
Amongst these utilities is a simple way to initialize the logging system
from either a file or the command line.
"""

import json
import logging
import logging.config
import os

from enum import Enum

import yaml


# this takes advantage of the fact that 0 == False and anything else == True
class CheckMessage(Enum):
    """Value to be returned from check_message function.

    Based on the result the following actions will happen:
    - ignore : the process_message function is not called
    - download : the input file will be downloaded and process_message is called
    - bypass : the file is NOT downloaded but process_message is still called
    """

    ignore = 0
    download = 1
    bypass = 2


def setup_logging(config_info=None):
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
