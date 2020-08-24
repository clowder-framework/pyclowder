"""Clowder Utils

This module contains utilities that make it easier to work with clowder.
Amongst these utilities is a simple way to initialize the logging system
from either a file or the command line.
"""

import datetime
import json
import logging
import logging.config
import os
import time
import zipfile
import tempfile
import requests

from enum import Enum

import yaml


# this takes advantage of the fact that 0 == False and anything else == True
# pylint: disable=too-few-public-methods
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


class StatusMessage(Enum):
    """Value of status to be sent to status_update function.

    Extractors can still define custom content in the message field
    of that function, but the status itself must be one of these. The
    full string will be STATUS: MESSAGE.
    """

    start = "STARTED"
    processing = "PROCESSING"
    done = "SUCCEEDED"
    skip = "SKIPPED"
    error = "ERROR"
    retry = "RESUBMITTED"


def iso8601time():
    if time.daylight == 0:
        tz = str.format('{0:+06.2f}', -float(time.timezone) / 3600).replace('.', ':')
    else:
        tz = str.format('{0:+06.2f}', -float(time.altzone) / 3600).replace('.', ':')
    now = datetime.datetime.now().replace(microsecond=0)
    return now.isoformat() + tz


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
    # if logging config is a url, download the file

    if config_info:
        temp_file = None
        if config_info.startswith("http://") or config_info.startswith("https://"):
            r = requests.get(config_info)
            r.raise_for_status()
            (temp_file, abs_path) = tempfile.mkstemp()
            with os.fdopen(temp_file, "wb") as tmp:
                for chunk in r.iter_content(chunk_size=1024):
                    tmp.write(chunk)
            config_info = temp_file

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

        if temp_file:
            os.remove(temp_file)
    else:
        logging.basicConfig(format='%(asctime)-15s [%(threadName)-15s] %(levelname)-7s :'
                                   ' %(name)s - %(message)s',
                            level=logging.INFO)
        logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARN)


def extract_zip_contents(zipfilepath):
    """Extract contents of a zipfile and return contents as list of file paths

    Keyword arguments:
    zipfilepath -- path of zipfile to extract
    """

    zipobj = zipfile.ZipFile(zipfilepath)
    output_folder = zipfilepath.replace(".zip", "")
    zipobj.extractall(output_folder)

    file_list = []
    for root, _, files in os.walk(output_folder):
        for currfile in files:
            file_list.append(os.path.join(root, currfile))

    return file_list
