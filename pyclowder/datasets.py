"""Clowder API

This module provides simple wrappers around the clowder Datasets API
"""
import json
import logging
import os
import tempfile
import requests
from pyclowder.utils import StatusMessage


def create_empty(connector, host, key, datasetname, description, parentid=None, spaceid=None):
    """Create a new dataset in Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetname -- name of new dataset to create
    description -- description of new dataset
    parentid -- id of parent collection
    spaceid -- id of the space to add dataset to
    """

    logger = logging.getLogger(__name__)

    url = '%sapi/datasets/createempty?key=%s' % (host, key)

    if parentid:
        if spaceid:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data={"name": datasetname, "description": description, "collection": [parentid],
                                   "space": [spaceid]}, verify=connector.ssl_verify if connector else True)
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data={"name": datasetname, "description": description, "collection": [parentid]},
                                   verify=connector.ssl_verify if connector else True)
    else:
        if spaceid:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data={"name": datasetname, "description": description, "space": [spaceid]},
                                   verify=connector.ssl_verify if connector else True)
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data={"name": datasetname, "description": description},
                                   verify=connector.ssl_verify if connector else True)

    result.raise_for_status()

    datasetid = result.json()['id']
    logger.debug("dataset id = [%s]", datasetid)

    return datasetid


def download(connector, host, key, datasetid):
    """Download dataset to be processed from Clowder as zip file.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the file that is currently being processed
    """

    connector.status_update(StatusMessage.processing, {"type": "dataset", "id": datasetid}, "Downloading dataset.")

    # fetch dataset zipfile
    url = '%sapi/datasets/%s/download?key=%s' % (host, datasetid, key)
    result = requests.get(url, stream=True,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    (filedescriptor, zipfile) = tempfile.mkstemp(suffix=".zip")
    with os.fdopen(filedescriptor, "w") as outfile:
        for chunk in result.iter_content(chunk_size=10*1024):
            outfile.write(chunk)

    return zipfile


def download_metadata(connector, host, key, datasetid, extractor=None):
    """Download dataset JSON-LD metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to fetch metadata of
    extractor -- extractor name to filter results (if only one extractor's metadata is desired)
    """

    filterstring = "" if extractor is None else "&extractor=%s" % extractor
    url = '%sapi/datasets/%s/metadata.jsonld?key=%s%s' % (host, datasetid, key, filterstring)

    # fetch data
    result = requests.get(url, stream=True,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return result.json()


def get_info(connector, host, key, datasetid):
    """Get basic dataset information from UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to get info of
    """

    url = "%sapi/datasets/%s?key=%s" % (host, datasetid, key)

    result = requests.get(url,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return json.loads(result.text)


def get_file_list(connector, host, key, datasetid):
    """Get list of files in a dataset as JSON object.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to get filelist of
    """

    url = "%sapi/datasets/%s/listFiles?key=%s" % (host, datasetid, key)

    result = requests.get(url, verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return json.loads(result.text)


def remove_metadata(connector, host, key, datasetid, extractor=None):
    """Delete dataset JSON-LD metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to fetch metadata of
    extractor -- extractor name to filter deletion
                    !!! ALL JSON-LD METADATA WILL BE REMOVED IF NO extractor PROVIDED !!!
    """

    filterstring = "" if extractor is None else "&extractor=%s" % extractor
    url = '%sapi/datasets/%s/metadata.jsonld?key=%s%s' % (host, datasetid, key, filterstring)

    # fetch data
    result = requests.delete(url, stream=True,
                             verify=connector.ssl_verify if connector else True)
    result.raise_for_status()


def submit_extraction(connector, host, key, datasetid, extractorname):
    """Submit dataset for extraction by given extractor.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset UUID to submit
    extractorname -- registered name of extractor to trigger
    """

    url = "%sapi/datasets/%s/extractions?key=%s" % (host, datasetid, key)

    result = requests.post(url,
                           headers={'Content-Type': 'application/json'},
                           data=json.dumps({"extractor": extractorname}),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return result.status_code


def upload_metadata(connector, host, key, datasetid, metadata):
    """Upload dataset JSON-LD metadata to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset that is currently being processed
    metadata -- the metadata to be uploaded
    """

    connector.status_update(StatusMessage.processing, {"type": "dataset", "id": datasetid},
                            "Uploading dataset metadata.")

    headers = {'Content-Type': 'application/json'}
    url = '%sapi/datasets/%s/metadata.jsonld?key=%s' % (host, datasetid, key)
    result = requests.post(url, headers=headers, data=json.dumps(metadata),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()
