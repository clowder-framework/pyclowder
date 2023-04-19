"""Clowder API

This module provides simple wrappers around the clowder Datasets API
"""

import json
import logging
import os
import tempfile

import requests

from pyclowder.client import ClowderClient
from pyclowder.collections import get_datasets, get_child_collections, delete as delete_collection
from pyclowder.utils import StatusMessage


def create_empty(connector, client, datasetname, description, parentid=None, spaceid=None):
    """Create a new dataset in Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetname -- name of new dataset to create
    description -- description of new dataset
    parentid -- id of parent collection
    spaceid -- id of the space to add dataset to
    """

    logger = logging.getLogger(__name__)

    url = '%s/api/v2/datasets' % client.host
    headers = {"Content-Type": "application/json",
               "Authorization": "Bearer " + client.key}
    result = requests.post(url, headers=headers,
                           data=json.dumps({"name": datasetname, "description": description}),
                           verify=connector.ssl_verify if connector else True)

    result.raise_for_status()

    datasetid = result.json()['id']
    logger.debug("dataset id = [%s]", datasetid)

    return datasetid


def delete(connector, client , datasetid):
    """Delete dataset from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the dataset to delete
    """
    headers = {"Authorization": "Bearer " + client.key}

    url = "%s/api/v2/datasets/%s" % (client.host, datasetid)

    result = requests.delete(url, headers=headers, verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return json.loads(result.text)


# TODO collection not implemented yet in v2
def delete_by_collection(connector, client, collectionid, recursive=True, delete_colls=False):
    """Delete datasets from Clowder by iterating through collection.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    collectionid -- the collection to walk
    recursive -- whether to also iterate across child collections
    delete_colls -- whether to also delete collections containing the datasets
    """
    dslist = get_datasets(connector, client.host, client.key, collectionid)
    for ds in dslist:
        delete(connector, client.host, client.key, ds['id'])

    if recursive:
        childcolls = get_child_collections(connector, client.host, client.key, collectionid)
        for coll in childcolls:
            delete_by_collection(connector, client.host, client.key, coll['id'], recursive, delete_colls)

    if delete_colls:
        delete_collection(connector, client.host, client.key, collectionid)


def download(connector, client, datasetid):
    """Download dataset to be processed from Clowder as zip file.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the file that is currently being processed
    """

    connector.message_process({"type": "dataset", "id": datasetid}, "Downloading dataset.")

    headers = {"Authorization": "Bearer " + client.key}
    # fetch dataset zipfile
    url = '%s/api/v2/datasets/%s/download' % (client.host, datasetid)
    result = requests.get(url, stream=True, headers=headers,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    (filedescriptor, zipfile) = tempfile.mkstemp(suffix=".zip")
    with os.fdopen(filedescriptor, "wb") as outfile:
        for chunk in result.iter_content(chunk_size=10 * 1024):
            outfile.write(chunk)

    return zipfile


def download_metadata(connector, client, datasetid, extractor=None):
    """Download dataset JSON-LD metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the dataset to fetch metadata of
    extractor -- extractor name to filter results (if only one extractor's metadata is desired)
    """
    headers = {"Authorization": "Bearer " + client.key}

    filterstring = "" if extractor is None else "&extractor=%s" % extractor
    url = '%s/api/v2/datasets/%s/metadata' % (client.host, datasetid)

    # fetch data
    result = requests.get(url, stream=True, headers=headers,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return result.json()


def get_info(connector, client, datasetid):
    """Get basic dataset information from UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the dataset to get info of
    """
    headers = {"Authorization": "Bearer " + client.key}

    url = "%s/api/v2/datasets/%s" % (client.host, datasetid)

    result = requests.get(url, headers=headers,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return json.loads(result.text)


def get_file_list(connector, client, datasetid):
    """Get list of files in a dataset as JSON object.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the dataset to get filelist of
    """
    headers = {"Authorization": "Bearer " + client.key}

    url = "%s/api/v2/datasets/%s/files" % (client.host, datasetid)

    result = requests.get(url, headers=headers, verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return json.loads(result.text)


def remove_metadata(connector, client, datasetid, extractor=None):
    """Delete dataset JSON-LD metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the dataset to fetch metadata of
    extractor -- extractor name to filter deletion
                    !!! ALL JSON-LD METADATA WILL BE REMOVED IF NO extractor PROVIDED !!!
    """
    headers = {"Authorization": "Bearer " + client.key}

    filterstring = "" if extractor is None else "&extractor=%s" % extractor
    url = '%s/api/v2/datasets/%s/metadata' % (client.host, datasetid)

    # fetch data
    result = requests.delete(url, stream=True, headers=headers,
                             verify=connector.ssl_verify if connector else True)
    result.raise_for_status()


def submit_extraction(connector, client, datasetid, extractorname):
    """Submit dataset for extraction by given extractor.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the dataset UUID to submit
    extractorname -- registered name of extractor to trigger
    """
    headers = {'Content-Type': 'application/json',
                "Authorization": "Bearer " + client.key}

    url = "%s/api/v2/datasets/%s/extractions?key=%s" % (client.host, datasetid)

    result = requests.post(url,
                           headers=headers,
                           data=json.dumps({"extractor": extractorname}),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return result.status_code


def upload_metadata(connector, client, datasetid, metadata):
    """Upload dataset JSON-LD metadata to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the dataset that is currently being processed
    metadata -- the metadata to be uploaded
    """
    headers = {'Content-Type': 'application/json',
               "Authorization": "Bearer " + client.key}
    connector.message_process({"type": "dataset", "id": datasetid}, "Uploading dataset metadata.")


    url = '%s/api/v2/datasets/%s/metadata' % (client.host, datasetid)
    result = requests.post(url, headers=headers, data=json.dumps(metadata),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

