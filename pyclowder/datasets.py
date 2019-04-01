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
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "collection": [parentid], "space": [spaceid]}),
                                   verify=connector.ssl_verify if connector else True)
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "collection": [parentid]}),
                                   verify=connector.ssl_verify if connector else True)
    else:
        if spaceid:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "space": [spaceid]}),
                                   verify=connector.ssl_verify if connector else True)
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description}),
                                   verify=connector.ssl_verify if connector else True)

    result.raise_for_status()

    datasetid = result.json()['id']
    logger.debug("dataset id = [%s]", datasetid)

    return datasetid


def delete(connector, host, key, datasetid):
    """Delete dataset from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to delete
    """
    url = "%sapi/datasets/%s?key=%s" % (host, datasetid, key)

    result = requests.delete(url, verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return json.loads(result.text)


def delete_by_collection(connector, host, key, collectionid, recursive=True, delete_colls=False):
    """Delete datasets from Clowder by iterating through collection.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    collectionid -- the collection to walk
    recursive -- whether to also iterate across child collections
    delete_colls -- whether to also delete collections containing the datasets
    """
    dslist = get_datasets(connector, host, key, collectionid)
    for ds in dslist:
        delete(connector, host, key, ds['id'])

    if recursive:
        childcolls = get_child_collections(connector, host, key, collectionid)
        for coll in childcolls:
            delete_by_collection(connector, host, key, coll['id'], recursive, delete_colls)

    if delete_colls:
        delete_collection(connector, host, key, collectionid)


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
    with os.fdopen(filedescriptor, "wb") as outfile:
        for chunk in result.iter_content(chunk_size=10 * 1024):
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

    url = "%sapi/datasets/%s/files?key=%s" % (host, datasetid, key)

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


def submit_extractions_by_collection(connector, host, key, collectionid, extractorname, recursive=True):
    """Manually trigger an extraction on all datasets in a collection.

        This will iterate through all datasets in the given collection and submit them to
        the provided extractor.

        Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        datasetid -- the dataset UUID to submit
        extractorname -- registered name of extractor to trigger
        recursive -- whether to also submit child collection datasets recursively (defaults to True)
    """

    dslist = get_datasets(connector, host, key, collectionid)

    for ds in dslist:
        submit_extraction(connector, host, key, ds['id'], extractorname)

    if recursive:
        childcolls = get_child_collections(connector, host, key, collectionid)
        for coll in childcolls:
            submit_extractions_by_collection(connector, host, key, coll['id'], extractorname, recursive)


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


class DatasetsApi(object):
    """
        API to manage the REST CRUD endpoints for datasets.
    """

    def __init__(self, client=None, host=None, key=None,
                 username=None, password=None):
        """Set client if provided otherwise create new one"""
        if client:
            self.client = client
        else:
            self.client = ClowderClient(host=host, key=key,
                                        username=username, password=password)

    def datasets_get(self):
        """
        Get the list of all available datasets.

        :return: Full list of datasets.
        :rtype: `requests.Response`
        """
        logging.debug("Getting all datasets")
        try:
            return self.client.get("/datasets")
        except Exception as e:
            logging.error("Error retrieving dataset list: %s", str(e))

    def dataset_get(self, dataset_id):
        """
        Get a specific dataset by id.

        :return: Sensor object as JSON.
        :rtype: `requests.Response`
        """
        logging.debug("Getting dataset %s" % dataset_id)
        try:
            return self.client.get("/datasets/%s" % dataset_id)
        except Exception as e:
            logging.error("Error retrieving dataset %s: %s" % (dataset_id, str(e)))

    def create_empty(self, dataset_id):
        """
        Create dataset.

        :return: If successful or not.
        :rtype: `requests.Response`
        """
        logging.debug("Adding dataset")
        try:
            return self.client.post("/datasets/createempty", dataset_id)
        except Exception as e:
            logging.error("Error adding datapoint %s: %s" % (dataset_id, str(e)))

    def dataset_delete(self, dataset_id):
        """
        Delete a specific dataset by id.

        :return: If successfull or not.
        :rtype: `requests.Response`
        """
        logging.debug("Deleting dataset %s" % dataset_id)
        try:
            return self.client.delete("/datasets/%s" % dataset_id)
        except Exception as e:
            logging.error("Error retrieving dataset %s: %s" % (dataset_id, str(e)))

    def upload_file(self, dataset_id, file):
        """
        Add a file to a dataset.

        :return: If successfull or not.
        :rtype: `requests.Response`
        """
        logging.debug("Uploading a file to dataset %s" % dataset_id)
        try:
            return self.client.post_file("/uploadToDataset/%s" % dataset_id, file)
        except Exception as e:
            logging.error("Error upload to dataset %s: %s" % (dataset_id, str(e)))

    def add_metadata(self, dataset_id, metadata):
        """
        Add a file to a dataset

        :return: If successfull or not.
        :rtype: `requests.Response`
        """

        logging.debug("Update metadata of dataset %s" % dataset_id)
        try:
            return self.client.post("/datasets/%s/metadata" % dataset_id, metadata)
        except Exception as e:
            logging.error("Error upload to dataset %s: %s" % (dataset_id, str(e)))
