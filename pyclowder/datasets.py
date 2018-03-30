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


# TODO: Functions outside DatasetsApi are deprecated
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

    client = DatasetsApi(host=host, key=key)
    return client.create(datasetname, description, parentid, spaceid)

def delete(connector, host, key, datasetid):
    """Delete dataset from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to delete
    """

    client = DatasetsApi(host=host, key=key)
    return client.delete(datasetid)

# TODO: Put this in BulkOperationsApi?
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
    """Download dataset as zip file.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the file that is currently being processed
    """

    client = DatasetsApi(host=host, key=key)
    return client.download(datasetid)

def download_metadata(connector, host, key, datasetid, extractor=None):
    """Download dataset JSON-LD metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to fetch metadata of
    extractor -- extractor name to filter results (if only one extractor's metadata is desired)
    """

    client = DatasetsApi(host=host, key=key)
    return client.download_metadata(datasetid, extractor)

def get_info(connector, host, key, datasetid):
    """Get basic dataset information from UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to get info of
    """

    client = DatasetsApi(host=host, key=key)
    return client.get_info(datasetid)

def get_file_list(connector, host, key, datasetid):
    """Get list of files in a dataset as JSON object.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to get filelist of
    """

    client = DatasetsApi(host=host, key=key)
    return client.get_file_list(datasetid)

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

    client = DatasetsApi(host=host, key=key)
    return client.remove_metadata(datasetid, extractor)

def submit_extraction(connector, host, key, datasetid, extractorname):
    """Submit dataset for extraction by given extractor.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset UUID to submit
    extractorname -- registered name of extractor to trigger
    """

    client = DatasetsApi(host=host, key=key)
    return client.submit_extraction(datasetid, extractorname)

# TODO: Put this in BulkOperationsApi?
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

    client = DatasetsApi(host=host, key=key)
    return client.add_metadata(datasetid, metadata)


class DatasetsApi(object):
    """
        API to manage the REST CRUD endpoints for datasets.
    """

    def __init__(self, client=None, host=None, key=None, username=None, password=None):
        """Set client if provided otherwise create new one"""

        if client:
            self.api_client = client
        else:
            self.client = ClowderClient(host=host, key=key, username=username, password=password)

    def datasets_get(self):
        """
        Get the list of all available datasets.

        :return: Full list of datasets.
        :rtype: `requests.Response`
        """
        logging.debug("Getting all datasets")
        try:
            return self.client.get("/datasets/")
        except Exception as e:
            logging.error("Error retrieving dataset list: %s", e.message)

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
            logging.error("Error retrieving dataset %s: %s" % (dataset_id, e.message))

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
            logging.error("Error adding datapoint %s: %s" % (dataset_id, e.message))

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
            logging.error("Error retrieving dataset %s: %s" % (dataset_id, e.message))

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
            logging.error("Error upload to dataset %s: %s" % (dataset_id, e.message))

    def add_metadata(self, dataset_id, metadata):
        """
        Add a file to a dataset

        :return: If successfull or not.
        :rtype: `requests.Response`
        """

        logging.debug("Update metadata of dataset %s" % dataset_id)
        try:
            return self.client.post("/datasets/%s/metadata" % dataset_id, metadata)
        except Exception:
            logging.error("Error upload to dataset %s: %s" % (dataset_id, e.message))


    def add_metadata(self, dataset_id, metadata):
        """Upload dataset JSON-LD metadata.

        Keyword arguments:
        dataset_id -- the dataset that is currently being processed
        metadata -- the metadata to be uploaded
        """

        self.client.post("datasets/%s/metadata.jsonld", metadata)


    def create(self, name, description="", parent_id=None, space_id=None):
        """Create a new dataset in Clowder.

        Keyword arguments:
        name -- name of new dataset to create
        description -- description of new dataset
        parent_id -- id of parent collection (or list of ids)
        space_id -- id of the space to add dataset to (or list of ids)
        """

        body = {
            "name": name,
            "description": description,
        }

        if parent_id:
            if isinstance(parent_id, list):
                body["collection"] = parent_id
            else:
                body["collection"] = [parent_id]
        if space_id:
            if isinstance(space_id, list):
                body["space"] = space_id
            else:
                body["space"] = [space_id]

        result = self.client.post("datasets/createempty", body)
        return result['id']


    def delete(self, dataset_id):
        """Delete a dataset from Clowder.

        Keyword arguments:
        dataset_id -- id of dataset to delete
        """

        return self.client.delete("datasets/%s" % dataset_id)


    def download(self, dataset_id):
        """Download a dataset from Clowder as zip.

        Keyword arguments:
        dataset_id -- id of dataset to download
        """

        fname = tempfile.mkstemp(suffix=".zip")
        return self.client.get_file("datasets/%s/download" % dataset_id, filename=fname)


    def download_metadata(self, dataset_id, extractor_name=None):
        """Download dataset JSON-LD metadata from Clowder.

        Keyword arguments:
        dataset_id -- the dataset to fetch metadata of
        extractor_name -- extractor name to filter results (if only one extractor's metadata is desired)
        """

        params = None if extractor_name is None else {"extractor": extractor_name}
        return self.client.get("datasets/%s/metadata.jsonld" % dataset_id, params)


    def get_info(self, dataset_id):
        """Download basic dataset information.

        Keyword arguments:
        dataset_id -- id of dataset to get info for
        """

        return self.client.get("datasets/%s" % dataset_id)


    def get_file_list(self, dataset_id):
        """Download list of dataset files as JSON.

        Keyword arguments:
        dataset_id -- id of dataset to get files for
        """

        return self.client.get("datasets/%s/files" % dataset_id)


    def remove_metadata(self, dataset_id, extractor_name=None):
        """Delete dataset JSON-LD metadata, optionally filtered by extractor name.

        Keyword arguments:
        dataset_id -- the dataset to fetch metadata of
        extractor_name -- extractor name to filter deletion
                        !!! ALL JSON-LD METADATA WILL BE REMOVED IF NO extractor PROVIDED !!!
        """

        params = None if extractor_name is None else {"extractor": extractor_name}
        return self.client.delete("datasets/%s/metadata.jsonld" % dataset_id, params)


    def submit_extraction(self, dataset_id, extractor_name):
        """Submit dataset for extraction by given extractor.

        Keyword arguments:
        dataset_id -- the dataset UUID to submit
        extractor_name -- registered name of extractor to trigger
        """

        return self.client.post("datasets/%s/extractions" % dataset_id,
                                {"extractor": extractor_name})
