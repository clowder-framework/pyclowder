"""Clowder API

This module provides simple wrappers around the clowder Datasets API
"""

import logging
import tempfile
from client import ClowderClient


class DatasetsApi(object):
    """
        API to manage the REST CRUD endpoints for datasets.
    """

    def __init__(self, client=None, host=None, key=None, username=None, password=None):
        """Set client if provided otherwise create new one"""
        from pyclowder.files import FilesApi
        self.FilesApi = FilesApi

        if client:
            self.client = client
        else:
            self.client = ClowderClient(host=host, key=key, username=username, password=password)

    def delete_by_collection(self, collection_id, recursive=True, delete_colls=False):
        from pyclowder.collections import CollectionsApi
        collapi = CollectionsApi
        return collapi.delete_all_datasets(collection_id, recursive, delete_colls)

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
        except Exception as e:
            logging.error("Error upload to dataset %s: %s" % (dataset_id, e.message))

    def add_metadata_jsonld(self, dataset_id, metadata):
        """Upload dataset JSON-LD metadata.

        Keyword arguments:
        dataset_id -- the dataset that is currently being processed
        metadata -- the metadata to be uploaded
        """

        self.client.post("datasets/%s/metadata.jsonld" % dataset_id, metadata)

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

        logging.debug("Getting dataset %s" % dataset_id)
        try:
            return self.client.get("/datasets/%s" % dataset_id)
        except Exception as e:
            logging.error("Error retrieving dataset %s: %s" % (dataset_id, e.message))

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

    def submit_all_files_for_extraction(self, dataset_id, extractor_name, extension=None):
        """Manually trigger an extraction on all files in a dataset.

        Keyword arguments:
        dataset_id -- the dataset UUID to submit
        extractor_name -- registered name of extractor to trigger
        extension -- extension to filter. e.g. 'tif' will only submit TIFF files for extraction
        """

        fileapi = self.FilesApi(self.client)
        filelist = self.get_file_list(dataset_id)
        for fi in filelist:
            if extension and not fi['filename'].endswith(extension):
                continue
            fileapi.submit_extraction(fi['id'], extractor_name)
