"""Clowder API

This module provides simple wrappers around the clowder Datasets API
"""

import logging
import tempfile
from pyclowder.client import ClowderClient
from pyclowder.files import FilesApi


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

    def add_file(self, dataset_id, file):
        """Add a file to a dataset.

        Keyword arguments:
        dataset_id -- the dataset that is currently being processed
        file -- path to file
        """
        logging.debug("Uploading a file to dataset %s" % dataset_id)
        try:
            return self.client.post_file("/uploadToDataset/%s" % dataset_id, file)
        except Exception as e:
            logging.error("Error upload to dataset %s: %s" % (dataset_id, str(e)))

    def add_folder(self, dataset_id, folder, parent_type, parent_id):
        """Add a folder to a dataset.

        Keyword arguments:
        dataset_id -- the dataset that is currently being processed
        folder -- path to folder
        parent_type -- type of parent dataset
        """

        body = {
            "name": folder,
            "parentId": parent_id,
            "parentType": parent_type,
        }

        return self.client.post('/datasets/%s/newFolder' % dataset_id, body)

    def add_metadata(self, dataset_id, metadata):
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

    def download(self, dataset_id, output_file=None):
        """Download a dataset from Clowder as zip.

        Keyword arguments:
        dataset_id -- id of dataset to download
        output_file -- name of zipfile to create; will create temp file if not specified
        """

        if output_file is None:
            output_file = tempfile.mkstemp(suffix=".zip")

        return self.client.get_file("datasets/%s/download" % dataset_id, filename=output_file)

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

    def get_metadata(self, dataset_id, extractor_name=None):
        """Download dataset JSON-LD metadata from Clowder.

        Keyword arguments:
        dataset_id -- the dataset to fetch metadata of
        extractor_name -- extractor name to filter results (if only one extractor's metadata is desired)
        """

        params = None if extractor_name is None else {"extractor": extractor_name}
        return self.client.get("datasets/%s/metadata.jsonld" % dataset_id, params)

    def list(self):
        """Get the list of all available datasets."""

        logging.debug("Getting all datasets")
        try:
            return self.client.get("datasets")
        except Exception as e:
            logging.error("Error retrieving dataset list: %s", str(e))

    def move_file_to_folder(self, dataset_id, folder_id, file_id):
        """Move a file in the dataset to a folder within the same dataset.

        Keyword arguments:
        dataset_id -- the dataset to process
        folder_id -- the folder to move the file into
        file_id -- the file to move
        """

        body = {
            'folderId/': folder_id
        }

        return self.client.post('/datasets/%s/moveFile/%s/%s' % (dataset_id, folder_id, file_id), body)

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

    def submit_files_for_extraction(self, dataset_id, extractor_name, extension=None):
        """Manually trigger an extraction on all files in a dataset.

        Keyword arguments:
        dataset_id -- the dataset UUID to submit
        extractor_name -- registered name of extractor to trigger
        extension -- extension to filter. e.g. 'tif' will only submit TIFF files for extraction
        """

        fileapi = FilesApi(self.client)
        filelist = self.get_file_list(dataset_id)
        for fi in filelist:
            if extension and not fi['filename'].endswith(extension):
                continue
            fileapi.submit_extraction(fi['id'], extractor_name)
