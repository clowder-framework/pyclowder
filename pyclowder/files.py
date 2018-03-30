"""Clowder API

This module provides simple wrappers around the clowder Files API
"""

import json
import os
import requests
from urllib3.filepost import encode_multipart_formdata

from datasets import DatasetsApi
from collections import CollectionsApi

# Some sources of urllib3 support warning suppression, but not all
try:
    from urllib3 import disable_warnings
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except:
    pass


# TODO: Functions outside FilesApi are deprecated
# pylint: disable=too-many-arguments
def download(connector, host, key, fileid, intermediatefileid=None, ext=""):
    """Download file to be processed from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    intermediatefileid -- either same as fileid, or the intermediate file to be used
    ext -- the file extension, the downloaded file will end with this extension
    """

    client = FilesApi(host=host, key=key)
    return client.download(fileid)

def download_info(connector, host, key, fileid):
    """Download file summary metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file to fetch metadata of
    """

    client = FilesApi(host=host, key=key)
    return client.download_info(fileid)

def download_metadata(connector, host, key, fileid, extractor=None):
    """Download file JSON-LD metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file to fetch metadata of
    extractor -- extractor name to filter results (if only one extractor's metadata is desired)
    """

    client = FilesApi(host=host, key=key)
    return client.download_metadata(fileid, extractor)

def submit_extraction(connector, host, key, fileid, extractorname):
    """Submit file for extraction by given extractor.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file UUID to submit
    extractorname -- registered name of extractor to trigger
    """

    client = FilesApi(host=host, key=key)
    return client.submit_extraction(fileid, extractorname)

def submit_extractions_by_dataset(connector, host, key, datasetid, extractorname, ext=False):
    """Manually trigger an extraction on all files in a dataset.

        This will iterate through all files in the given dataset and submit them to
        the provided extractor.

        Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        datasetid -- the dataset UUID to submit
        extractorname -- registered name of extractor to trigger
        ext -- extension to filter. e.g. 'tif' will only submit TIFF files for extraction.
    """

    dsapi = DatasetsApi(host=host, key=key)
    dsapi.submit_all_files_for_extraction(datasetid, extractorname, ext)

def submit_extractions_by_collection(connector, host, key, collectionid, extractorname, ext=False, recursive=True):
    """Manually trigger an extraction on all files in a collection.

        This will iterate through all datasets in the given collection and submit them to
        the submit_extractions_by_dataset(). Does not operate recursively if there are nested collections.

        Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        collectionid -- the collection UUID to submit
        extractorname -- registered name of extractor to trigger
        ext -- extension to filter. e.g. 'tif' will only submit TIFF files for extraction
        recursive -- whether to also submit child collection files recursively (defaults to True)
    """

    collapi = CollectionsApi(host=host, key=key)
    collapi.submit_all_files_for_extraction(collectionid, extractorname, ext, recursive)

def upload_metadata(connector, host, key, fileid, metadata):
    """Upload file JSON-LD metadata to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    metadata -- the metadata to be uploaded
    """

    client = FilesApi(host=host, key=key)
    return client.add_medadata(fileid, metadata)

# pylint: disable=too-many-arguments
def upload_preview(connector, host, key, fileid, previewfile, previewmetadata, preview_mimetype=None):
    """Upload preview to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    previewfile -- the file containing the preview
    previewmetadata -- any metadata to be associated with preview, can contain a section_id
                    to indicate the section this preview should be associated with.
    preview_mimetype -- (optional) MIME type of the preview file. By default, this is obtained from the
                    file itself and this parameter can be ignored. E.g. 'application/vnd.clowder+custom+xml'
    """

    client = FilesApi(host=host, key=key)
    return client.add_preview(fileid, previewfile, previewmetadata, preview_mimetype)

def upload_tags(connector, host, key, fileid, tags):
    """Upload file tag to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    tags -- the tags to be uploaded
    """

    client = FilesApi(host=host, key=key)
    return client.add_tags(fileid, tags)

def upload_thumbnail(connector, host, key, fileid, thumbnail):
    """Upload thumbnail to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that the thumbnail should be associated with
    thumbnail -- the file containing the thumbnail
    """

    client = FilesApi(host=host, key=key)
    return client.add_thumbnail(fileid, thumbnail)

def upload_to_dataset(connector, host, key, datasetid, filepath, check_duplicate=False):
    """Upload file to existing Clowder dataset.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset that the file should be associated with
    filepath -- path to file
    check_duplicate -- check if filename already exists in dataset and skip upload if so
    """

    client = FilesApi(host=host, key=key)
    return client.upload_to_dataset(datasetid, filepath, connector.mounted_paths, check_duplicate)


class FilesApi(object):
    """
        API to manage the REST CRUD endpoints for files.
    """

    def __init__(self, client=None, host=None, key=None, username=None, password=None):
        """Set client if provided otherwise create new one"""

        if client:
            self.api_client = client
        else:
            self.client = ClowderClient(host=host, key=key, username=username, password=password)


    def add_medadata(self, file_id, metadata):
        """Upload file JSON-LD metadata.

        Keyword arguments:
        file_id -- the file that is currently being processed
        metadata -- the metadata to be uploaded
        """

        self.client.post("files/%s/metadata.jsonld", metadata)


    def add_preview(self, file_id, preview_file, preview_metadata, preview_mime=None):
        """Upload a file preview.

        Keyword arguments:
        file_id -- the file that is currently being processed
        preview_file -- the file containing the preview
        preview_metadata: any metadata to be associated with preview,
                        this can contain a section_id to indicate the
                        section this preview should be associated with.
        preview_mime: mimetype of preview file being uploaded.
        """

        # upload preview
        prev = self.client.post_file("previews", preview_file, mime=preview_mime)

        # associate uploaded preview with original collection
        if file_id and not (preview_metadata and preview_metadata['section_id']):
            self.client.post("files/%s/previews/%s" % (file_id, prev['id']))

        # associate metadata with preview
        if preview_metadata is not None:
            self.client.post("previews/%s/metadata" % prev['id'], preview_metadata)

        return prev['id']


    def add_tags(self, file_id, tags):
        """Upload tags to a file.

        Keyword arguments:
        file_id -- the file that is currently being processed
        tags -- the tags to be uploaded
        """

        self.client.post("files/%s/tags", json.dumps(tags))


    def add_thumbnail(self, file_id, thumbnail):
        """Upload a file thumbnail.

        Keyword arguments:
        file_id -- the file that is currently being processed
        thumbnail -- the file containing the thumbnail
        """

        # upload preview
        thumb = self.client.post_file("fileThumbnail", thumbnail)

        # associate uploaded thumbnail with original file
        if file_id:
            self.client.post("files/%s/thumbnails/%s" % (file_id, thumb['id']))

        return thumb['id']


    def download(self, file_id):
        """Download a file.

        Keyword arguments:
        file_id -- id of file to download
        """

        return self.client.get_file("files/%s/blob" % file_id)


    def download_info(self, file_id):
        """Download file summary metadata.

        Keyword arguments:
        file_id -- id of file to download info for
        """

        return self.client.get("files/%s/metadata" % file_id)


    def download_metadata(self, file_id, extractor_name=None):
        """Download file JSON-LD metadata.

        Keyword arguments:
        file_id -- the file to fetch metadata of
        extractor_name -- extractor name to filter results (if only one extractor's metadata is desired)
        """

        params = None if extractor_name is None else {"extractor": extractor_name}
        return self.client.get("files/%s/metadata.jsonld" % file_id, params)


    def submit_extraction(self, file_id, extractor_name):
        """Submit file for extraction by given extractor.

        Keyword arguments:
        file_id -- the file UUID to submit
        extractor_name -- registered name of extractor to trigger
        """

        return self.client.post("files/%s/extractions" % file_id,
                                {"extractor": extractor_name})


    def upload_to_dataset(self, dataset_id, filepath, mounted_paths={}, check_duplicate=False):
        """Upload file to existing Clowder dataset.

        Keyword arguments:
        dataset_id -- the dataset that the file should be associated with
        filepath -- path to file
        check_duplicate -- check if filename already exists in dataset and skip upload if so
        mounted_paths -- dict mapping Clowder path : local path
        """

        if check_duplicate:
            dsapi = DatasetsApi(self.client)
            ds_files = dsapi.get_file_list(dataset_id)
            for f in ds_files:
                if f['filename'] == os.path.basename(filepath):
                    return None

        for source_path in mounted_paths:
            if filepath.startswith(mounted_paths[source_path]):
                return self._upload_to_dataset_local(dataset_id, filepath, mounted_paths)

        if os.path.exists(filepath):
            result = self.client.post_file("uploadToDataset/%s" % dataset_id, filepath)
            return result['id']
        else:
            raise


    def _upload_to_dataset_local(self, dataset_id, filepath, mounted_paths):
        """Upload file POINTER to existing Clowder dataset. Does not copy actual file bytes."""

        if os.path.exists(filepath):
            # Replace local path with remote path before uploading
            for source_path in mounted_paths:
                if filepath.startswith(mounted_paths[source_path]):
                    filepath = filepath.replace(mounted_paths[source_path], source_path)
                    break

            (content, header) = encode_multipart_formdata([
                ("file", '{"path": "%s"}' % filepath)
            ])
            result = self.client.post("uploadToDataset/%s" % dataset_id, content,
                                      headers={'Content-Type': header})
            return result['id']
        else:
            raise
