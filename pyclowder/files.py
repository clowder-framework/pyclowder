"""Clowder API

This module provides simple wrappers around the clowder Files API
"""

import json
import os
import requests
from urllib3.filepost import encode_multipart_formdata
from client import ClowderClient

# Some sources of urllib3 support warning suppression, but not all
try:
    from urllib3 import disable_warnings
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except:
    pass


class FilesApi(object):
    """
        API to manage the REST CRUD endpoints for files.
    """

    def __init__(self, client=None, host=None, key=None, username=None, password=None):
        """Set client if provided otherwise create new one"""
        from pyclowder.datasets import DatasetsApi
        self.DatasetsApi = DatasetsApi

        if client:
            self.client = client
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

        self.client.post("files/%s/tags" % file_id, json.dumps(tags))

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

    def get_info(self, file_id):
        """Download file summary metadata.

        Keyword arguments:
        file_id -- id of file to download info for
        """

        return self.client.get("files/%s/metadata" % file_id)

    def get_metadata(self, file_id, extractor_name=None):
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
            dsapi = self.DatasetsApi(self.client)
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
