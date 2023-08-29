"""Clowder API

This module provides simple wrappers around the clowder Datasets API
"""

import json
import logging
import os
import tempfile

import requests
from pyclowder.client import ClowderClient
import pyclowder.api.v2.datasets as v2datasets
import pyclowder.api.v1.datasets as v1datasets
from pyclowder.collections import get_datasets, get_child_collections, delete as delete_collection
from pyclowder.utils import StatusMessage

clowder_version = int(os.getenv('CLOWDER_VERSION', '1'))
# Import dataset API methods based on Clowder version
if clowder_version == 2:
    import pyclowder.api.v2.datasets as datasets
else:
    import pyclowder.api.v1.datasets as datasets


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
    client = ClowderClient(host=host, key=key)
    datasetid = datasets.create_empty(connector, client, datasetname, description, parentid, spaceid)
    return datasetid


def delete(connector, host, key, datasetid):
    """Delete dataset from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to delete
    """
    client = ClowderClient(host=host, key=key)
    result = datasets.delete(connector, client, datasetid)
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
    client = ClowderClient(host=host, key=key)
    dslist = get_datasets(connector, host, key, collectionid)
    for ds in dslist:
        delete(connector, client, ds['id'])

    if recursive:
        childcolls = get_child_collections(connector, client, collectionid)
        for coll in childcolls:
            delete_by_collection(connector, host, key, coll['id'], recursive, delete_colls)

    if delete_colls:
        delete_collection(connector, client, collectionid)


def download(connector, host, key, datasetid):
    """Download dataset to be processed from Clowder as zip file.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the file that is currently being processed
    """
    client = ClowderClient(host=host, key=key)
    zipfile = datasets.download(connector, client, datasetid)
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
    client = ClowderClient(host=host, key=key)
    result_json = datasets.download_metadata(connector, client, datasetid, extractor)
    return result_json


def get_info(connector, host, key, datasetid):
    """Get basic dataset information from UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to get info of
    """
    client = ClowderClient(host=host, key=key)
    info = datasets.get_info(connector, client, datasetid)
    return info


def get_file_list(connector, host, key, datasetid):
    """Get list of files in a dataset as JSON object.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset to get filelist of
    """
    client = ClowderClient(host=host, key=key)
    file_list = datasets.get_file_list(connector, client, datasetid)
    return file_list


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
    client = ClowderClient(host=host, key=key)
    datasets.remove_metadata(connector, client, datasetid, extractor)


def submit_extraction(connector, host, key, datasetid, extractorname):
    """Submit dataset for extraction by given extractor.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset UUID to submit
    extractorname -- registered name of extractor to trigger
    """
    client = ClowderClient(host=host, key=key)
    return datasets.submit_extraction(connector, client, datasetid, extractorname)


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
    client = ClowderClient(host=host, key=key)
    dslist = get_datasets(connector, client, collectionid)

    for ds in dslist:
        submit_extraction(connector, client, ds['id'], extractorname)

    if recursive:
        childcolls = get_child_collections(connector, client, collectionid)
        for coll in childcolls:
            submit_extractions_by_collection(connector, client, coll['id'], extractorname, recursive)


def upload_tags(connector, host, key, datasetid, tags):
    """Upload dataset tag to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset that is currently being processed
    tags -- the tags to be uploaded
    """
    client = ClowderClient(host=host, key=key)
    connector.status_update(StatusMessage.processing, {"type": "dataset", "id": datasetid}, "Uploading dataset tags.")

    headers = {'Content-Type': 'application/json'}
    url = '%sapi/datasets/%s/tags?key=%s' % (client.host, datasetid, client.key)
    result = connector.post(url, headers=headers, data=json.dumps(tags),
                            verify=connector.ssl_verify if connector else True)


def upload_metadata(connector, host, key, datasetid, metadata):
    """Upload dataset JSON-LD metadata to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset that is currently being processed
    metadata -- the metadata to be uploaded
    """
    client = ClowderClient(host=host, key=key)
    datasets.upload_metadata(connector, client, datasetid, metadata)


def upload_preview(connector, host, key, datasetid, previewfile, previewmetadata=None, preview_mimetype=None,
                   visualization_name=None, visualization_description=None, visualization_config_data=None,
                   visualization_component_id=None):
    """Upload preview to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset that is currently being processed
    previewfile -- the file containing the preview
    previewmetadata -- any metadata to be associated with preview, can contain a section_id
                    to indicate the section this preview should be associated with.
    preview_mimetype -- (optional) MIME type of the preview file. By default, this is obtained from the
                    file itself and this parameter can be ignored. E.g. 'application/vnd.clowder+custom+xml'
    """

    client = ClowderClient(host=host, key=key)
    preview_id = datasets.upload_preview(connector, client, datasetid, previewfile, previewmetadata, preview_mimetype,
                                         visualization_name=visualization_name,
                                         visualization_description=visualization_description,
                                         visualization_config_data=visualization_config_data,
                                         visualization_component_id=visualization_component_id)
    return preview_id


def upload_thumbnail(connector, host, key, datasetid, thumbnail):
    """Upload thumbnail to Clowder.

            Keyword arguments:
            connector -- connector information, used to get missing parameters and send status updates
            host -- the clowder host, including http and port, should end with a /
            key -- the secret key to login to clowder
            datasetid -- the dataset that the thumbnail should be associated with
            thumbnail -- the file containing the thumbnail
            """
    logger = logging.getLogger(__name__)

    client = ClowderClient(host=host, key=key)
    return datasets.upload_thumbnail(connector, client, datasetid, thumbnail)
