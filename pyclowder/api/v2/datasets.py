"""Clowder API

This module provides simple wrappers around the clowder Datasets API
"""

import json
import logging
import os
import tempfile

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

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
               "X-API-KEY": client.key}
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
    headers = {"X-API-KEY": client.key}
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

    headers = {"X-API-KEY": client.key}
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
    headers = {"X-API-KEY": client.key}

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
    headers = {"X-API-KEY": client.key}

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
    headers = {"X-API-KEY": client.key}

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
    headers = {"X-API-KEY": client.key}

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
                "X-API-KEY": client.key}

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
               "X-API-KEY": client.key}
    connector.message_process({"type": "dataset", "id": datasetid}, "Uploading dataset metadata.")


    url = '%s/api/v2/datasets/%s/metadata' % (client.host, datasetid)
    result = requests.post(url, headers=headers, data=json.dumps(metadata),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

def upload_preview(connector, client, datasetid, previewfile, previewmetadata=None, preview_mimetype=None,
                   visualization_name=None, visualization_description=None, visualization_config_data=None,
                   visualization_component_id=None):
    """Upload visualization to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datsetid -- the dataset that is currently being processed
    previewfile -- the file containing the preview
    previewmetadata -- any metadata to be associated with preview, can contain a section_id
                    to indicate the section this preview should be associated with.
    preview_mimetype -- (optional) MIME type of the preview file. By default, this is obtained from the
                    file itself and this parameter can be ignored. E.g. 'application/vnd.clowder+custom+xml'
    """

    connector.message_process({"type": "dataset", "id": datasetid}, "Uploading dataset preview.")
    logger = logging.getLogger(__name__)

    preview_id = None
    visualization_config_id = None

    if os.path.exists(previewfile):

        # upload visualization URL
        visualization_config_url = '%s/api/v2/visualizations/config' % client.host

        if visualization_config_data is None:
            visualization_config_data = dict()

        payload = json.dumps({
            "resource": {
                "collection": "datasets",
                "resource_id": datasetid
            },
            "client": client.host,
            "parameters": visualization_config_data,
            "visualization_mimetype": preview_mimetype,
            "visualization_component_id": visualization_component_id
        })

        headers = {
            "X-API-KEY": client.key,
            "Content-Type": "application/json"
        }

        response = connector.post(visualization_config_url, headers=headers, data=payload,
                                  verify=connector.ssl_verify if connector else True)

        if response.status_code == 200:
            visualization_config_id = response.json()['id']
            logger.debug("Uploaded visualization config ID = [%s]", visualization_config_id)
        else:
            logger.error("An error occurred when uploading visualization config to dataset: " + datasetid)

        if visualization_config_id is not None:

            # upload visualization URL
            visualization_url = '%s/api/v2/visualizations?name=%s&description=%s&config=%s' % (
                client.host, visualization_name, visualization_description, visualization_config_id)

            filename = os.path.basename(previewfile)
            if preview_mimetype is not None:
                multipart_encoder_object = MultipartEncoder(
                    fields={'file': (filename, open(previewfile, 'rb'), preview_mimetype)})
            else:
                multipart_encoder_object = MultipartEncoder(fields={'file': (filename, open(previewfile, 'rb'))})
            headers = {'X-API-KEY': client.key,
                       'Content-Type': multipart_encoder_object.content_type}
            response = connector.post(visualization_url, data=multipart_encoder_object, headers=headers,
                                      verify=connector.ssl_verify if connector else True)

            if response.status_code == 200:
                preview_id = response.json()['id']
                logger.debug("Uploaded visualization data ID = [%s]", preview_id)
            else:
                logger.error("An error occurred when uploading the visualization data to dataset: " + datasetid)
    else:
        logger.error("Visualization data file not found")

    return preview_id

def upload_thumbnail(connector, client, datasetid, thumbnail):
    """Upload thumbnail to Clowder.

            Keyword arguments:
            connector -- connector information, used to get missing parameters and send status updates
            host -- the clowder host, including http and port, should end with a /
            key -- the secret key to login to clowder
            datasetid -- the dataset that the thumbnail should be associated with
            thumbnail -- the file containing the thumbnail
            """

    logger = logging.getLogger(__name__)

    connector.message_process({"type": "dataset", "id": datasetid}, "Uploading thumbnail to dataset.")

    url = '%s/api/v2/thumbnails' % (client.host)

    if os.path.exists(thumbnail):
        file_data = {"file": open(thumbnail, 'rb')}
        headers = {"X-API-KEY": client.key}
        result = connector.post(url, files=file_data, headers=headers,
                                verify=connector.ssl_verify if connector else True)

        thumbnailid = result.json()['id']
        logger.debug("uploaded thumbnail id = [%s]", thumbnailid)

        connector.message_process({"type": "dataset", "id": datasetid}, "Uploading thumbnail to dataset.")
        headers = {'Content-Type': 'application/json',
                   'X-API-KEY': client.key}
        url = '%s/api/v2/datasets/%s/thumbnail/%s' % (client.host, datasetid, thumbnailid)
        result = connector.patch(url, headers=headers,
                                 verify=connector.ssl_verify if connector else True)
        return result.json()["thumbnail_id"]
    else:
        logger.error("unable to upload thumbnail %s to dataset %s", thumbnail, datasetid)
