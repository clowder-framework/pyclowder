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

def create_empty_dataset(host, clowder_user, clowder_pass, datasetname, description, parentid=None, spaceid=None):
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

    url = '%sapi/datasets/createempty' % host

    if parentid:
        if spaceid:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "collection": [parentid], "space": [spaceid]}),
                                   auth=(clowder_user, clowder_pass))
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "collection": [parentid]}),
                                   auth=(clowder_user, clowder_pass))
    else:
        if spaceid:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description,
                                                    "space": [spaceid]}),
                                   auth=(clowder_user, clowder_pass))
        else:
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": datasetname, "description": description}),
                                   auth=(clowder_user, clowder_pass))

    result.raise_for_status()

    datasetid = result.json()['id']
    logger.debug("dataset id = [%s]", datasetid)

    return datasetid

def get_datasetid_by_name(host, secret_key, dsname):
    """Looks up the ID of a dataset by nanme
    Args:
        host(str): the URI of the host making the connection
        secret_key(str): used with the host API
        dsname(str): the dataset name to look up
    Return:
        Returns the ID of the dataset if it's found. Returns None if the dataset
        isn't found
    """
    url = "%sapi/datasets?key=%s&title=%s&exact=true" % (host, secret_key, dsname)

    try:
        result = requests.get(url)
        result.raise_for_status()

        md = result.json()
        md_len = len(md)
    except Exception as ex:     # pylint: disable=broad-except
        md = None
        md_len = 0
        logging.debug(ex.message)

    if md and md_len > 0 and "id" in md[0]:
        return md[0]["id"]

    return None

def get_dataset_folderid_by_name(host, secret_key, dataset_id, folder_name):
    folder_url = f"{host}/api/datasets/{dataset_id}/folders?key={secret_key}"
    dataset_folders = requests.get(folder_url)
    result = dataset_folders.json()
    for folder in result:
        current_folder_name = folder['name'].lstrip('/')
        if current_folder_name == folder_name:
            return folder['id']
    return None

def add_dataset_to_collection(host, secret_key, dataset_id, collection_id):
    # Didn't find space, so we must associate it now
    url = "%sapi/collections/%s/datasets/%s?key=%s" % (host, collection_id, dataset_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()

def add_dataset_to_space(host, secret_key, dataset_id, space_id):
    # Didn't find space, so we must associate it now
    url = "%sapi/spaces/%s/addDatasetToSpace/%s?key=%s" % (host, space_id, dataset_id, secret_key)
    result = requests.post(url)
    result.raise_for_status()

def get_dataset_or_create(host, secret_key, clowder_user, clowder_pass, dsname, parent_colln=None, parent_space=None):
    # Fetch dataset from Clowder by name, or create it if not found
    ds_id = get_datasetid_by_name(host, secret_key, dsname)

    if not ds_id:
        return create_empty_dataset(host, clowder_user, clowder_pass, dsname, "",
                                    parent_colln, parent_space)
    else:
        if parent_colln:
            add_dataset_to_collection(host, secret_key, ds_id, parent_colln)
        if parent_space:
            add_dataset_to_space(host, secret_key, ds_id, parent_space)
        return ds_id

def get_dataset_folder_or_create(host, secret_key, clowder_user, clowder_pass, dataset_id, folder_name):
    folder_id = get_dataset_folderid_by_name(host, secret_key, dataset_id, folder_name)
    if folder_id is None:
        folder_post_url = f"{host}/api/datasets/{dataset_id}/newFolder?key={secret_key}"
        payload = json.dumps({'name': folder_name,
                              'parentId': dataset_id,
                              'parentType': "dataset"})
        r = requests.post(folder_post_url, data=payload)
        r.raise_for_status()
        folder_id = r.json()["id"]
    return folder_id
clowder_version = int(os.getenv('CLOWDER_VERSION', '1'))

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
    if clowder_version == 2:
        datasetid = v2datasets.create_empty(connector, client, datasetname, description, parentid, spaceid)
    else:
        datasetid = v1datasets.create_empty(connector, client, datasetname, description, parentid, spaceid)
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
    if clowder_version == 2:
        result = v2datasets.delete(connector, client, datasetid)
    else:
        result = v2datasets.delete(connector, client, datasetid)
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
    if clowder_version == 2:
        zipfile = v2datasets.download(connector, client, datasetid)
    else:
        zipfile = v1datasets.download(connector, client, datasetid)
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
    if clowder_version == 2:
        result_json = v2datasets.download_metadata(connector, client, datasetid, extractor)
        return result_json
    else:
        result_json = v1datasets.download_metadata(connector, client, datasetid, extractor)
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
    if clowder_version == 2:
        info = v2datasets.get_info(connector, client, datasetid)
    else:
        info = v1datasets.get_info(connector, client, datasetid)
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
    if clowder_version == 2:
        file_list = v2datasets.get_file_list(connector, client, datasetid)
    else:
        file_list = v1datasets.get_file_list(connector, client, datasetid)
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
    if clowder_version == 2:
        v2datasets.remove_metadata(connector, client, datasetid, extractor)
    else:
        v1datasets.remove_metadata(connector, client, datasetid, extractor)


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
    if clowder_version == 2:
        result_status_code = v2datasets.submit_extraction(connector, client, datasetid, extractorname)
    else:
        result_status_code = v1datasets.submit_extraction(connector, client, datasetid, extractorname)


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
    if clowder_version == 2:
        v2datasets.upload_metadata(connector, client, datasetid, metadata)
    else:
        v1datasets.upload_metadata(connector, client, datasetid, metadata)
