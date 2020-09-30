"""Clowder API

This module provides simple wrappers around the clowder Collections API
"""

import json
import logging
import requests

from pyclowder.client import ClowderClient


def create_empty(connector, host, key, collectionname, description, parentid=None, spaceid=None):
    """Create a new collection in Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    collectionname -- name of new dataset to create
    description -- description of new dataset
    parentid -- id of parent collection
    spaceid -- id of the space to add dataset to
    """

    logger = logging.getLogger(__name__)

    if parentid:
        if spaceid:
            url = '%sapi/collections/newCollectionWithParent?key=%s' % (host, key)
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname, "description": description,
                                                    "parentId": [parentid], "space": spaceid}),
                                   verify=connector.ssl_verify if connector else True)
        else:
            url = '%sapi/collections/newCollectionWithParent?key=%s' % (host, key)
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname, "description": description,
                                                    "parentId": [parentid]}),
                                   verify=connector.ssl_verify if connector else True)
    else:
        if spaceid:
            url = '%sapi/collections?key=%s' % (host, key)
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname, "description": description,
                                                    "space": spaceid}),
                                   verify=connector.ssl_verify if connector else True)
        else:
            url = '%sapi/collections?key=%s' % (host, key)
            result = requests.post(url, headers={"Content-Type": "application/json"},
                                   data=json.dumps({"name": collectionname, "description": description}),
                                   verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    collectionid = result.json()['id']
    logger.debug("collection id = [%s]", collectionid)

    return collectionid


def delete(connector, host, key, collectionid):
    url = "%sapi/collections/%s?key=%s" % (host, collectionid, key)

    result = requests.delete(url, verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return json.loads(result.text)


def get_child_collections(connector, host, key, collectionid):
    """Get list of child collections in collection by UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    collectionid -- the collection to get children of
    """

    url = "%sapi/collections/%s/getChildCollections?key=%s" % (host, collectionid, key)

    result = requests.get(url,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return json.loads(result.text)


def get_datasets(connector, host, key, collectionid):
    """Get list of datasets in collection by UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the collection to get datasets of
    """

    url = "%sapi/collections/%s/datasets?key=%s" % (host, collectionid, key)

    result = requests.get(url,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return json.loads(result.text)


# pylint: disable=too-many-arguments
def upload_preview(connector, host, key, collectionid, previewfile, previewmetadata):
    """Upload preview to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    collectionid -- the file that is currently being processed
    preview -- the file containing the preview
    previewdata: any metadata to be associated with preview,
                    this can contain a section_id to indicate the
                    section this preview should be associated with.
    """

    connector.message_process({"type": "collection", "id": collectionid}, "Uploading collection preview.")

    logger = logging.getLogger(__name__)
    headers = {'Content-Type': 'application/json'}

    # upload preview
    url = '%sapi/previews?key=%s' % (host, key)
    with open(previewfile, 'rb') as filebytes:
        result = requests.post(url, files={"File": filebytes},
                               verify=connector.ssl_verify if connector else True)
        result.raise_for_status()
    previewid = result.json()['id']
    logger.debug("preview id = [%s]", previewid)

    # associate uploaded preview with original collection
    if collectionid and not (previewmetadata and 'section_id' in previewmetadata and previewmetadata['section_id']):
        url = '%sapi/collections/%s/previews/%s?key=%s' % (host, collectionid, previewid, key)
        result = requests.post(url, headers=headers, data=json.dumps({}),
                               verify=connector.ssl_verify if connector else True)
        result.raise_for_status()

    # associate metadata with preview
    if previewmetadata is not None:
        url = '%sapi/previews/%s/metadata?key=%s' % (host, previewid, key)
        result = requests.post(url, headers=headers, data=json.dumps(previewmetadata),
                               verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    return previewid


class CollectionsApi(object):
    """
        API to manage the REST CRUD endpoints for collections
    """
    def __init__(self, client=None, host=None, key=None, username=None, password=None):
        """Set client if provided otherwise create new one"""
        if client:
            self.api_client = client
        else:
            self.client = ClowderClient(host=host, key=key, username=username, password=password)

    def create(self, name, description, parent_id, space_id):
        """Create a new collection in Clowder.

        Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        collectionname -- name of new dataset to create
        description -- description of new dataset
        parentid -- id of parent collection
        spaceid -- id of the space to add dataset to
        """

        if parent_id:
            if space_id:
                body = {
                    "name": name,
                    "description": description,
                    "parentId": [parent_id],
                    "space": space_id
                }
                result = self.client.post("/collections/newCollectionWithParents", body)
            else:
                body = {
                    "name": name,
                    "description": description,
                    "parentId": [parent_id],
                }
                result = self.client.post("/collections/newCollectionWithParent", body)
        else:
            if space_id:
                body = {
                    "name": name,
                    "description": description,
                    "space": space_id
                }
                result = self.client.post("/collections", body)
            else:
                body = {
                    "name": name,
                    "description": description,
                }
                result = self.client.post("/collections", body)
        result.raise_for_status()

        collection_id = result.json()['id']
        logging.debug("collection id = [%s]", collection_id)

        return collection_id

    def get_all_collections(self):
        """
        Get All Collections in Clowder

        :return: List of collections in Clowder
        """
        return self.client.get("/collections")
