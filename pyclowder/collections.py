"""Clowder API

This module provides simple wrappers around the clowder Collections API
"""

import logging
from client import ClowderClient


# TODO: Functions outside CollectionsApi are deprecated
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

    client = CollectionsApi(host=host, key=key)
    return client.create(collectionname, description, parentid, spaceid)

def delete(connector, host, key, collectionid):

    client = CollectionsApi(host=host, key=key)
    return client.delete(collectionid)

def get_child_collections(connector, host, key, collectionid):
    """Get list of child collections in collection by UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    collectionid -- the collection to get children of
    """

    client = CollectionsApi(host=host, key=key)
    return client.get_child_collections(collectionid)

def get_datasets(connector, host, key, collectionid):
    """Get list of datasets in collection by UUID.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the collection to get datasets of
    """

    client = CollectionsApi(host=host, key=key)
    return client.get_datasets(collectionid)
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

    client = CollectionsApi(host=host, key=key)
    return client.upload_preview(collectionid, previewfile, previewmetadata)


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


    def add_preview(self, collection_id, preview_file, preview_metadata):
        """Upload a collection preview.

        Keyword arguments:
        collection_id -- the collection that is currently being processed
        preview_file -- the file containing the preview
        preview_metadata: any metadata to be associated with preview,
                        this can contain a section_id to indicate the
                        section this preview should be associated with.
        """

        # upload preview
        prev = self.client.post_file("previews", preview_file)

        # associate uploaded preview with original collection
        if collection_id and not (preview_metadata and preview_metadata['section_id']):
            self.client.post("collections/%s/previews/%s" % (collection_id, prev['id']))

        # associate metadata with preview
        if preview_metadata is not None:
            self.client.post("previews/%s/metadata" % prev['id'], preview_metadata)

        return prev['id']


    def create(self, name, description="", parent_id=None, space_id=None):
        """Create a new collection in Clowder.

        Keyword arguments:
        name -- name of new collection to create
        description -- description of new collection
        parent_id -- id of parent collection (or list of ids)
        space_id -- id of the space to add collection to
        """

        body = {
            "name": name,
            "description": description,
        }

        if parent_id:
            if isinstance(parent_id, list):
                body["parentId"] = parent_id
            else:
                body["parentId"] = [parent_id]
            if space_id:
                body["space"] = space_id
            result = self.client.post("collections/newCollectionWithParent", body)
        else:
            if space_id:
                body["space"] = space_id
            result = self.client.post("collections", body)

        return result['id']


    def delete(self, collection_id):
        """Delete a collection from Clowder.

        Keyword arguments:
        collection_id -- id of collection to delete
        """

        return self.client.delete("collections/%s" % collection_id)


    def get_all_collections(self):
        """Get all Collections in Clowder."""

        return self.client.get("/collections")


    def get_child_collections(self, collection_id):
        """List child collections of a collection.

        Keyword arguments:
        collection_id -- id of collection to get children of
        """

        return self.client.get("collections/%s/getChildCollections" % collection_id)


    def get_datasets(self, collection_id):
        """Get list of datasets in a collection.

        Keyword arguments:
        collection_id -- id of collection to get datasets of
        """

        return self.client.get("collections/%s/datasets" % collection_id)


