"""Clowder API

This module provides simple wrappers around the clowder Files API
"""

import json
import logging
import os

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder

from pyclowder.client import ClowderClient
from pyclowder.collections import get_datasets, get_child_collections
from pyclowder.datasets import get_file_list

clowder_version = int(os.getenv('CLOWDER_VERSION', '1'))
# Import files API methods based on Clowder version
if clowder_version == 2:
    import pyclowder.api.v2.files as files
else:
    import pyclowder.api.v1.files as files

# Some sources of urllib3 support warning suppression, but not all
try:
    from urllib3 import disable_warnings
    from requests.packages.urllib3.exceptions import InsecureRequestWarning

    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except:
    pass


def get_download_url(connector, host, key, fileid, intermediatefileid=None, ext=""):
    client = ClowderClient(host=host, key=key)
    download_url = files.get_download_url(connector, client, fileid, intermediatefileid, ext)
    return download_url


# pylint: disable=too-many-arguments
def download(connector, host, key, fileid, intermediatefileid=None, ext="", tracking=True):
    """Download file to be processed from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    intermediatefileid -- either same as fileid, or the intermediate file to be used
    ext -- the file extension, the downloaded file will end with this extension
    tracking -- should the download action be tracked
    """
    client = ClowderClient(host=host, key=key)
    inputfilename = files.download(connector, client, fileid, intermediatefileid, ext)
    return inputfilename


def download_info(connector, host, key, fileid):
    """Download file summary metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file to fetch metadata of
    """
    client = ClowderClient(host=host, key=key)
    result = files.download_info(connector, client, fileid)
    return result.json()


def download_summary(connector, host, key, fileid):
    """Download file summary metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file to fetch metadata of
    """
    client = ClowderClient(host=host, key=key)
    result = files.download_summary(connector, client, fileid)
    return result.json()


def download_metadata(connector, host, key, fileid, extractor=None):
    """Download file JSON-LD metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file to fetch metadata of
    extractor -- extractor name to filter results (if only one extractor's metadata is desired)
    """
    client = ClowderClient(host=host, key=key)
    result = files.download_metadata(connector, client, fileid, extractor)
    return result.json()


def delete(connector, host, key, fileid):
    """Delete file from Clowder.

        Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        host -- the clowder host, including http and port, should end with a /
        key -- the secret key to login to clowder
        fileid -- the file to delete
        """
    client = ClowderClient(host=host, key=key)
    result = files.delete(connector, client, fileid)
    result.raise_for_status()

    return json.loads(result.text)

def submit_extraction(connector, host, key, fileid, extractorname):
    """Submit file for extraction by given extractor.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file UUID to submit
    extractorname -- registered name of extractor to trigger
    """
    client = ClowderClient(host=host, key=key)
    result = files.submit_extraction(connector, client, fileid, extractorname)
    return result.json()


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
    client = ClowderClient(host=host, key=key)
    filelist = get_file_list(connector, client, datasetid)

    for f in filelist:
        # Only submit files that end with given extension, if specified
        if ext and not f['filename'].endswith(ext):
            continue

        submit_extraction(connector, host, key, f['id'], extractorname)


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

    dslist = get_datasets(connector, host, key, collectionid)

    for ds in dslist:
        submit_extractions_by_dataset(connector, host, key, ds['id'], extractorname, ext)

    if recursive:
        childcolls = get_child_collections(connector, host, key, collectionid)
        for coll in childcolls:
            submit_extractions_by_collection(connector, host, key, coll['id'], extractorname, ext, recursive)


def upload_metadata(connector, host, key, fileid, metadata):
    """Upload file JSON-LD metadata to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    metadata -- the metadata to be uploaded
    """
    client = ClowderClient(host=host, key=key)
    files.upload_metadata(connector, client, fileid, metadata)


# pylint: disable=too-many-arguments
def upload_preview(connector, host, key, fileid, previewfile, previewmetadata=None, preview_mimetype=None,
                   visualization_name=None, visualization_description=None, visualization_config_data=None,
                   visualization_component_id=None):
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

    client = ClowderClient(host=host, key=key)
    preview_id = files.upload_preview(connector, client, fileid, previewfile, previewmetadata, preview_mimetype,
                                      visualization_name=visualization_name,
                                      visualization_description=visualization_description,
                                      visualization_config_data=visualization_config_data,
                                      visualization_component_id=visualization_component_id)
    return preview_id


def upload_tags(connector, host, key, fileid, tags):
    """Upload file tag to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    tags -- the tags to be uploaded
    """
    client = ClowderClient(host=host, key=key)
    connector.message_process({"type": "file", "id": fileid}, "Uploading file tags.")

    headers = {'Content-Type': 'application/json'}
    url = '%sapi/files/%s/tags?key=%s' % (client.host, fileid, client.key)
    result = connector.post(url, headers=headers, data=json.dumps(tags),
                            verify=connector.ssl_verify if connector else True)


def upload_thumbnail(connector, host, key, fileid, thumbnail):
    """Upload thumbnail to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that the thumbnail should be associated with
    thumbnail -- the file containing the thumbnail
    """
    
    client = ClowderClient(host=host, key=key)
    thumbnail_id = files.upload_thumbnail(connector, client, fileid, thumbnail)
    return thumbnail_id


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
    client = ClowderClient(host=host, key=key)
    if clowder_version == 2:
        return files.upload_to_dataset(connector, client, datasetid, filepath, check_duplicate)
    else:
        logger = logging.getLogger(__name__)

        if check_duplicate:
            ds_files = get_file_list(connector, client, datasetid)
            for f in ds_files:
                if f['filename'] == os.path.basename(filepath):
                    logger.debug("found %s in dataset %s; not re-uploading" % (f['filename'], datasetid))
                    return None

        for source_path in connector.mounted_paths:
            if filepath.startswith(connector.mounted_paths[source_path]):
                return _upload_to_dataset_local(connector, client.host, client.key, datasetid, filepath)

        url = '%sapi/uploadToDataset/%s?key=%s' % (client.host, datasetid, client.key)

        if os.path.exists(filepath):
            filename = os.path.basename(filepath)
            m = MultipartEncoder(
                fields={'file': (filename, open(filepath, 'rb'))}
            )
            result = connector.post(url, data=m, headers={'Content-Type': m.content_type},
                                    verify=connector.ssl_verify if connector else True)

            uploadedfileid = result.json()['id']
            logger.debug("uploaded file id = [%s]", uploadedfileid)

            return uploadedfileid
        else:
            logger.error("unable to upload file %s (not found)", filepath)


def _upload_to_dataset_local(connector, host, key, datasetid, filepath):
    """Upload file POINTER to existing Clowder dataset. Does not copy actual file bytes.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset that the file should be associated with
    filepath -- path to file
    """
    client = ClowderClient(host=host, key=key)
    uploadedfileid = files._upload_to_dataset_local(connector, client, datasetid, filepath)
    return uploadedfileid
