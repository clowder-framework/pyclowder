"""Clowder API

This module provides simple wrappers around the clowder Files API
"""

import json
import logging
import os
import tempfile

import requests
from urllib3.filepost import encode_multipart_formdata

from pyclowder.utils import StatusMessage

# Some sources of urllib3 support warning suppression, but not all
try:
    from urllib3 import disable_warnings
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except:
    pass


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

    connector.status_update(StatusMessage.processing, {"type": "file", "id": fileid}, "Downloading file.")

    # TODO: intermediateid doesn't really seem to be used here, can we remove entirely?
    if not intermediatefileid:
        intermediatefileid = fileid

    url = '%sapi/files/%s?key=%s' % (host, intermediatefileid, key)
    result = requests.get(url, stream=True, verify=connector.ssl_verify)
    result.raise_for_status()

    (inputfile, inputfilename) = tempfile.mkstemp(suffix=ext)
    with os.fdopen(inputfile, "w") as outputfile:
        for chunk in result.iter_content(chunk_size=10*1024):
            outputfile.write(chunk)

    return inputfilename


def download_info(connector, host, key, fileid):
    """Download file summary metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file to fetch metadata of
    """

    url = '%sapi/files/%s/metadata?key=%s' % (host, fileid, key)

    # fetch data
    result = requests.get(url, stream=True,
                          verify=connector.ssl_verify)
    result.raise_for_status()

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

    filterstring = "" if extractor is None else "&extractor=%s" % extractor
    url = '%sapi/files/%s/metadata.jsonld?key=%s%s' % (host, fileid, key, filterstring)

    # fetch data
    result = requests.get(url, stream=True,
                          verify=connector.ssl_verify)
    result.raise_for_status()

    return result.json()


def upload_metadata(connector, host, key, fileid, metadata):
    """Upload file JSON-LD metadata to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    metadata -- the metadata to be uploaded
    """

    connector.status_update(StatusMessage.processing, {"type": "file", "id": fileid}, "Uploading file metadata.")

    headers = {'Content-Type': 'application/json'}
    url = '%sapi/files/%s/metadata.jsonld?key=%s' % (host, fileid, key)
    result = requests.post(url, headers=headers, data=json.dumps(metadata),
                           verify=connector.ssl_verify)
    result.raise_for_status()


# pylint: disable=too-many-arguments
def upload_preview(connector, host, key, fileid, previewfile, previewmetadata):
    """Upload preview to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    previewfile -- the file containing the preview
    previewmetadata -- any metadata to be associated with preview, can contain a section_id
                    to indicate the section this preview should be associated with.
    """

    connector.status_update(StatusMessage.processing, {"type": "file", "id": fileid}, "Uploading file preview.")

    logger = logging.getLogger(__name__)
    headers = {'Content-Type': 'application/json'}

    # upload preview
    url = '%sapi/previews?key=%s' % (host, key)
    with open(previewfile, 'rb') as filebytes:
        result = requests.post(url, files={"File": filebytes},
                               verify=connector.ssl_verify)
        result.raise_for_status()
    previewid = result.json()['id']
    logger.debug("preview id = [%s]", previewid)

    # associate uploaded preview with orginal file
    if fileid and not (previewmetadata and previewmetadata['section_id']):
        url = '%sapi/files/%s/previews/%s?key=%s' % (host, fileid, previewid, key)
        result = requests.post(url, headers=headers, data=json.dumps({}),
                               verify=connector.ssl_verify)
        result.raise_for_status()

    # associate metadata with preview
    if previewmetadata is not None:
        url = '%sapi/previews/%s/metadata?key=%s' % (host, previewid, key)
        result = requests.post(url, headers=headers, data=json.dumps(previewmetadata),
                               verify=connector.ssl_verify)
        result.raise_for_status()

    return previewid


def upload_tags(connector, host, key, fileid, tags):
    """Upload file tag to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that is currently being processed
    tags -- the tags to be uploaded
    """

    connector.status_update(StatusMessage.processing, {"type": "file", "id": fileid}, "Uploading file tags.")

    headers = {'Content-Type': 'application/json'}
    url = '%sapi/files/%s/tags?key=%s' % (host, fileid, key)
    result = requests.post(url, headers=headers, data=json.dumps(tags),
                           verify=connector.ssl_verify)
    result.raise_for_status()


def upload_thumbnail(connector, host, key, fileid, thumbnail):
    """Upload thumbnail to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that the thumbnail should be associated with
    thumbnail -- the file containing the thumbnail
    """

    logger = logging.getLogger(__name__)
    url = host + 'api/fileThumbnail?key=' + key

    # upload preview
    with open(thumbnail, 'rb') as inputfile:
        result = requests.post(url, files={"File": inputfile},
                               verify=connector.ssl_verify)
        result.raise_for_status()
    thumbnailid = result.json()['id']
    logger.debug("thumbnail id = [%s]", thumbnailid)

    # associate uploaded preview with orginal file/dataset
    if fileid:
        headers = {'Content-Type': 'application/json'}
        url = host + 'api/files/' + fileid + '/thumbnails/' + thumbnailid + '?key=' + key
        result = requests.post(url, headers=headers, data=json.dumps({}),
                               verify=connector.ssl_verify)
        result.raise_for_status()

    return thumbnailid


def upload_to_dataset(connector, host, key, datasetid, filepath):
    """Upload file to existing Clowder dataset.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    datasetid -- the dataset that the file should be associated with
    filepath -- path to file
    """

    logger = logging.getLogger(__name__)

    for source_path in connector.mounted_paths:
        if filepath.startswith(connector.mounted_paths[source_path]):
            return _upload_to_dataset_local(connector, host, key, datasetid, filepath)

    url = '%sapi/uploadToDataset/%s?key=%s' % (host, datasetid, key)

    if os.path.exists(filepath):
        result = requests.post(url, files={"File": open(filepath, 'rb')},
                               verify=connector.ssl_verify)
        result.raise_for_status()

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

    logger = logging.getLogger(__name__)
    url = '%sapi/uploadToDataset/%s?key=%s' % (host, datasetid, key)

    if os.path.exists(filepath):
        # Replace local path with remote path before uploading
        for source_path in connector.mounted_paths:
            if filepath.startswith(connector.mounted_paths[source_path]):
                filepath = filepath.replace(connector.mounted_paths[source_path],
                                            source_path)
                break

        (content, header) = encode_multipart_formdata([
            ("file", '{"path":"%s"}' % filepath)
        ])
        result = requests.post(url, data=content, headers={'Content-Type': header},
                               verify=connector.ssl_verify)
        result.raise_for_status()

        uploadedfileid = result.json()['id']
        logger.debug("uploaded file id = [%s]", uploadedfileid)

        return uploadedfileid
    else:
        logger.error("unable to upload local file %s (not found)", filepath)
