"""Clowder API

This module provides simple wrappers around the clowder Files API
"""

import json
import logging
import os
import tempfile

import requests
from requests_toolbelt.multipart.encoder import MultipartEncoder
from urllib3.filepost import encode_multipart_formdata
from pyclowder.client import ClowderClient
from pyclowder.datasets import get_file_list
from pyclowder.collections import get_datasets, get_child_collections

# Some sources of urllib3 support warning suppression, but not all
try:
    from urllib3 import disable_warnings
    from requests.packages.urllib3.exceptions import InsecureRequestWarning
    requests.packages.urllib3.disable_warnings(InsecureRequestWarning)
except:
    pass


def get_download_url(connector, client, fileid, intermediatefileid=None, ext=""):
    """Download file to be processed from Clowder.

        Keyword arguments:
        connector -- connector information, used to get missing parameters and send status updates
        client -- ClowderClient containing authentication credentials
        fileid -- the file that is currently being processed
        intermediatefileid -- either same as fileid, or the intermediate file to be used
        ext -- the file extension, the downloaded file will end with this extension
        """

    connector.message_process({"type": "file", "id": fileid}, "Getting download url for file.")

    # TODO: intermediateid doesn't really seem to be used here, can we remove entirely?
    if not intermediatefileid:
        intermediatefileid = fileid

    url = '%s/api/v2/files/%s' % (client.host, intermediatefileid)
    return url

# pylint: disable=too-many-arguments
def download(connector, client, fileid, intermediatefileid=None, ext=""):
    """Download file to be processed from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    fileid -- the file that is currently being processed
    intermediatefileid -- either same as fileid, or the intermediate file to be used
    ext -- the file extension, the downloaded file will end with this extension
    """

    connector.message_process({"type": "file", "id": fileid}, "Downloading file.")



    # TODO: intermediateid doesn't really seem to be used here, can we remove entirely?
    if not intermediatefileid:
        intermediatefileid = fileid

    url = '%s/api/v2/files/%s' % (client.host, intermediatefileid)
    headers = {"Authorization": "Bearer " + client.key}
    result = connector.get(url, stream=True, verify=connector.ssl_verify if connector else True, headers=headers)

    (inputfile, inputfilename) = tempfile.mkstemp(suffix=ext)

    try:
        with os.fdopen(inputfile, "wb") as outputfile:
            for chunk in result.iter_content(chunk_size=10 * 1024):
                outputfile.write(chunk)
        return inputfilename
    except Exception:
        os.remove(inputfilename)
        raise


def download_info(connector, client, fileid):
    """Download file summary metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    fileid -- the file to fetch metadata of
    """

    url = '%s/api/v2/files/%s/metadata' % (client.host, fileid)
    headers = {"Authorization": "Bearer " + client.key}
    # fetch data
    result = connector.get(url, stream=True, verify=connector.ssl_verify if connector else True, headers=headers)

    return result


def download_metadata(connector,client, fileid, extractor=None):
    """Download file JSON-LD metadata from Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    fileid -- the file to fetch metadata of
    extractor -- extractor name to filter results (if only one extractor's metadata is desired)
    """

    filterstring = "" if extractor is None else "?extractor=%s" % extractor
    url = '%s/api/v2/files/%s/metadata?%s' % (client.host, fileid, filterstring)
    headers = {"Authorization": "Bearer " + client.key}

    # fetch data
    result = connector.get(url, stream=True, verify=connector.ssl_verify if connector else True, headers=headers)

    return result


def submit_extraction(connector, client, fileid, extractorname):
    """Submit file for extraction by given extractor.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    fileid -- the file UUID to submit
    extractorname -- registered name of extractor to trigger
    """

    url = "%s/api/v2/files/%s/extractions?key=%s" % (client.host, fileid, client.key)
    result = connector.post(url,
                            headers={'Content-Type': 'application/json',
                                     "Authorization": "Bearer " + client.key},
                            data=json.dumps({"extractor": extractorname}),
                            verify=connector.ssl_verify if connector else True)

    return result


def upload_metadata(connector, client, fileid, metadata):
    """Upload file JSON-LD metadata to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    fileid -- the file that is currently being processed
    metadata -- the metadata to be uploaded
    """

    connector.message_process({"type": "file", "id": fileid}, "Uploading file metadata.")
    headers = {'Content-Type': 'application/json',
               'Authorization':'Bearer ' + client.key}
    print(metadata)
    as_json = json.dumps(metadata)
    url = '%s/api/v2/files/%s/metadata' % (client.host, fileid)
    result = connector.post(url, headers=headers, data=json.dumps(metadata),
                            verify=connector.ssl_verify if connector else True)



# TODO not implemented in v2
# pylint: disable=too-many-arguments
def upload_preview(connector, client, fileid, previewfile, previewmetadata=None, preview_mimetype=None):
    """Upload preview to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    fileid -- the file that is currently being processed
    previewfile -- the file containing the preview
    previewmetadata -- any metadata to be associated with preview, can contain a section_id
                    to indicate the section this preview should be associated with.
    preview_mimetype -- (optional) MIME type of the preview file. By default, this is obtained from the
                    file itself and this parameter can be ignored. E.g. 'application/vnd.clowder+custom+xml'
    """

    connector.message_process({"type": "file", "id": fileid}, "Uploading file preview.")

    logger = logging.getLogger(__name__)
    headers = {'Content-Type': 'application/json'}

    # upload preview
    url = '%s/api/previews?key=%s' % (client.host, client.key)
    with open(previewfile, 'rb') as filebytes:
        # If a custom preview file MIME type is provided, use it to generate the preview file object.
        if preview_mimetype is not None:
            result = connector.post(url, files={"File": (os.path.basename(previewfile), filebytes, preview_mimetype)},
                                    verify=connector.ssl_verify if connector else True)
        else:
            result = connector.post(url, files={"File": filebytes}, verify=connector.ssl_verify if connector else True)

    previewid = result.json()['id']
    logger.debug("preview id = [%s]", previewid)

    # associate uploaded preview with orginal file
    if fileid and not (previewmetadata and 'section_id' in previewmetadata and previewmetadata['section_id']):
        url = '%s/api/files/%s/previews/%s?key=%s' % (client.host, fileid, previewid, client.key)
        result = connector.post(url, headers=headers, data=json.dumps({}),
                                verify=connector.ssl_verify if connector else True)

    # associate metadata with preview
    if previewmetadata is not None:
        url = '%s/api/previews/%s/metadata?key=%s' % (client.host, previewid, client.key)
        result = connector.post(url, headers=headers, data=json.dumps(previewmetadata),
                                verify=connector.ssl_verify if connector else True)

    return previewid

# TODO not implemented in v2
def upload_tags(connector, client, fileid, tags):
    """Upload file tag to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    fileid -- the file that is currently being processed
    tags -- the tags to be uploaded
    """

    connector.message_process({"type": "file", "id": fileid}, "Uploading file tags.")

    headers = {'Content-Type': 'application/json'}
    url = '%s/api/files/%s/tags?key=%s' % (client.host, fileid, client.key)
    result = connector.post(url, headers=headers, data=json.dumps(tags),
                            verify=connector.ssl_verify if connector else True)

# TODO not implemented in v2
def upload_thumbnail(connector, client, fileid, thumbnail):
    """Upload thumbnail to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    fileid -- the file that the thumbnail should be associated with
    thumbnail -- the file containing the thumbnail
    """

    logger = logging.getLogger(__name__)
    url = client.host + 'api/fileThumbnail?key=' + client.key

    # upload preview
    with open(thumbnail, 'rb') as inputfile:
        result = connector.post(url, files={"File": inputfile}, verify=connector.ssl_verify if connector else True)
    thumbnailid = result.json()['id']
    logger.debug("thumbnail id = [%s]", thumbnailid)

    # associate uploaded preview with orginal file/dataset
    if fileid:
        headers = {'Content-Type': 'application/json'}
        url = client.host + 'api/files/' + fileid + '/thumbnails/' + thumbnailid + '?key=' + client.key
        connector.post(url, headers=headers, data=json.dumps({}), verify=connector.ssl_verify if connector else True)

    return thumbnailid


def upload_to_dataset(connector, client, datasetid, filepath, check_duplicate=False):
    """Upload file to existing Clowder dataset.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the dataset that the file should be associated with
    filepath -- path to file
    check_duplicate -- check if filename already exists in dataset and skip upload if so
    """

    logger = logging.getLogger(__name__)

    if check_duplicate:
        ds_files = get_file_list(connector, client.host, client.key, datasetid)
        for f in ds_files:
            if f['name'] == os.path.basename(filepath):
                logger.debug("found %s in dataset %s; not re-uploading" % (f['name'], datasetid))
                return None

    for source_path in connector.mounted_paths:
        if filepath.startswith(connector.mounted_paths[source_path]):
            return _upload_to_dataset_local(connector, client, datasetid, filepath)

    url = '%s/api/v2/datasets/%s/files' % (client.host, datasetid)

    if os.path.exists(filepath):
        filename = os.path.basename(filepath)
        m = MultipartEncoder(
            fields={'file': (filename, open(filepath, 'rb'))}
        )
        headers = {"Authorization": "Bearer " + client.key,
                    'Content-Type': m.content_type}
        result = connector.post(url, data=m, headers=headers,
                                verify=connector.ssl_verify if connector else True)

        uploadedfileid = result.json()['id']
        logger.debug("uploaded file id = [%s]", uploadedfileid)

        return uploadedfileid
    else:
        logger.error("unable to upload file %s (not found)", filepath)


def _upload_to_dataset_local(connector, client, datasetid, filepath):
    """Upload file POINTER to existing Clowder dataset. Does not copy actual file bytes.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    client -- ClowderClient containing authentication credentials
    datasetid -- the dataset that the file should be associated with
    filepath -- path to file
    """
    logger = logging.getLogger(__name__)
    url = '%s/api/v2/datatsets/%s/files' % (client.host, datasetid)

    if os.path.exists(filepath):
        # Replace local path with remote path before uploading
        for source_path in connector.mounted_paths:
            if filepath.startswith(connector.mounted_paths[source_path]):
                filepath = filepath.replace(connector.mounted_paths[source_path],
                                            source_path)
                break

        filename = os.path.basename(filepath)
        m = MultipartEncoder(
            fields={'file': (filename, open(filepath, 'rb'))}
        )
        headers = {"Authorization": "Bearer " + client.key,
                    'Content-Type': m.content_type}
        result = connector.post(url, data=m, headers=headers,
                                verify=connector.ssl_verify if connector else True)

        uploadedfileid = result.json()['id']
        logger.debug("uploaded file id = [%s]", uploadedfileid)

        return uploadedfileid
    else:
        logger.error("unable to upload local file %s (not found)", filepath)
