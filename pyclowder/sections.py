"""Clowder API

This module provides simple wrappers around the clowder Datasets API
"""

import json
import logging

import requests


def upload(connector, host, key, sectiondata):
    """Upload section to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    sectiondata -- section data to send
    """

    logger = logging.getLogger(__name__)
    headers = {'Content-Type': 'application/json'}

    # upload section
    url = '%sapi/sections?key=%s' % (host, key)
    result = requests.post(url, headers=headers, data=json.dumps(sectiondata),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    sectionid = result.json()['id']
    logger.debug("section id = [%s]", sectionid)

    return sectionid


def upload_tags(connector, host, key, sectionid, tags):
    """Upload section tag to Clowder.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    sectionid -- the section that is currently being processed
    tags -- the tags to be uploaded
    """

    connector.message_process({"type": "section", "id": sectionid}, "Uploading section tags.")

    headers = {'Content-Type': 'application/json'}
    url = '%sapi/sections/%s/tags?key=%s' % (host, sectionid, key)
    result = requests.post(url, headers=headers, data=json.dumps(tags),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()


def upload_description(connector, host, key, sectionid, description):
    """Upload description to a section.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    sectionid -- the section that is currently being processed
    description -- the description to be uploaded
    """

    connector.message_process({"type": "section", "id": sectionid},
                              "Uploading section description.")

    headers = {'Content-Type': 'application/json'}
    url = '%sapi/sections/%s/description?key=%s' % (host, sectionid, key)
    result = requests.post(url, headers=headers, data=json.dumps(description),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()
