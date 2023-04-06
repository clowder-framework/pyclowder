"""
    ClowderClient
    ~~~
    This module contains a basic client to interact with the Clowder API.
"""

import json
import logging
import os
import tempfile

import requests


class ClowderClient(object):
    """
    Client to Clowder API to store connection information.

    The `path` parameter used by many of the methods in this class call a specific path relative to the host + "api".
    For example passing in "/version" for host "https://seagrant-dev.ncsa.illinois.edu/clowder/" will call
    "https://seagrant-dev.ncsa.illinois.edu/clowder/api/version". Make sure to include the slash at the beginning of
    the fragment.
    """
    logger = logging.getLogger(__name__)

    def __init__(self, **kwargs):
        """
        Create an instance of `ClowderClient`.

        :param ClowderClient client: Optional clowderclient to copy all parameters from, any additional parameters
        will override the values in this client
        :param string host: The root host url of the specific API we are connecting to.
        :param string key: The API key used to write to the API. Set this or `username`/`password` below.
        :param string username: HTTP Basic Authentication username. Set this or `key`.
        :param string password: HTTP Basic Authentication password. Set this or `key`.
         """

        # clone operator
        if 'client' in kwargs:
            client = kwargs.get('client')
            self.host = kwargs.get('host', client.host)
            self.key = kwargs.get('key', client.key)
            self.username = kwargs.get('username', client.username)
            self.password = kwargs.get('password', client.password)
        else:
            self.host = kwargs.get('host', 'http://localhost:9000')
            self.key = kwargs.get('key', None)
            self.username = kwargs.get('username', None)
            self.password = kwargs.get('password', None)

        # make sure the host does not end with a slash
        self.host = self.host.rstrip('/')

        # warning if both key and username/password present
        if not self.key and not self.username:
            self.logger.warning("No key or username/password present.")
        if self.key and self.username and self.password:
            self.logger.info("Both key and username/password present, will use username/password for calls.")
