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
        :param string host: The root host url of the specific geostreaming API we are connecting to.
        :param string key: The API key used to write to the API. Set this or `username`/`password` below.
        :param string username: HTTP Basic Authentication username. Set this or `key`.
        :param string password: HTTP Basic Authentication password. Set this or `key`.
        :param int retries: Number of times to retry before giving up.
        :param float timeout: Number of seconds to try to connect, and wait between retries.
        :param boolean ssl: Should ssl certificates be validated, default is true
         """

        # clone operator
        if 'client' in kwargs:
            client = kwargs.get('client')
            self.host = kwargs.get('host', client.host)
            self.key = kwargs.get('key', client.key)
            self.username = kwargs.get('username', client.username)
            self.password = kwargs.get('password', client.password)
            self.retries = kwargs.get('retries', client.retries)
            self.timeout = kwargs.get('timeout', client.timeout)
            self.ssl = kwargs.get('ssl', client.ssl)
        else:
            self.host = kwargs.get('host', 'http://localhost:9000')
            self.key = kwargs.get('key', None)
            self.username = kwargs.get('username', None)
            self.password = kwargs.get('password', None)
            self.retries = kwargs.get('retries', 0)
            self.timeout = kwargs.get('timeout', 5)
            self.ssl = kwargs.get('ssl', True)

        # make sure the host does not end with a slash
        self.host = self.host.rstrip('/')

        # warning if both key and username/password present
        if not self.key and not self.username:
            self.logger.warning("No key or username/password present.")
        if self.key and self.username and self.password:
            self.logger.info("Both key and username/password present, will use username/password for calls.")

    def get(self, path, params=None, headers=None):
        """
        Call HTTP GET against `path`. This version returns an object parsed from the response.

        :param string path: Endpoint path relative to Clowder api.
        :param dict params: Additional parameters to pass to clowder.
        :param dict headers: Additional headers to pass to clowder, if not set content-type will be set.
        :return: the json-encoded content of a response.
        :raises: `requests.HTTPError`
        """
        attempt = 0
        url = '%s/api/%s' % (self.host, path.lstrip('/'))
        if params is None:
            params = dict()
        if headers is None:
            headers = {'content-type': 'application/json'}
        if self.username and self.password:
            auth = (self.username, self.password)
            params['key'] = None
        elif self.key:
            auth = None
            params['key'] = self.key
        else:
            auth = None
        while True:
            try:
                response = requests.get(url, headers=headers, params=params,
                                        auth=auth, timeout=self.timeout, verify=self.ssl)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as e:
                attempt += 1
                if attempt > self.retries:
                    self.logger.exception("Error calling GET url %s: %s" % (url, str(e)))
                    raise e
                else:
                    self.logger.debug("Error calling GET url %s: %s" % (url, str(e)))

    def post(self, path, content, params=None, headers=None):
        """
        Call HTTP POST against `path` with `content` in body.

        :param path: Endpoint path relative to Clowder api.
        :param content: Content to send as the body of the request as a dict.
        :param dict params: Additional parameters to pass to clowder.
        :param dict headers: Additional headers to pass to clowder, if not set content-type will be set.
        :return: the json-encoded content of a response.
        :raises: `requests.HTTPError`
        """
        attempt = 0
        url = '%s/api/%s' % (self.host, path.lstrip('/'))
        if params is None:
            params = dict()
        if headers is None:
            headers = {'content-type': 'application/json'}
        if self.username and self.password:
            auth = (self.username, self.password)
            params['key'] = None
        elif self.key:
            auth = None
            params['key'] = self.key
        else:
            auth = None
        while True:
            try:
                response = requests.post(url, data=json.dumps(content), headers=headers, params=params,
                                         auth=auth, timeout=self.timeout, verify=self.ssl)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as e:
                attempt += 1
                if attempt > self.retries:
                    self.logger.exception("Error calling POST url %s: %s" % (url, str(e)))
                    raise e
                else:
                    self.logger.debug("Error calling POST url %s: %s" % (url, str(e)))

    def delete(self, path, params=None, headers=None):
        """
        Call HTTP DELETE against `path`.

        :param path: Endpoint path relative to Clowder api.
        :param dict params: Additional parameters to pass to clowder.
        :param dict headers: Additional headers to pass to clowder
        :raises: `requests.HTTPError`
        """
        attempt = 0
        url = '%s/api/%s' % (self.host, path.lstrip('/'))
        if params is None:
            params = dict()
        if headers is None:
            headers = {'content-type': 'application/json'}
        if self.username and self.password:
            auth = (self.username, self.password)
            params['key'] = None
        elif self.key:
            auth = None
            params['key'] = self.key
        else:
            auth = None
        while True:
            try:
                response = requests.delete(url, headers=headers, params=params,
                                           auth=auth, timeout=self.timeout, verify=self.ssl)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as e:
                attempt += 1
                if attempt > self.retries:
                    self.logger.exception("Error calling DELETE url %s: %s" % (url, str(e)))
                    raise e
                else:
                    self.logger.debug("Error calling DELETE url %s: %s" % (url, str(e)))

    def get_file(self, path, filename=None, params=None, headers=None):
        """
        Call HTTP GET against `path` and writes the result to a file.

        :param path: Endpoint path relative to Clowder api.
        :param filename: The name of the file, if not set a temporary file is created.
        :param dict params: Additional parameters to pass to clowder.
        :param dict headers: Additional headers to pass to clowder.
        :return: the filename where the output is written.
        :raises: `requests.HTTPError`
        """
        attempt = 0
        url = '%s/api/%s' % (self.host, path.lstrip('/'))
        if params is None:
            params = dict()
        if headers is None:
            headers = {'content-type': 'application/json'}
        if self.username and self.password:
            auth = (self.username, self.password)
            params['key'] = None
        elif self.key:
            auth = None
            params['key'] = self.key
        else:
            auth = None
        if filename is None:
            (fd, filename) = tempfile.mkstemp(".tmp", "clowder")
            os.close(fd)

        while True:
            try:
                response = requests.get(url, stream=True, headers=headers, params=params,
                                        auth=auth, timeout=self.timeout, verify=self.ssl)
                response.raise_for_status()
                with open(filename, mode="wb") as outputfile:
                    for chunk in response.iter_content(chunk_size=10 * 1024):
                        outputfile.write(chunk)
                return filename
            except requests.HTTPError as e:
                os.remove(filename)
                attempt += 1
                if attempt > self.retries:
                    self.logger.exception("Error calling DELETE url %s: %s" % (url, e.response.text))
                    raise e
                else:
                    self.logger.debug("Error calling DELETE url %s: %s" % (url, e.response.text))
            except Exception:
                os.remove(filename)
                raise

    def post_file(self, path, filename, params=None, headers=None):
        """
        Call HTTP POST against `path` with `content` in body. Header with content-type is not required.

        :param path: Endpoint path relative to Clowder api.
        :param filename: The name of the file to post.
        :param dict params: Additional parameters to pass to clowder.
        :param dict headers: Additional headers to pass to clowder.
        :return: the json-encoded content of a response.
        :raises: `requests.HTTPError`
        """
        attempt = 0
        url = '%s/api/%s' % (self.host, path.lstrip('/'))
        if params is None:
            params = dict()
        if self.username and self.password:
            auth = (self.username, self.password)
            params['key'] = None
        elif self.key:
            auth = None
            params['key'] = self.key
        else:
            auth = None
        while True:
            try:
                response = requests.post(url, files={"File": open(filename, 'rb')}, headers=headers, params=params,
                                         auth=auth, timeout=self.timeout, verify=self.ssl)
                response.raise_for_status()
                return response.json()
            except requests.HTTPError as e:
                attempt += 1
                if attempt > self.retries:
                    self.logger.exception("Error calling POST url %s: %s" % (url, str(e)))
                    raise e
                else:
                    self.logger.debug("Error calling POST url %s: %s" % (url, str(e)))
