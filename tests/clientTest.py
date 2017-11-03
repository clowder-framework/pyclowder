import unittest

import pyclowder.client


class TestClientMethods(unittest.TestCase):
    def test_host_no_slash(self):
        client = pyclowder.client.ClowderClient(host='https://clowder.ncsa.illinois.edu/clowder/')
        self.assertEqual(client.host, 'https://clowder.ncsa.illinois.edu/clowder')

    def test_host_version(self):
        client = pyclowder.client.ClowderClient(host='https://clowder.ncsa.illinois.edu/clowder/')
        status = client.get('/status')
        self.assertIn('version', status)
        version = status['version']
        self.assertEqual(version.get('branch'), 'master')
