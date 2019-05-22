"""Clowder API

This module provides simple wrappers around the clowder Sections API
"""

import json
from client import ClowderClient


class SectionsApi(object):
    """
        API to manage the REST CRUD endpoints for sections.
    """

    def __init__(self, client=None, host=None, key=None, username=None, password=None):
        """Set client if provided otherwise create new one"""

        if client:
            self.api_client = client
        else:
            self.client = ClowderClient(host=host, key=key, username=username, password=password)

    def add_description(self, section_id, description):
        """Upload description to a section."""

        self.client.post("sections/%s/description" % section_id, json.dumps(description))

    def add_tags(self, section_id, tags):
        """Upload section tag."""

        self.client.post("sections/%s/tags" % section_id, json.dumps(tags))

    def upload(self, section_data):
        """Upload section to Clowder.

        Keyword arguments:
        section_data -- section data to send
        """

        section = self.client.post("sections", section_data)
        return section['id']
