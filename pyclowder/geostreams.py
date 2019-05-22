"""Clowder API

This module provides simple wrappers around the clowder Geostreams API
"""

from client import ClowderClient


class GeostreamsApi(object):
    """
        API to manage the REST CRUD endpoints for geostreams.
    """

    def __init__(self, client=None, host=None, key=None,
                 username=None, password=None):
        """Set client if provided otherwise create new one"""
        from pyclowder.files import FilesApi
        self.FilesApi = FilesApi

        if client:
            self.client = client
        else:
            self.client = ClowderClient(host=host, key=key,
                                        username=username, password=password)

    def create_sensor(self, name, geom, type, region):
        """Create a new sensor in Geostreams.

        Keyword arguments:
        name -- name of new sensor to create
        geom -- GeoJSON object of sensor geometry
        type -- JSON object with {"id", "title", and "sensorType"}
        region -- region of sensor
        """

        body = {
            "name": name,
            "type": "Point",
            "geometry": geom,
            "properties": {
                "popupContent": name,
                "type": type,
                "name": name,
                "region": region
            }
        }

        self.client.post("geostreams/sensors", body)

    def create_stream(self, name, sensor_id, geom, properties=None):
        """Create a new stream in Geostreams.

        Keyword arguments:
        name -- name of new stream to create
        sensor_id -- id of sensor to attach stream to
        geom -- GeoJSON object of sensor geometry
        properties -- JSON object with any desired properties
        """

        if properties is None:
            properties = {}

        body = {
            "name": name,
            "type": "Feature",
            "geometry": geom,
            "properties": properties,
            "sensor_id": str(sensor_id)
        }

        self.client.post("geostreams/streams", body)

    def create_datapoint(self, stream_id, geom, starttime, endtime, properties=None):
        """Create a new datapoint in Geostreams.

        Keyword arguments:
        stream_id -- id of stream to attach datapoint to
        geom -- GeoJSON object of sensor geometry
        starttime -- start time, in format 2017-01-25T09:33:02-06:00
        endtime -- end time, in format 2017-01-25T09:33:02-06:00
        properties -- JSON object with any desired properties
        """

        if properties is None:
            properties = {}

        body = {
            "start_time": starttime,
            "end_time": endtime,
            "type": "Point",
            "geometry": geom,
            "properties": properties,
            "stream_id": str(stream_id)
        }

        self.client.post("geostreams/datapoints", body)

    def get_datapoints(self, stream_id):
        pass

    def get_sensor_by_name(self, name):
        """Get sensor by name from Geostreams.

        Keyword arguments:
        name -- name of sensor to search for
        """

        return self.client.get("geostreams/sensors?sensor_name=%s" % name)

    def get_sensors_by_circle(self, lon, lat, radius):
        """Get sensors by coordinate from Geostreams.

        Keyword arguments:
        lon -- longitude of point
        lat -- latitude of point
        radius -- distance in meters around point to search
        """

        return self.client.get("geostreams/sensors?geocode=%s,%s,%s" % (lat, lon, radius))

    def get_sensors_by_polygon(self, coord_list):
        """Get sensors by coordinate from Geostreams.

        Keyword arguments:
        coord_list -- list of (lon/lat) coordinate pairs forming polygon vertices
        """

        coord_strings = [str(i) for i in coord_list]
        return self.client.get("geostreams/sensors?geocode=%s" % (','.join(coord_strings)))

    def get_stream_by_name(self, name):
        """Get stream by name from Geostreams.

        Keyword arguments:
        name -- name of stream to search for
        """

        return self.client.get("geostreams/streams?stream_name=%s" % name)

    def get_streams_by_circle(self, lon, lat, radius):
        """Get streams by coordinate from Geostreams.

        Keyword arguments:
        lon -- longitude of point
        lat -- latitude of point
        radius -- distance in meters around point to search
        """

        return self.client.get("geostreams/stream?geocode=%s,%s,%s" % (lat, lon, radius))

    def get_streams_by_polygon(self, coord_list):
        """Get streams by coordinate from Geostreams.

        Keyword arguments:
        coord_list -- list of (lon/lat) coordinate pairs forming polygon vertices
        """

        coord_strings = [str(i) for i in coord_list]
        return self.client.get("geostreams/stream?geocode=%s" % (','.join(coord_strings)))
