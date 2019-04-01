"""Clowder API

This module provides simple wrappers around the clowder Geostreams API
"""

import json
import logging

import requests


def create_sensor(connector, host, key, sensorname, geom, type, region):
    """Create a new sensor in Geostreams.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    sensorname -- name of new sensor to create
    geom -- GeoJSON object of sensor geometry
    type -- JSON object with {"id", "title", and "sensorType"}
    region -- region of sensor
    """

    logger = logging.getLogger(__name__)

    body = {
        "name": sensorname,
        "type": "Point",
        "geometry": geom,
        "properties": {
            "popupContent": sensorname,
            "type": type,
            "name": sensorname,
            "region": region
        }
    }

    url = "%sapi/geostreams/sensors?key=%s" % (host, key)

    result = requests.post(url, headers={'Content-type': 'application/json'},
                           data=json.dumps(body),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    sensorid = result.json()['id']
    logger.debug("sensor id = [%s]", sensorid)

    return sensorid


def create_stream(connector, host, key, streamname, sensorid, geom, properties=None):
    """Create a new stream in Geostreams.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    streamname -- name of new stream to create
    sensorid -- id of sensor to attach stream to
    geom -- GeoJSON object of sensor geometry
    properties -- JSON object with any desired properties
    """

    logger = logging.getLogger(__name__)

    if not properties:
        properties = {}

    body = {
        "name": streamname,
        "type": "Feature",
        "geometry": geom,
        "properties": properties,
        "sensor_id": str(sensorid)
    }

    url = "%sapi/geostreams/streams?key=%s" % (host, key)

    result = requests.post(url, headers={'Content-type': 'application/json'},
                           data=json.dumps(body),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    streamid = result.json()['id']
    logger.debug("stream id = [%s]", streamid)

    return streamid


def create_datapoint(connector, host, key, streamid, geom, starttime, endtime, properties=None):
    """Create a new datapoint in Geostreams.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    streamid -- id of stream to attach datapoint to
    geom -- GeoJSON object of sensor geometry
    starttime -- start time, in format 2017-01-25T09:33:02-06:00
    endtime -- end time, in format 2017-01-25T09:33:02-06:00
    properties -- JSON object with any desired properties
    """

    logger = logging.getLogger(__name__)

    if not properties:
        properties = {}

    body = {
        "start_time": starttime,
        "end_time": endtime,
        "type": "Point",
        "geometry": geom,
        "properties": properties,
        "stream_id": str(streamid)
    }

    url = '%sapi/geostreams/datapoints?key=%s' % (host, key)

    result = requests.post(url, headers={'Content-type': 'application/json'},
                           data=json.dumps(body),
                           verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    dpid = result.json()['id']
    logger.debug("datapoint id = [%s]", dpid)

    return dpid


def get_sensor_by_name(connector, host, key, sensorname):
    """Get sensor by name from Geostreams, or return None.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    sensorname -- name of sensor to search for
    """

    logger = logging.getLogger(__name__)

    url = "%sapi/geostreams/sensors?sensor_name=%s&key=%s" % (host, sensorname, key)

    result = requests.get(url,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    for sens in result.json():
        if 'name' in sens and sens['name'] == sensorname:
            logger.debug("found sensor '%s' = [%s]" % (sensorname, sens['id']))
            return sens

    return None


def get_sensors_by_circle(connector, host, key, lon, lat, radius=0):
    """Get sensor by coordinate from Geostreams, or return None.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    lon -- longitude of point
    lat -- latitude of point
    radius -- distance in meters around point to search
    """

    url = "%sapi/geostreams/sensors?geocode=%s,%s,%s&key=%s" % (host, lat, lon, radius, key)

    result = requests.get(url,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    # Return first sensor
    jbody = result.json()
    if len(jbody) > 0:
        return jbody
    else:
        return None


def get_sensors_by_polygon(connector, host, key, coord_list):
    """Get sensor by coordinate from Geostreams, or return None.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    coord_list -- list of (lon/lat) coordinate pairs forming polygon vertices
    """

    coord_strings = [str(i) for i in coord_list]
    url = "%sapi/geostreams/sensors?geocode=%s&key=%s" % (host, ','.join(coord_strings), key)

    result = requests.get(url,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    # Return first sensor
    jbody = result.json()
    if len(jbody) > 0:
        return jbody
    else:
        return None


def get_stream_by_name(connector, host, key, streamname):
    """Get stream by name from Geostreams, or return None.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    streamname -- name of stream to search for
    """

    logger = logging.getLogger(__name__)

    url = "%sapi/geostreams/streams?stream_name=%s&key=%s" % (host, streamname, key)

    result = requests.get(url,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    for strm in result.json():
        if 'name' in strm and strm['name'] == streamname:
            logger.debug("found stream '%s' = [%s]" % (streamname, strm['id']))
            return strm

    return None


def get_streams_by_circle(connector, host, key, lon, lat, radius=0):
    """Get stream by coordinate from Geostreams, or return None.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    lon -- longitude of point
    lat -- latitude of point
    radius -- distance in meters around point to search
    """

    url = "%sapi/geostreams/stream?geocode=%s,%s,%s&key=%s" % (host, lat, lon, radius, key)

    result = requests.get(url,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    jbody = result.json()
    if len(jbody) > 0:
        return jbody
    else:
        return None


def get_streams_by_polygon(connector, host, key, coord_list):
    """Get stream by coordinate from Geostreams, or return None.

    Keyword arguments:
    connector -- connector information, used to get missing parameters and send status updates
    host -- the clowder host, including http and port, should end with a /
    key -- the secret key to login to clowder
    coord_list -- list of (lon/lat) coordinate pairs forming polygon vertices
    """

    coord_strings = [str(i) for i in coord_list]
    url = "%sapi/geostreams/stream?geocode=%s&key=%s" % (host, ','.join(coord_strings), key)

    result = requests.get(url,
                          verify=connector.ssl_verify if connector else True)
    result.raise_for_status()

    jbody = result.json()
    if len(jbody) > 0:
        return jbody
    else:
        return None
