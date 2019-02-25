#!/usr/bin/env python

import datetime
import http.server
import json
import logging
import os
import threading
import time
import urllib.parse

import pika
import requests

rabbitmq_uri = os.getenv('RABBITMQ_URI', 'amqp://guest:guest@localhost/%2F')
rabbitmq_mgmt_port = os.getenv('RABBITMQ_MGMT_PORT', '15672')
rabbitmq_mgmt_url = ''

extractors = {}

update_frequency = 60

hostName = ""
hostPort = 9999


# ----------------------------------------------------------------------
# WEB SERVER
# ----------------------------------------------------------------------
class MyServer(http.server.BaseHTTPRequestHandler):
    def do_GET(self):
        self.send_response(200)
        self.send_header('Content-type', 'application/json')
        self.end_headers()
        self.wfile.write(bytes(json.dumps(extractors), 'utf-8'))


def http_server():
    server = http.server.HTTPServer((hostName, hostPort), MyServer)
    try:
        server.serve_forever()
    finally:
        server.server_close()


# ----------------------------------------------------------------------
# MESSAGES IN QUEUES
# ----------------------------------------------------------------------
def get_mgmt_queue_messages(queue):
    global rabbitmq_username, rabbitmq_password
    try:
        response = requests.get(rabbitmq_mgmt_url + queue, auth=(rabbitmq_username, rabbitmq_password), timeout=5)
        response.raise_for_status()
        return response.json()['messages']
    except:
        logging.exception("Error getting list of messages in %s" % queue)
        return 0


def update_counts():
    global extractors, update_frequency

    while True:
        for versions in extractors.values():
            for extractor in versions.values():
                # use management api to get counts
                old_waiting = get_mgmt_queue_messages(extractor['queue'])
                new_waiting = get_mgmt_queue_messages('extractors.' + extractor['queue'])
                errors = get_mgmt_queue_messages('error.' + extractor['queue'])

                extractor['messages'] = {
                    'queues': {
                        'total': old_waiting + new_waiting,
                        'direct': new_waiting,
                        'topic': old_waiting
                    },
                    'error': errors
                }

        time.sleep(update_frequency)


# ----------------------------------------------------------------------
# EXTRACTOR HEARTBEATS
# ----------------------------------------------------------------------
def callback(ch, method, properties, body):
    global extractors

    data = json.loads(body.decode('utf-8'))
    data['updated'] = datetime.datetime.now().isoformat()
    if 'id' not in data and 'extractor_info' not in data and 'queue' not in data:
        logging.error("missing fields in json : %r " % body)
        return

    extractor_info = data['extractor_info']

    if extractor_info['name'] not in extractors:
        extractors[extractor_info['name']] = {}

    if extractor_info['version'] not in extractors[extractor_info['name']]:
        extractors[extractor_info['name']][extractor_info['version']] = {
            'extractor_info': extractor_info,
            'queue': data['queue'],
            'extractors': {}
        }
    extractor = extractors[extractor_info['name']][extractor_info['version']]

    extractor['extractors'][data['id']] = {
        'last_seen': datetime.datetime.now().isoformat(),
    }

    if extractor['queue'] != data['queue']:
        logging.error("mismatched queue names %s != %s." % (data['queue'], extractor['queue']))
        extractor['queue'] = data['queue']


def extractors_monitor():
    global rabbitmq_mgmt_url, rabbitmq_mgmt_port, rabbitmq_username, rabbitmq_password

    params = pika.URLParameters(rabbitmq_uri)
    connection = pika.BlockingConnection(params)

    # create management url
    rabbitmq_url = ''
    if rabbitmq_mgmt_port != '':
        if params.ssl:
            rabbitmq_mgmt_url = 'https://'
        else:
            rabbitmq_mgmt_url = 'http://'
        rabbitmq_mgmt_url = "%s%s:%s/api/queues/%s/" % (rabbitmq_mgmt_url, params.host, rabbitmq_mgmt_port,
                                                        urllib.parse.quote_plus(params.virtual_host))
        rabbitmq_username = params.credentials.username
        rabbitmq_password = params.credentials.password

    # connect to channel
    channel = connection.channel()

    # create extractors exchange for fanout
    channel.exchange_declare(exchange='extractors', exchange_type='fanout', durable=True)

    # create anonymous queue
    result = channel.queue_declare(exclusive=True)
    channel.queue_bind(exchange='extractors', queue=result.method.queue)

    # listen for messages
    channel.basic_consume(callback, queue=result.method.queue, no_ack=True)

    channel.start_consuming()


# ----------------------------------------------------------------------
# MAIN
# ----------------------------------------------------------------------
if __name__ == "__main__":
    logging.basicConfig(format='%(asctime)-15s [%(threadName)-15s] %(levelname)-7s :'
                               ' %(name)s - %(message)s',
                        level=logging.INFO)
    logging.getLogger('requests.packages.urllib3.connectionpool').setLevel(logging.WARN)

    thread = threading.Thread(target=http_server)
    thread.setDaemon(True)
    thread.start()

    thread = threading.Thread(target=update_counts)
    thread.setDaemon(True)
    thread.start()

    extractors_monitor()
