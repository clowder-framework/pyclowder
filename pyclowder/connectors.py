"""Connectors

The system has two connectors defined by default. The connectors are used to
start the extractors. The connector will look for messages and call the
check_message and process_message of the extractor. The two connectors are
RabbitMQConnector and HPCConnector. Both of these will call the check_message
first, and based on the result of this will ignore the message, download the
file and call process_message or bypass the download and just call the
process_message.

RabbitMQConnector

The RabbitMQ connector connects to a RabbitMQ instance, creates a queue and
binds itself to that queue. Any message in the queue will be fetched and
passed to the check_message and process_message. This connector takesthree
parameters:

* rabbitmq_uri [REQUIRED] : the uri of the RabbitMQ server
* rabbitmq_exchange [OPTIONAL] : the exchange to which to bind the queue
* rabbitmq_key [OPTIONAL] : the key that binds the queue to the exchange

HPCConnector

The HPC connector will run extractions based on the pickle files that are
passed in to the constructor as an argument. Once all pickle files are
processed the extractor will stop. The pickle file is assumed to have one
additional argument, the logfile that is being monitored to send feedback
back to clowder. This connector takes a single argument (which can be list):

* picklefile [REQUIRED] : a single file, or list of files that are the
                          pickled messsages to be processed.
"""

import json
import logging
import os
import pickle
import subprocess
import time
import tempfile
import threading

import pika
import requests

import pyclowder.datasets
import pyclowder.files
import pyclowder.utils


class Connector(object):
    """ Class that will listen for messages.

     Once a message is received this will start the extraction process. It is assumed
    that there is only one Connector per thread.
    """

    registered_clowder = list()

    def __init__(self, extractor_info, check_message=None, process_message=None, ssl_verify=True, mounted_paths=None):
        self.extractor_info = extractor_info
        self.check_message = check_message
        self.process_message = process_message
        self.ssl_verify = ssl_verify
        if mounted_paths is None:
            self.mounted_paths = {}
        else:
            self.mounted_paths = mounted_paths

    def listen(self):
        """Listen for incoming messages.

         This function will not return until all messages are processed, or the process is
         interrupted.
         """
        pass

    def alive(self):
        """Return whether connection is still alive or not."""
        return True

    # pylint: disable=too-many-branches,too-many-statements
    def _process_message(self, body):
        """The actual processing of the message.

        This will register the extractor with the clowder instance that the message came from.
        Next it will call check_message to see if the message should be processed and if the
        file should be downloaded. Finally it will call the actual process_message function.
        """

        logger = logging.getLogger(__name__)

        message_type = body['routing_key']
        retry_count = 0 if 'retry_count' not in body else body['retry_count']

        # id of file that was added
        fileid = body['id']
        intermediatefileid = body['intermediateId']
        # id of dataset file was added to
        datasetid = body.get('datasetId', '')
        # reference to parent of resource (file parent is usually a dataset)
        if datasetid != '':
            parent_ref = {"type": "dataset", "id": datasetid}
        else:
            # TODO: enhance this for collection and dataset parents
            parent_ref = {}
        # name of file that was added - only on file messages, NOT dataset messages!
        filename = body.get('filename', '')
        # get & clean clowder connection details
        secret_key = body['secretKey']
        host = body['host']
        if not host.endswith('/'):
            host += '/'
        if host == "":
            # TODO CATS-583 error with metadata only
            return

        # determine resource type
        if message_type.find(".dataset.") > -1:
            resource_type = "dataset"
        elif message_type.find(".file.") > -1:
            resource_type = "file"
        elif message_type.find("metadata.added") > -1:
            resource_type = "metadata"
        elif message_type == "extractors."+self.extractor_info['name']:
            # This was a manually submitted extraction
            if datasetid == fileid:
                resource_type = "dataset"
            else:
                resource_type = "file"
        else:
            # This will be default value
            resource_type = "file"

        # determine what to download (if needed) and add relevant data to resource
        if resource_type == "dataset":
            resource_id = datasetid
            ext = ''
            datasetinfo = pyclowder.datasets.get_info(self, host, secret_key, datasetid)
            filelist = pyclowder.datasets.get_file_list(self, host, secret_key, datasetid)
            # populate filename field with the file that triggered this message
            latest_file = None
            for f in filelist:
                if f['id'] == fileid:
                    latest_file = f['filename']
                    break
            resource = {
                "type": "dataset",
                "id": resource_id,
                "name": datasetinfo["name"],
                "files": filelist,
                "latest_file": latest_file,
                "parent": parent_ref,
                "dataset_info": datasetinfo
            }

        elif resource_type == "file":
            resource_id = fileid
            ext = os.path.splitext(filename)[1]
            resource = {
                "type": "file",
                "id": resource_id,
                "intermediate_id": intermediatefileid,
                "name": filename,
                "file_ext": ext,
                "parent": parent_ref
            }

        elif resource_type == "metadata":
            resource_id = body['resourceId']
            resource = {
                "type": body['resourceType'],
                "id": resource_id,
                "parent": parent_ref,
                "metadata": body['metadata']
            }

        # register extractor
        url = "%sapi/extractors" % host
        if url not in Connector.registered_clowder:
            Connector.registered_clowder.append(url)
            self.register_extractor("%s?key=%s" % (url, secret_key))

        # tell everybody we are starting to process the file
        self.status_update(pyclowder.utils.StatusMessage.start, resource, "Started processing")

        # checks whether to process the file in this message or not
        # pylint: disable=too-many-nested-blocks
        try:
            check_result = pyclowder.utils.CheckMessage.download
            if self.check_message:
                check_result = self.check_message(self, host, secret_key, resource, body)
            if check_result != pyclowder.utils.CheckMessage.ignore:
                if self.process_message:

                    # PREPARE THE FILE FOR PROCESSING ---------------------------------------
                    if resource["type"] == "file":
                        inputfile = None
                        have_local_file = False
                        try:
                            if check_result != pyclowder.utils.CheckMessage.bypass:
                                # first check if file is accessible locally
                                file_metadata = pyclowder.files.download_info(self, host, secret_key, resource["id"])
                                if 'filepath' in file_metadata:
                                    file_path = file_metadata['filepath']
                                    for source_path in self.mounted_paths:
                                        if file_path.startswith(source_path):
                                            inputfile = file_path.replace(source_path,
                                                                          self.mounted_paths[source_path])
                                            have_local_file = True
                                            break

                                # otherwise download file
                                if not have_local_file:
                                    inputfile = pyclowder.files.download(self, host, secret_key, fileid,
                                                                         intermediatefileid, ext)
                                resource['local_paths'] = [inputfile]

                            self.process_message(self, host, secret_key, resource, body)
                        finally:
                            if inputfile is not None and not have_local_file:
                                try:
                                    os.remove(inputfile)
                                except OSError:
                                    logger.exception("Error removing download file")

                    # PREPARE THE DATASET FOR PROCESSING ---------------------------------------
                    else:
                        inputzip = None
                        tmp_files_created = []
                        tmp_dirs_created = []
                        file_paths = []
                        try:
                            if check_result != pyclowder.utils.CheckMessage.bypass:
                                # first check if any files in dataset accessible locally
                                ds_file_list = pyclowder.datasets.get_file_list(self, host,
                                                                                secret_key, resource["id"])
                                located_files = []
                                missing_files = []
                                for dsf in ds_file_list:
                                    have_local_file = False
                                    if 'filepath' in dsf:
                                        dsf_path = dsf['filepath']
                                        for source_path in self.mounted_paths:
                                            if dsf_path.startswith(source_path):
                                                # Store pointer to local file if found
                                                have_local_file = True
                                                inputfile = dsf_path.replace(source_path,
                                                                             self.mounted_paths[source_path])
                                                if os.path.exists(inputfile):
                                                    located_files.append(inputfile)

                                                    # Also download metadata for the file (normally in ds .zip file)
                                                    md = pyclowder.files.download_metadata(self, host, secret_key,
                                                                                           dsf["id"])
                                                    md_dir = tempfile.mkdtemp(suffix=dsf['id'])
                                                    tmp_dirs_created.append(md_dir)
                                                    md_name = os.path.basename(dsf_path)+"_metadata.json"
                                                    (fd, md_file) = tempfile.mkstemp(suffix=md_name, dir=md_dir)
                                                    with os.fdopen(fd, "w") as tmp_file:
                                                        tmp_file.write(json.dumps(md))
                                                    located_files.append(md_file)
                                                    tmp_files_created.append(md_file)
                                                    break
                                    if not have_local_file:
                                        missing_files.append(dsf)

                                # if some files found locally, check & download any that weren't
                                if len(located_files) > 0:
                                    for dsf in missing_files:
                                        dsf_path = dsf['filepath']

                                        # Download file to temp directory
                                        file_ext = dsf['filename'].split(".")[-1]
                                        inputfile = pyclowder.files.download(self, host, secret_key, dsf['id'],
                                                                             dsf['id'], ".%s" % file_ext)
                                        located_files.append(inputfile)
                                        tmp_files_created.append(inputfile)
                                        logger.info("Downloaded file: %s" % inputfile)

                                        # Also download metadata for the file (normally in ds .zip file)
                                        md = pyclowder.files.download_metadata(self, host, secret_key,
                                                                               dsf["id"])
                                        md_dir = tempfile.mkdtemp(suffix=dsf['id'])
                                        tmp_dirs_created.append(md_dir)
                                        md_name = os.path.basename(dsf_path)+"_metadata.json"
                                        (fd, md_file) = tempfile.mkstemp(suffix=md_name, dir=md_dir)
                                        with os.fdopen(fd, "w") as tmp_file:
                                            tmp_file.write(json.dumps(md))
                                        located_files.append(md_file)
                                        tmp_files_created.append(md_file)

                                    # Lastly need to get dataset metadata (normally in ds .zip file)
                                    md = pyclowder.datasets.download_metadata(self, host, secret_key,
                                                                              datasetid)
                                    md_dir = tempfile.mkdtemp(suffix=datasetid)
                                    tmp_dirs_created.append(md_dir)
                                    md_name = "%s_dataset_metadata.json" % datasetid
                                    (fd, md_file) = tempfile.mkstemp(suffix=md_name, dir=md_dir)
                                    with os.fdopen(fd, "w") as tmp_file:
                                        tmp_file.write(json.dumps(md))
                                    located_files.append(md_file)
                                    tmp_files_created.append(md_file)

                                    file_paths = located_files

                                # If we didn't find any files locally, download dataset .zip
                                else:
                                    inputzip = pyclowder.datasets.download(self, host, secret_key,
                                                                           datasetid)
                                    file_paths = pyclowder.utils.extract_zip_contents(inputzip)
                                    tmp_files_created += file_paths

                            resource['local_paths'] = file_paths
                            self.process_message(self, host, secret_key, resource, body)
                        finally:
                            if inputzip is not None:
                                try:
                                    os.remove(inputzip)
                                except OSError:
                                    logger.exception("Error removing download zip file")
                            for tmp_f in tmp_files_created:
                                try:
                                    os.remove(tmp_f)
                                except OSError:
                                    logger.exception("Error removing temporary dataset file")

            else:
                self.status_update(pyclowder.utils.StatusMessage.processing, resource, "Skipped in check_message")

            self.message_ok(resource)

        except SystemExit as exc:
            status = "sys.exit : " + exc.message
            logger.exception("[%s] %s", resource_id, status)
            self.status_update(pyclowder.utils.StatusMessage.error, resource, status)
            self.message_resubmit(resource, retry_count)
            raise
        except KeyboardInterrupt:
            status = "keyboard interrupt"
            logger.exception("[%s] %s", fileid, status)
            self.status_update(pyclowder.utils.StatusMessage.error, resource, status)
            self.message_resubmit(resource, retry_count)
            raise
        except GeneratorExit:
            status = "generator exit"
            logger.exception("[%s] %s", fileid, status)
            self.status_update(pyclowder.utils.StatusMessage.error, resource, status)
            self.message_resubmit(resource, retry_count)
            raise
        except StandardError as exc:
            status = "standard error : " + str(exc.message)
            logger.exception("[%s] %s", resource_id, status)
            self.status_update(pyclowder.utils.StatusMessage.error, resource, status)
            if retry_count < 10:
                self.message_resubmit(resource, retry_count+1)
            else:
                self.message_error(resource)
        except subprocess.CalledProcessError as exc:
            status = str.format("Error processing [exit code={}]\n{}", exc.returncode, exc.output)
            logger.exception("[%s] %s", resource_id, status)
            self.status_update(pyclowder.utils.StatusMessage.error, resource, status)
            self.message_error(resource)
        except Exception as exc:  # pylint: disable=broad-except
            status = "Error processing : " + exc.message
            logger.exception("[%s] %s", resource_id, status)
            self.status_update(pyclowder.utils.StatusMessage.error, resource, status)
            self.message_error(resource)

    def register_extractor(self, endpoints):
        """Register extractor info with Clowder.

        This assumes a file called extractor_info.json to be located in either the
        current working directory, or the folder where the main program is started.
        """

        # don't do any work if we wont register the endpoint
        if not endpoints or endpoints == "":
            return

        logger = logging.getLogger(__name__)

        headers = {'Content-Type': 'application/json'}
        data = self.extractor_info

        for url in endpoints.split(','):
            if url not in Connector.registered_clowder:
                Connector.registered_clowder.append(url)
                try:
                    result = requests.post(url.strip(), headers=headers,
                                           data=data,
                                           verify=self.ssl_verify)
                    result.raise_for_status()
                    logger.debug("Registering extractor with %s : %s", url, result.text)
                except Exception as exc:  # pylint: disable=broad-except
                    logger.exception('Error in registering extractor: ' + str(exc))

    # pylint: disable=no-self-use
    def status_update(self, status, resource, message):
        """Sends a status message.

        These messages, unlike logger messages, will often be send back to clowder to let
        the instance know the progress of the extractor.

        Keyword arguments:
        status - START | PROCESSING | DONE | ERROR
        resource  - descriptor object with {"type", "id"} fields
        message - contents of the status update
        """
        logging.getLogger(__name__).info("[%s] : %s: %s", resource["id"], status, message)

    def message_ok(self, resource):
        self.status_update(pyclowder.utils.StatusMessage.done, resource, "Done processing")

    def message_error(self, resource):
        self.status_update(pyclowder.utils.StatusMessage.error, resource, "Error processing message")

    def message_resubmit(self, resource, retry_count):
        self.status_update(pyclowder.utils.StatusMessage.processing, resource, "Resubmitting message (attempt #%s)"
                           % retry_count)


# pylint: disable=too-many-instance-attributes
class RabbitMQConnector(Connector):
    """Listens for messages on RabbitMQ.

    This will connect to rabbitmq and register the extractor with a queue. If the exchange
    and key are specified it will bind the exchange to the queue. If an exchange is
    specified it will always try to bind the special key extractors.<extractor_info[name]> to the
    exchange and queue.
    """

    # pylint: disable=too-many-arguments
    def __init__(self, extractor_info, rabbitmq_uri, rabbitmq_exchange=None, rabbitmq_key=None,
                 check_message=None, process_message=None, ssl_verify=True, mounted_paths=None):
        Connector.__init__(self, extractor_info, check_message, process_message, ssl_verify, mounted_paths)
        self.rabbitmq_uri = rabbitmq_uri
        self.rabbitmq_exchange = rabbitmq_exchange
        self.rabbitmq_key = rabbitmq_key
        self.channel = None
        self.connection = None
        self.consumer_tag = None
        self.worker = None

    def connect(self):
        """connect to rabbitmq using URL parameters"""

        parameters = pika.URLParameters(self.rabbitmq_uri)
        self.connection = pika.BlockingConnection(parameters)

        # connect to channel
        self.channel = self.connection.channel()

        # setting prefetch count to 1 so we only take 1 message of the bus at a time,
        # so other extractors of the same type can take the next message.
        self.channel.basic_qos(prefetch_count=1)

        # declare the queue in case it does not exist
        self.channel.queue_declare(queue=self.extractor_info['name'], durable=True)
        self.channel.queue_declare(queue='error.'+self.extractor_info['name'], durable=True)

        # register with an exchange
        if self.rabbitmq_exchange:
            # declare the exchange in case it does not exist
            self.channel.exchange_declare(exchange=self.rabbitmq_exchange, exchange_type='topic',
                                          durable=True)

            # connect queue and exchange
            if self.rabbitmq_key:
                if isinstance(self.rabbitmq_key, str):
                    self.channel.queue_bind(queue=self.extractor_info['name'],
                                            exchange=self.rabbitmq_exchange,
                                            routing_key=self.rabbitmq_key)
                else:
                    for key in self.rabbitmq_key:
                        self.channel.queue_bind(queue=self.extractor_info['name'],
                                                exchange=self.rabbitmq_exchange,
                                                routing_key=key)

            self.channel.queue_bind(queue=self.extractor_info['name'],
                                    exchange=self.rabbitmq_exchange,
                                    routing_key="extractors." + self.extractor_info['name'])

    def listen(self):
        """Listen for messages coming from RabbitMQ"""

        # check for connection
        if not self.channel:
            self.connect()

        # create listener
        self.consumer_tag = self.channel.basic_consume(self.on_message, queue=self.extractor_info['name'],
                                                       no_ack=False)

        # start listening
        logging.getLogger(__name__).info("Starting to listen for messages.")
        try:
            # pylint: disable=protected-access
            while self.channel and self.channel._consumer_infos:
                self.channel.connection.process_data_events(time_limit=1)  # 1 second
                if self.worker:
                    self.worker.process_messages(self.channel)
                    if self.worker.thread and not self.worker.thread.isAlive():
                        self.worker = None
        except SystemExit:
            raise
        except KeyboardInterrupt:
            raise
        except GeneratorExit:
            raise
        except Exception:  # pylint: disable=broad-except
            logging.getLogger(__name__).exception("Error while consuming messages.")
        finally:
            logging.getLogger(__name__).info("Stopped listening for messages.")
            if self.channel:
                self.channel.close()
                self.channel = None
            if self.connection:
                self.connection.close()
                self.connection = None

    def stop(self):
        """Tell the connector to stop listening for messages."""
        if self.channel:
            self.channel.stop_consuming(self.consumer_tag)

    def alive(self):
        return self.connection is not None

    def on_message(self, channel, method, header, body):
        """When the message is received this will call the generic _process_message in
        the connector class. Any message will only be acked if the message is processed,
        or there is an exception (except for SystemExit and SystemError exceptions).
        """

        json_body = json.loads(body)
        if 'routing_key' not in json_body and method.routing_key:
            json_body['routing_key'] = method.routing_key

        self.worker = RabbitMQHandler(self.extractor_info, self.check_message, self.process_message,
                                      self.ssl_verify, self.mounted_paths, method, header, body)
        self.worker.start_thread(json_body)


class RabbitMQHandler(Connector):
    """Simple handler that will process a single message at a time.

    To avoid sharing non-threadsafe channels across threads, this will maintain
    a queue of messages that the super- loop can access and send later.
    """

    def __init__(self, extractor_info, check_message=None, process_message=None, ssl_verify=True,
                 mounted_paths=None, method=None, header=None, body=None):
        Connector.__init__(self, extractor_info, check_message, process_message, ssl_verify, mounted_paths)
        self.method = method
        self.header = header
        self.body = body
        self.messages = []
        self.thread = None

    def start_thread(self, json_body):
        """Start the separate thread for processing & create a queue for messages.

        messages is a list of message objects:
        {
            "type": status/ok/error/resubmit
            "resource": resource
            "status": status (status_update only)
            "message": message content (status_update only)
            "retry_count": retry_count (message_resubmit only)
        }
        """
        self.thread = threading.Thread(target=self._process_message, args=(json_body,))
        self.thread.start()

    def process_messages(self, channel):
        while self.messages:
            msg = self.messages.pop()

            if msg["type"] == 'status':
                properties = pika.BasicProperties(delivery_mode=2, correlation_id=self.header.correlation_id)
                channel.basic_publish(exchange='',
                                      routing_key=self.header.reply_to,
                                      properties=properties,
                                      body=json.dumps(msg['status']))

            elif msg["type"] == 'ok':
                channel.basic_ack(self.method.delivery_tag)

            elif msg["type"] == 'error':
                properties = pika.BasicProperties(delivery_mode=2)
                channel.basic_publish(exchange='',
                                      routing_key='error.' + self.extractor_info['name'],
                                      properties=properties,
                                      body=self.body)
                channel.basic_ack(self.method.delivery_tag)

            elif msg["type"] == 'resubmit':
                retry_count = msg['retry_count']
                queue = self.extractor_info['name']
                properties = pika.BasicProperties(delivery_mode=2, reply_to=self.header.reply_to)
                jbody = json.loads(self.body)
                jbody['retry_count'] = retry_count
                if 'exchange' not in jbody and self.method.exchange:
                    jbody['exchange'] = self.method.exchange
                if 'routing_key' not in jbody and self.method.routing_key and self.method.routing_key != queue:
                    jbody['routing_key'] = self.method.routing_key
                channel.basic_publish(exchange='',
                                      routing_key=queue,
                                      properties=properties,
                                      body=json.dumps(jbody))
                channel.basic_ack(self.method.delivery_tag)

    def status_update(self, status, resource, message):
        super(RabbitMQHandler, self).status_update(status, resource, message)
        status_report = dict()
        # TODO: Update this to check resource["type"] once Clowder better supports dataset events
        status_report['file_id'] = resource["id"]
        status_report['extractor_id'] = self.extractor_info['name']
        status_report['status'] = "%s: %s" % (status, message)
        status_report['start'] = pyclowder.utils.iso8601time()
        self.messages.append({"type": "status",
                              "status": status_report,
                              "resource": resource,
                              "message": message})

    def message_ok(self, resource):
        super(RabbitMQHandler, self).message_ok(resource)
        self.messages.append({"type": "ok"})

    def message_error(self, resource):
        super(RabbitMQHandler, self).message_error(resource)
        self.messages.append({"type": "error"})

    def message_resubmit(self, resource, retry_count):
        super(RabbitMQHandler, self).message_resubmit(resource, retry_count)
        self.messages.append({"type": "resubmit", "retry_count": retry_count})


class HPCConnector(Connector):
    """Takes pickle files and processes them."""

    # pylint: disable=too-many-arguments
    def __init__(self, extractor_info, picklefile,
                 check_message=None, process_message=None, ssl_verify=True, mounted_paths=None):
        Connector.__init__(self, extractor_info, check_message, process_message, ssl_verify, mounted_paths)
        self.picklefile = picklefile
        self.logfile = None

    def listen(self):
        """Reads the picklefile, sets up the logfile and call _process_message."""
        if isinstance(self.picklefile, str):
            try:
                with open(self.picklefile, 'rb') as pfile:
                    body = pickle.load(pfile)
                    self.logfile = body['logfile']
                    self._process_message(body)
            finally:
                self.logfile = None
        else:
            for onepickle in self.picklefile:
                try:
                    with open(onepickle, 'rb') as pfile:
                        body = pickle.load(pfile)
                        self.logfile = body['logfile']
                        self._process_message(body)
                finally:
                    self.logfile = None

    def alive(self):
        return self.logfile is not None

    def status_update(self, status, resource, message):
        """Store notification on log file with update"""

        logger = logging.getLogger(__name__)
        logger.debug("[%s] : %s : %s", resource["id"], status, message)

        if self.logfile and os.path.isfile(self.logfile) is True:
            try:
                with open(self.logfile, 'a') as log:
                    statusreport = dict()
                    statusreport['file_id'] = resource["id"]
                    statusreport['extractor_id'] = self.extractor_info['name']
                    statusreport['status'] = "%s: %s" % (status, message)
                    statusreport['start'] = time.strftime('%Y-%m-%dT%H:%M:%S')
                    log.write(json.dumps(statusreport) + '\n')
            except:
                logger.exception("Error: unable to write extractor status to log file")
                raise
