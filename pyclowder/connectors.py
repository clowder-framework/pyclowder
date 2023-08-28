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

import errno
import json
import logging
import os
import pickle
import subprocess
import sys
import time
import tempfile
import threading
import uuid

import pika
import requests

import pyclowder.datasets
import pyclowder.files
import pyclowder.utils

import smtplib
from email.mime.text import MIMEText
from email.mime.multipart import MIMEMultipart
from string import Template


class Connector(object):
    """ Class that will listen for messages.

     Once a message is received this will start the extraction process. It is assumed
    that there is only one Connector per thread.
    """

    def __init__(self, extractor_name, extractor_info, check_message=None, process_message=None, ssl_verify=True,
                 mounted_paths=None, clowder_url=None, max_retry=10):
        self.extractor_name = extractor_name
        self.extractor_info = extractor_info
        self.check_message = check_message
        self.process_message = process_message
        self.ssl_verify = ssl_verify
        if mounted_paths is None:
            self.mounted_paths = {}
        else:
            self.mounted_paths = mounted_paths
        self.clowder_url = clowder_url
        self.max_retry = max_retry

        filename = 'notifications.json'
        self.smtp_server = None
        if os.path.isfile(filename):
            try:
                with open(filename) as notifications_file:
                    notifications_content = notifications_file.read()
                    notifications_template = Template(notifications_content)
                    notifications_json = json.loads(notifications_content)
                    notifications_json['extractor_name'] = extractor_name
                    notifications = notifications_template.safe_substitute(notifications_json)

                    notifications_interpolate = json.loads(notifications)
                    self.smtp_server = os.getenv('EMAIL_SERVER', None)
                    self.emailmsg = MIMEMultipart('alternative')

                    self.emailmsg['From'] = os.getenv('EMAIL_SENDER', notifications_json.get('sender'))
                    self.emailmsg['Subject'] = notifications_interpolate.get('notifications').get('email').get(
                        'subject')
                    self.emailmsg['Body'] = notifications_interpolate.get('notifications').get('email').get('body')
            except Exception:  # pylint: disable=broad-except
                print("Error loading notifications.json")

    def email(self, emaillist, clowderurl):
        """ Send extraction completion as the email notification """
        logger = logging.getLogger(__name__)
        if emaillist and self.smtp_server:
            server = smtplib.SMTP(self.smtp_server)
            msg = MIMEMultipart('alternative')
            msg['Subject'] = self.emailmsg['Subject']
            msg['From'] = self.emailmsg['From']
            msg['To'] = ' '.join(emaillist)
            content = "%s \n%s" % (self.emailmsg['Body'], clowderurl)
            content = MIMEText(content.encode('utf-8'), _charset='utf-8')
            msg.attach(content)

            try:
                logger.debug("send email notification to %s, %s " % (emaillist, msg.as_string()))
                server.sendmail(msg['From'], emaillist, msg.as_string())
            except:
                logger.warning("failed to send email notification to %s" % emaillist)
                pass
            server.quit()

    def listen(self):
        """Listen for incoming messages.

         This function will not return until all messages are processed, or the process is
         interrupted.
         """
        pass

    def alive(self):
        """Return whether connection is still alive or not."""
        return True

    def _build_resource(self, body, host, secret_key, clowder_version):
        """Examine message body and create resource object based on message type.

        Example FILE message -- *.file.#
        {   "filename":         name of the triggering file without path,
            "id":               UUID of the triggering file
            "intermediateId":   UUID of the triggering file (deprecated)
            "datasetId":        UUID of dataset that holds the file
            "host":             URL of Clowder host; can include things like 'localhost' from browser address bar
            "secretKey":        API secret key for Clowder host
            "fileSize":         file size in bytes
            "flags":            any additional flags
        }

        Example DATASET message -- *.dataset.file.#
        {   "id":               UUID of the triggering file
            "intermediateId":   UUID of the triggering file (deprecated)
            "datasetId":        UUID of dataset that holds the file
            "host":             URL of Clowder host; can include things like 'localhost' from browser address bar
            "secretKey":        API secret key for Clowder host
            "fileSize":         file size in bytes
            "flags":            any additional flags
        }

        Example METADATA message -- *.metadata.#
        {   "resourceType":     what type of object metadata was added to; 'file' or 'dataset'
            "resourceId":       UUID of the triggering resource (file or dataset)
            "metadata":         actual metadata that was added or removed
            "id":               UUID of the triggering file (blank for 'dataset' type)
            "intermediateId":   (deprecated)
            "datasetId":        UUID of the triggering dataset (blank for 'file' type)
            "host":             URL of Clowder host; can include things like 'localhost' from browser address bar
            "secretKey":        API secret key for Clowder host
            "fileSize":         file size in bytes
            "flags":            any additional flags
        }
        """

        logger = logging.getLogger(__name__)

        # See docstring for information about these fields
        fileid = body.get('id', '')
        intermediatefileid = body.get('intermediateId', '')
        datasetid = body.get('datasetId', '')
        filename = body.get('filename', '')

        # determine resource type; defaults to file
        resource_type = "file"
        message_type = body['routing_key']
        if message_type.find(".dataset.") > -1:
            resource_type = "dataset"
        elif message_type.find(".file.") > -1:
            resource_type = "file"
        elif message_type.find("metadata.added") > -1:
            resource_type = "metadata"
        elif message_type == "extractors." + self.extractor_name \
                or message_type == "extractors." + self.extractor_info['name']:
            # This was a manually submitted extraction
            if datasetid == fileid:
                resource_type = "dataset"
            else:
                resource_type = "file"
        elif message_type.endswith(self.extractor_info['name']) or message_type.endswith(self.extractor_name):
            # This was migrated from another queue (e.g. error queue) so use extractor default
            for key, value in self.extractor_info['process'].items():
                if key == "dataset":
                    resource_type = "dataset"
                else:
                    resource_type = "file"

        # determine what to download (if needed) and add relevant data to resource
        if resource_type == "dataset":
            try:
                datasetinfo = pyclowder.datasets.get_info(self, host, secret_key, datasetid)
                filelist = pyclowder.datasets.get_file_list(self, host, secret_key, datasetid)
                triggering_file = None
                for f in filelist:
                    if f['id'] == fileid:
                        triggering_file = f['filename']
                        break

                return {
                    "type": "dataset",
                    "id": datasetid,
                    "name": datasetinfo["name"],
                    "files": filelist,
                    "triggering_file": triggering_file,
                    "parent": {},
                    "dataset_info": datasetinfo
                }
            except:
                msg = "[%s] : Error downloading dataset preprocess information." % datasetid
                logger.exception(msg)
                # Can't create full resource object but can provide essential details for status_update
                resource = {
                    "type": "dataset",
                    "id": datasetid
                }
                self.message_error(resource)
                return None

        elif resource_type == "file":
            ext = os.path.splitext(filename)[1]
            if clowder_version == 2:
                return {
                    "type": "file",
                    "id": fileid,
                    "intermediate_id": intermediatefileid,
                    "name": filename,
                    "file_ext": ext,
                    "parent": {"type": "dataset",
                               "id": datasetid}
                }
            else:
                return {
                    "type": "file",
                    "id": fileid,
                    "intermediate_id": intermediatefileid,
                    "name": filename,
                    "file_ext": ext,
                    "parent": {"type": "dataset",
                               "id": datasetid}
                }

        elif resource_type == "metadata":
            return {
                "type": "metadata",
                "id": body['resourceId'],
                "parent": {"type": body['resourceType'],
                           "id": body['resourceId']},
                "metadata": body['metadata']
            }

    def _check_for_local_file(self, file_metadata):
        """ Try to get pointer to locally accessible copy of file for extractor."""

        # first check if file is accessible locally
        if 'filepath' in file_metadata:
            file_path = file_metadata['filepath']

            # first simply check if file is present locally
            if os.path.isfile(file_path):
                return file_path

            # otherwise check any mounted paths...
            if len(self.mounted_paths) > 0:
                for source_path in self.mounted_paths:
                    if file_path.startswith(source_path):
                        return file_path.replace(source_path, self.mounted_paths[source_path])

        return None

    def _download_file_metadata(self, host, secret_key, fileid, filepath):
        """Download metadata for a file into a temporary _metadata.json file.

        Returns:
            (tmp directory created, tmp file created)
        """
        file_md = pyclowder.files.download_metadata(self, host, secret_key, fileid)
        md_name = os.path.basename(filepath)+"_metadata.json"

        md_dir = tempfile.mkdtemp(suffix=fileid)
        (fd, md_file) = tempfile.mkstemp(suffix=md_name, dir=md_dir)

        with os.fdopen(fd, "wb") as tmp_file:
            tmp_file.write(json.dumps(file_md))

        return (md_dir, md_file)

    def _prepare_dataset(self, host, secret_key, resource):
        logger = logging.getLogger(__name__)

        file_paths = []
        located_files = []
        missing_files = []
        tmp_files_created = []
        tmp_dirs_created = []

        # Create a temporary folder to hold any links to local files we may need
        temp_link_dir = tempfile.mkdtemp()
        tmp_dirs_created.append(temp_link_dir)

        # first check if any files in dataset accessible locally
        ds_file_list = pyclowder.datasets.get_file_list(self, host, secret_key, resource["id"])
        for ds_file in ds_file_list:
            file_path = self._check_for_local_file(ds_file)
            if not file_path:
                missing_files.append(ds_file)
            else:
                # Create a link to the original file if the "true" name of the file doesn't match what's on disk
                if not file_path.lower().endswith(ds_file['filename'].lower()):
                    ln_name = os.path.join(temp_link_dir, ds_file['filename'])
                    os.symlink(file_path, ln_name)
                    tmp_files_created.append(ln_name)
                    file_path = ln_name

                # Also get file metadata in format expected by extrator
                (file_md_dir, file_md_tmp) = self._download_file_metadata(host, secret_key, ds_file['id'],
                                                                          ds_file['filepath'])
                located_files.append(file_path)
                located_files.append(file_md_tmp)
                tmp_files_created.append(file_md_tmp)
                tmp_dirs_created.append(file_md_dir)

        # If only some files found locally, check & download any that were missed
        if len(located_files) > 0:
            for ds_file in missing_files:
                # Download file to temp directory
                inputfile = pyclowder.files.download(self, host, secret_key, ds_file['id'], ds_file['id'],
                                                     ds_file['file_ext'], tracking=False)
                # Also get file metadata in format expected by extractor
                (file_md_dir, file_md_tmp) = self._download_file_metadata(host, secret_key, ds_file['id'],
                                                                          ds_file['filepath'])
                located_files.append(inputfile)
                located_files.append(file_md_tmp)
                tmp_files_created.append(inputfile)
                tmp_files_created.append(file_md_tmp)
                tmp_dirs_created.append(file_md_dir)

            # Also, get dataset metadata (normally included in dataset .zip download file)
            ds_md = pyclowder.datasets.download_metadata(self, host, secret_key, resource["id"])
            md_name = "%s_dataset_metadata.json" % resource["id"]
            md_dir = tempfile.mkdtemp(suffix=resource["id"])
            (fd, md_file) = tempfile.mkstemp(suffix=md_name, dir=md_dir)
            with os.fdopen(fd, "wb") as tmp_file:
                tmp_file.write(json.dumps(ds_md))
            located_files.append(md_file)
            tmp_files_created.append(md_file)
            tmp_dirs_created.append(md_dir)

            file_paths = located_files

        # If we didn't find any files locally, download dataset .zip as normal
        else:
            try:
                inputzip = pyclowder.datasets.download(self, host, secret_key, resource["id"])
                file_paths = pyclowder.utils.extract_zip_contents(inputzip)
                tmp_files_created += file_paths
                tmp_files_created.append(inputzip)
            except Exception as e:
                logger.exception("No files found and download failed")

        return (file_paths, tmp_files_created, tmp_dirs_created)

    # pylint: disable=too-many-branches,too-many-statements
    def _process_message(self, body):
        """The actual processing of the message.

        This will call check_message to see if the message should be processed and if the
        file should be downloaded. Finally it will call the actual process_message function.
        """

        logger = logging.getLogger(__name__)
        emailaddrlist = None
        if body.get('notifies'):
            emailaddrlist = body.get('notifies')
            logger.debug(emailaddrlist)
        # source_host is original from the message, host is remapped to CLOWDER_URL if given
        source_host = body.get('host', '')
        host = self.clowder_url if self.clowder_url else source_host
        if host == '' or source_host == '':
            logging.error("Host is empty, this is bad.")
            return
        if not source_host.endswith('/'): source_host += '/'
        if not host.endswith('/'): host += '/'
        secret_key = body.get('secretKey', '')
        retry_count = 0 if 'retry_count' not in body else body['retry_count']
        clowder_version = int(body.get('clowderVersion', os.getenv('CLOWDER_VERSION', '1')))
        resource = self._build_resource(body, host, secret_key, clowder_version)
        if not resource:
            logging.error("No resource found, this is bad.")
            return

        # tell everybody we are starting to process the file
        self.status_update(pyclowder.utils.StatusMessage.start, resource, "Started processing.")

        # checks whether to process the file in this message or not
        # pylint: disable=too-many-nested-blocks
        try:
            check_result = pyclowder.utils.CheckMessage.download
            if self.check_message:
                check_result = self.check_message(self, source_host, secret_key, resource, body)
            if check_result != pyclowder.utils.CheckMessage.ignore:
                if self.process_message:

                    # FILE MESSAGES ---------------------------------------
                    if resource["type"] == "file":
                        file_path = None
                        found_local = False
                        try:
                            if check_result != pyclowder.utils.CheckMessage.bypass:
                                file_metadata = pyclowder.files.download_info(self, host, secret_key, resource["id"])
                                file_path = self._check_for_local_file(file_metadata)
                                if not file_path:
                                    file_path = pyclowder.files.download(self, host, secret_key, resource["id"],
                                                                         resource["intermediate_id"],
                                                                         resource["file_ext"],
                                                                         tracking=False)
                                else:
                                    found_local = True
                                resource['local_paths'] = [file_path]

                            self.process_message(self, source_host, secret_key, resource, body)

                            clowderurl = "%sfiles/%s" % (source_host, body.get('id', ''))
                            # notification of extraction job is done by email.
                            self.email(emailaddrlist, clowderurl)
                        finally:
                            if file_path is not None and not found_local:
                                try:
                                    os.remove(file_path)
                                except OSError:
                                    logger.exception("Error removing download file")

                    # DATASET/METADATA MESSAGES ---------------------------------------
                    else:
                        file_paths, tmp_files, tmp_dirs = [], [], []
                        try:
                            if check_result != pyclowder.utils.CheckMessage.bypass:
                                (file_paths, tmp_files, tmp_dirs) = self._prepare_dataset(host, secret_key, resource)
                            resource['local_paths'] = file_paths

                            self.process_message(self, source_host, secret_key, resource, body)
                            clowderurl = "%sdatasets/%s" % (source_host, body.get('datasetId', ''))
                            # notificatino of extraction job is done by email.
                            self.email(emailaddrlist, clowderurl)
                        finally:
                            for tmp_f in tmp_files:
                                try:
                                    os.remove(tmp_f)
                                except OSError:
                                    logger.exception("Error removing temporary dataset file")
                            for tmp_d in tmp_dirs:
                                try:
                                    os.rmdir(tmp_d)
                                except OSError:
                                    logger.exception("Error removing temporary dataset directory")

            else:
                self.status_update(pyclowder.utils.StatusMessage.skip, resource, "Skipped in check_message")

            self.message_ok(resource)

        except SystemExit as exc:
            message = str.format("sys.exit: {}", str(exc))
            logger.exception("[%s] %s", resource['id'], message)
            self.message_resubmit(resource, retry_count, message)
            raise
        except KeyboardInterrupt:
            message = "keyboard interrupt"
            logger.exception("[%s] %s", resource['id'], message)
            self.message_resubmit(resource, retry_count, message)
            raise
        except GeneratorExit:
            message = "generator exit"
            logger.exception("[%s] %s", resource['id'], message)
            self.message_resubmit(resource, retry_count, message)
            raise
        except subprocess.CalledProcessError as exc:
            message = str.format("Error in subprocess [exit code={}]:\n{}", exc.returncode, exc.output)
            logger.exception("[%s] %s", resource['id'], message)
            self.message_error(resource, message)
        except PyClowderExtractionAbort as exc:
            message = str.format("Aborting message: {}", exc.message)
            logger.exception("[%s] %s", resource['id'], message)
            self.message_error(resource, message)
        except Exception as exc:  # pylint: disable=broad-except
            message = str(exc)
            logger.exception("[%s] %s", resource['id'], message)
            if retry_count < self.max_retry:
                message = "(#%s) %s" % (retry_count+1, message)
                self.message_resubmit(resource, retry_count+1, message)
            else:
                self.message_error(resource, message)

    # pylint: disable=no-self-use
    def status_update(self, status, resource, message):
        """Sends a status message.

        These messages, unlike logger messages, will often be send back to clowder to let
        the instance know the progress of the extractor.

        Keyword arguments:
        status - pyclowder.utils.StatusMessage value
        resource  - descriptor object with {"type", "id"} fields
        message - contents of the status update
        """
        logging.getLogger(__name__).info("[%s] : %s: %s", resource["id"], status, message)

    def message_ok(self, resource, message="Done processing."):
        self.status_update(pyclowder.utils.StatusMessage.done, resource, message)

    def message_error(self, resource, message="Error processing message."):
        self.status_update(pyclowder.utils.StatusMessage.error, resource, message)

    def message_resubmit(self, resource, retry_count, message="Resubmitting message."):
        self.status_update(pyclowder.utils.StatusMessage.retry, resource, message)

    def message_process(self, resource, message):
        self.status_update(pyclowder.utils.StatusMessage.processing, resource, message)

    def get(self, url, params=None, raise_status=True, **kwargs):
        """
        This methods wraps the Python requests GET method
        :param url: URl to use in GET request
        :param params: (optional) GET request parameters
        :param raise_status: (optional) If set to True, call raise_for_status. Default is True.
        :param kwargs: List of other optional arguments to pass to GET call
        :return: Response of the GET request
        """

        response = requests.get(url, params=params, **kwargs)
        if raise_status:
            response.raise_for_status()

        return response

    def post(self, url, data=None, json_data=None, raise_status=True, **kwargs):
        """
        This methods wraps the Python requests POST method
        :param url: URl to use in POST request
        :param data: (optional) data (Dictionary, bytes, or file-like object) to send in the body of POST request
        :param json_data: (optional) json data to send with POST request
        :param raise_status: (optional) If set to True, call raise_for_status. Default is True.
        :param kwargs: List of other optional arguments to pass to POST call
        :return: Response of the POST request
        """

        response = requests.post(url, data=data, json=json_data, **kwargs)
        if raise_status:
            response.raise_for_status()

        return response

    def patch(self, url, data=None, json_data=None, raise_status=True, **kwargs):
        """
        This methods wraps the Python requests PATCH method
        :param url: URl to use in PATCH request
        :param data: (optional) data (Dictionary, bytes, or file-like object) to send in the body of PATCH request
        :param json_data: (optional) json data to send with PATCH request
        :param raise_status: (optional) If set to True, call raise_for_status. Default is True.
        :param kwargs: List of other optional arguments to pass to PATCH call
        :return: Response of the PATCH request
        """

        response = requests.patch(url, data=data, json=json_data, **kwargs)
        if raise_status:
            response.raise_for_status()

        return response

    def put(self, url, data=None, raise_status=True, **kwargs):
        """
        This methods wraps the Python requests PUT method
        :param url: URl to use in PUT request
        :param data: (optional) data to send with PUT request
        :param raise_status: (optional) If set to True, call raise_for_status. Default is True.
        :param kwargs: List of other optional arguments to pass to PUT call
        :return: Response of the PUT request
        """

        response = requests.put(url, data=data, **kwargs)
        if raise_status:
            response.raise_for_status()

        return response

    def delete(self, url, raise_status=True, **kwargs):
        """
        This methods wraps the Python requests DELETE method
        :param url: URl to use in DELETE request
        :param raise_status: (optional) If set to True, call raise_for_status. Default is True.
        :param kwargs: List of other optional arguments to pass to DELETE call
        :return: Response of the DELETE request
        """

        response = requests.delete(url, **kwargs)
        if raise_status:
            response.raise_for_status()

        return response


# pylint: disable=too-many-instance-attributes
class RabbitMQConnector(Connector):
    """Listens for messages on RabbitMQ.

    This will connect to rabbitmq and register the extractor with a queue.
    """

    # pylint: disable=too-many-arguments
    def __init__(self, extractor_name, extractor_info,
                 rabbitmq_uri, rabbitmq_key=None, rabbitmq_queue=None,
                 check_message=None, process_message=None, ssl_verify=True, mounted_paths=None,
                 heartbeat=5*60, clowder_url=None, max_retry=10):
        super(RabbitMQConnector, self).__init__(extractor_name, extractor_info, check_message, process_message,
                                                ssl_verify, mounted_paths, clowder_url, max_retry)
        self.rabbitmq_uri = rabbitmq_uri
        self.rabbitmq_key = rabbitmq_key
        if rabbitmq_queue is None:
            self.rabbitmq_queue = extractor_info['name']
        else:
            self.rabbitmq_queue = rabbitmq_queue
        self.channel = None
        self.connection = None
        self.consumer_tag = None
        self.worker = None
        self.announcer = None
        self.heartbeat = float(heartbeat)

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
        self.channel.queue_declare(queue=self.rabbitmq_queue, durable=True)
        self.channel.queue_declare(queue='error.'+self.rabbitmq_queue, durable=True)

        # start the extractor announcer
        self.announcer = RabbitMQBroadcast(self.rabbitmq_uri, self.extractor_info, self.rabbitmq_queue, self.heartbeat)
        self.announcer.start_thread()

    def listen(self):
        """Listen for messages coming from RabbitMQ"""

        # check for connection
        if not self.channel:
            self.connect()

        # create listener
        self.consumer_tag = self.channel.basic_consume(queue=self.rabbitmq_queue,
                                                       on_message_callback=self.on_message,
                                                       auto_ack=False)

        # start listening
        logging.getLogger(__name__).info("Starting to listen for messages.")
        try:
            # pylint: disable=protected-access
            while self.channel and self.channel.is_open and self.channel._consumer_infos:
                self.channel.connection.process_data_events(time_limit=1)  # 1 second
                if self.worker:
                    self.worker.process_messages(self.channel, self.rabbitmq_queue)
                    if self.worker.is_finished():
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
            if self.channel and self.channel.is_open:
                try:
                    self.channel.close()
                except Exception:
                    logging.getLogger(__name__).exception("Error while closing channel.")
            self.channel = None
            if self.connection and self.connection.is_open:
                try:
                    self.connection.close()
                except Exception:
                    logging.getLogger(__name__).exception("Error while closing connection.")
            if self.announcer:
                self.announcer.stop_thread()

            self.connection = None

    def stop(self):
        """Tell the connector to stop listening for messages."""
        if self.channel:
            self.channel.stop_consuming(self.consumer_tag)

    def alive(self):
        return self.connection is not None

    @staticmethod
    def _decode_body(body, codecs=None):
        if not codecs:
            codecs = ['utf8', 'iso-8859-1']
        # see https://stackoverflow.com/a/15918519
        for i in codecs:
            try:
                return body.decode(i)
            except UnicodeDecodeError:
                pass
        raise ValueError("Cannot decode body")

    def on_message(self, channel, method, header, body):
        """When the message is received this will call the generic _process_message in
        the connector class. Any message will only be acked if the message is processed,
        or there is an exception (except for SystemExit and SystemError exceptions).
        """

        try:
            json_body = json.loads(self._decode_body(body))
            if 'routing_key' not in json_body and method.routing_key:
                json_body['routing_key'] = method.routing_key

            if 'jobid' in json_body:
                job_id = json_body['jobid']
            elif 'job_id' in json_body:
                job_id = json_body['job_id']
            else:
                job_id = None

            self.worker = RabbitMQHandler(self.extractor_name, self.extractor_info, job_id, self.check_message,
                                          self.process_message, self.ssl_verify, self.mounted_paths, self.clowder_url,
                                          method, header, body)
            self.worker.start_thread(json_body)

        except ValueError:
            # something went wrong, move message to error queue and give up on this message immediately
            logging.exception("Error processing message, message moved to error queue")
            properties = pika.BasicProperties(delivery_mode=2, reply_to=header.reply_to)
            channel.basic_publish(exchange='',
                                  routing_key='error.' + self.extractor_name,
                                  properties=properties,
                                  body=body)
            channel.basic_ack(method.delivery_tag)


class RabbitMQBroadcast:
    def __init__(self, rabbitmq_uri, extractor_info, rabbitmq_queue, heartbeat):
        self.active = True
        self.rabbitmq_uri = rabbitmq_uri
        self.extractor_info = extractor_info
        self.rabbitmq_queue = rabbitmq_queue
        self.heartbeat = heartbeat
        self.id = str(uuid.uuid4())
        self.connection = None
        self.channel = None
        self.thread = None

    def start_thread(self):
        parameters = pika.URLParameters(self.rabbitmq_uri)
        self.connection = pika.BlockingConnection(parameters)

        # connect to channel
        self.channel = self.connection.channel()

        # create extractors exchange for fanout
        self.channel.exchange_declare(exchange='extractors', exchange_type='fanout', durable=True)

        self.thread = threading.Thread(target=self.send_heartbeat)
        self.thread.daemon = True
        self.thread.start()

    def stop_thread(self):
        self.thread = None

    def send_heartbeat(self):
        # create the message we will send
        message = {
            'id': self.id,
            'queue': self.rabbitmq_queue,
            'extractor_info': self.extractor_info
        }
        next_heartbeat = time.time()
        while self.thread:
            try:
                self.channel.connection.process_data_events()
                if time.time() >= next_heartbeat:
                    self.channel.basic_publish(exchange='extractors', routing_key='', body=json.dumps(message))
                    next_heartbeat = time.time() + self.heartbeat
            except SystemExit:
                raise
            except KeyboardInterrupt:
                raise
            except GeneratorExit:
                raise
            except Exception:  # pylint: disable=broad-except
                logging.getLogger(__name__).exception("Error while sending heartbeat.")
                sys.exit(-1)
            time.sleep(1)


class RabbitMQHandler(Connector):
    """Simple handler that will process a single message at a time.

    To avoid sharing non-threadsafe channels across threads, this will maintain
    a queue of messages that the super- loop can access and send later.
    """

    def __init__(self, extractor_name, extractor_info, job_id, check_message=None, process_message=None, ssl_verify=True,
                 mounted_paths=None, clowder_url=None, method=None, header=None, body=None, max_retry=10):

        super(RabbitMQHandler, self).__init__(extractor_name, extractor_info, check_message, process_message,
                                              ssl_verify, mounted_paths, clowder_url, max_retry)
        self.method = method
        self.header = header
        self.body = body
        self.job_id = job_id
        self.messages = []
        self.thread = None
        self.finished = False
        self.lock = threading.Lock()

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
        self.thread.daemon = True
        self.thread.start()

    def is_finished(self):
        with self.lock:
            return self.thread and not self.thread.is_alive() and self.finished and len(self.messages) == 0

    def process_messages(self, channel, rabbitmq_queue):
        while self.messages:
            with self.lock:
                msg = self.messages.pop(0)

            # PROCESSING - Standard update message during extractor processing
            if msg["type"] == 'status':
                if self.header.reply_to:
                    properties = pika.BasicProperties(delivery_mode=2, correlation_id=self.header.correlation_id)
                    channel.basic_publish(exchange='',
                                          routing_key=self.header.reply_to,
                                          properties=properties,
                                          body=json.dumps(msg['payload']))

            # DONE - Extractor finished without error
            elif msg["type"] == 'ok':
                channel.basic_ack(self.method.delivery_tag)
                with self.lock:
                    self.finished = True

            # ERROR - Extractor encountered error and message goes to error queue
            elif msg["type"] == 'error':
                properties = pika.BasicProperties(delivery_mode=2, reply_to=self.header.reply_to)
                channel.basic_publish(exchange='',
                                      routing_key='error.' + rabbitmq_queue,
                                      properties=properties,
                                      body=self.body)
                channel.basic_ack(self.method.delivery_tag)
                with self.lock:
                    self.finished = True

            # RESUBMITTING - Extractor encountered error and message is resubmitted to same queue
            elif msg["type"] == 'resubmit':
                jbody = json.loads(self.body)
                jbody['retry_count'] = msg['retry_count']
                if 'routing_key' not in jbody and self.method.routing_key and self.method.routing_key != rabbitmq_queue:
                    jbody['routing_key'] = self.method.routing_key

                properties = pika.BasicProperties(delivery_mode=2, reply_to=self.header.reply_to)
                channel.basic_publish(exchange='',
                                      routing_key=rabbitmq_queue,
                                      properties=properties,
                                      body=json.dumps(jbody))
                channel.basic_ack(self.method.delivery_tag)
                with self.lock:
                    self.finished = True

            else:
                logging.getLogger(__name__).error("Received unknown message type [%s]." % msg["type"])

    def status_update(self, status, resource, message):
        super(RabbitMQHandler, self).status_update(status, resource, message)

        with self.lock:
            # TODO: Remove 'status' from payload later and read from message_type and message in Clowder 2.0
            self.messages.append({"type": "status",
                                  "resource": resource,
                                  "payload": {
                                      "file_id":      resource["id"],
                                      "extractor_id": self.extractor_info['name'],
                                      "job_id":       self.job_id,
                                      "status":       "%s: %s" % (status, message),
                                      "start":        pyclowder.utils.iso8601time(),
                                      "message_type": "%s" % status,
                                      "message":      message
                                  }})

    def message_ok(self, resource, message="Done processing."):
        super(RabbitMQHandler, self).message_ok(resource, message)
        with self.lock:
            self.messages.append({"type": "ok"})

    def message_error(self, resource, message="Error processing message."):
        super(RabbitMQHandler, self).message_error(resource, message)
        with self.lock:
            self.messages.append({"type": "error"})

    def message_resubmit(self, resource, retry_count, message=None):
        if message is None:
            message = "(#%s)" % retry_count
        super(RabbitMQHandler, self).message_resubmit(resource, retry_count, message)
        with self.lock:
            self.messages.append({"type": "resubmit", "retry_count": retry_count})


class HPCConnector(Connector):
    """Takes pickle files and processes them."""

    # pylint: disable=too-many-arguments
    def __init__(self, extractor_name, extractor_info, picklefile, job_id=None,
                 check_message=None, process_message=None, ssl_verify=True, mounted_paths=None, max_retry=10):
        super(HPCConnector, self).__init__(extractor_name, extractor_info, check_message, process_message,
                                           ssl_verify, mounted_paths, max_retry=max_retry)
        self.job_id = job_id
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
                    statusreport['job_id'] = self.job_id
                    statusreport['status'] = "%s: %s" % (status, message)
                    statusreport['start'] = time.strftime('%Y-%m-%dT%H:%M:%S')
                    log.write(json.dumps(statusreport) + '\n')
            except:
                logger.exception("Error: unable to write extractor status to log file")
                raise


class LocalConnector(Connector):
    """
    Class that will handle processing of files locally. Needed for Big Data support.

    This will get the file to be processed from environment variables

    """

    def __init__(self, extractor_name, extractor_info, input_file_path, process_message=None, output_file_path=None, max_retry=10):
        super(LocalConnector, self).__init__(extractor_name, extractor_info, process_message=process_message, max_retry=max_retry)
        self.input_file_path = input_file_path
        self.output_file_path = output_file_path
        self.completed_processing = False

    def listen(self):
        local_parameters = dict()
        local_parameters["inputfile"] = self.input_file_path
        local_parameters["outputfile"] = self.output_file_path

        # Set other parameters to emtpy string
        local_parameters["fileid"] = None
        local_parameters["id"] = None
        local_parameters["host"] = None
        local_parameters["intermediateId"] = None
        local_parameters["fileSize"] = None
        local_parameters["flags"] = None
        local_parameters["filename"] = None
        local_parameters["logfile"] = None
        local_parameters["datasetId"] = None
        local_parameters["secretKey"] = None
        local_parameters["routing_key"] = None

        ext = os.path.splitext(self.input_file_path)[1]
        resource = {
            "type": "file",
            "id": "",
            "intermediate_id": "",
            "name": self.input_file_path,
            "file_ext": ext,
            "parent": dict(),
            "local_paths": [self.input_file_path]
        }

        # TODO: BD-1638 Call _process_message by generating pseudo JSON responses from get method
        self.process_message(self, "", "", resource, local_parameters)
        self.completed_processing = True

    def alive(self):
        return not self.completed_processing

    def stop(self):
        pass

    def get(self, url, params=None, raise_status=True, **kwargs):
        logging.getLogger(__name__).debug("GET: " + url)
        return None

    def post(self, url, data=None, json_data=None, raise_status=True, **kwargs):

        logging.getLogger(__name__).debug("POST: " + url)
        # Handle metadata POST endpoints
        if url.find("/technicalmetadatajson") != -1 or url.find("/metadata.jsonld") != -1:

            json_metadata_formatted_string = json.dumps(json.loads(data), indent=4, sort_keys=True)
            logging.getLogger(__name__).debug(json_metadata_formatted_string)
            extension = ".json"

            # If output file path is not set
            if self.output_file_path is None or self.output_file_path == "":
                # Create json filename from the input filename
                json_filename = self.input_file_path + extension
            else:
                json_filename = str(self.output_file_path)
                if not json_filename.endswith(extension):
                    json_filename += extension

            # Checking permissions using EAFP (Easier to Ask for Forgiveness than Permission) technique
            try:
                json_file = open(json_filename, "w")
            except IOError as e:
                if e.errno == errno.EACCES:
                    logging.getLogger(__name__).exception(
                        "You do not have enough permissions to create the output file " + json_filename)
                else:
                    raise
            else:
                with json_file:
                    json_file.write(json_metadata_formatted_string)
                    logging.getLogger(__name__).debug("Metadata output file path: " + json_filename)

    def put(self, url, data=None, raise_status=True, **kwargs):
        logging.getLogger(__name__).debug("PUT: " + url)
        return None

    def delete(self, url, raise_status=True, **kwargs):
        logging.getLogger(__name__).debug("DELETE: " + url)
        return None


class PyClowderExtractionAbort(Exception):
    """Raise exception that will not be subject to retry attempts (i.e. errors that are expected to fail again).

    Attributes:
        message -- explanation of the error
    """

    def __init__(self, message):
        self.message = message
