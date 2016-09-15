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
import sys
import time

import pika
import requests

import pyclowder.files
from pyclowder.utils import CheckMessage


class Connector(object):
    """ Class that will listen for messages.

     Once a message is received this will start the extraction process. It is assumed
    that there is only one Connector per thread.
    """

    registered_clowder = list()

    def __init__(self, extractor_name, check_message=None, process_message=None, ssl_verify=True):
        self.extractor_name = extractor_name
        self.check_message = check_message
        self.process_message = process_message
        self.ssl_verify = ssl_verify

    def listen(self):
        """Listen for incoming messages.

         This function will not return until all messages are processed, or the process is
         interrupted.
         """
        pass

    # pylint: disable=too-many-branches,too-many-statements
    def _process_message(self, body):
        """The actual processing of the message.

        This will register the extractor with the clowder instance that the message came from.
        Next it will call check_message to see if the message should be processed and if the
        file should be downloaded. Finally it will call the actual process_message function.
        """

        logger = logging.getLogger(__name__)

        # id of file that was added
        fileid = body['id']
        body['fileid'] = body['id']
        intermediatefileid = body['intermediateId']
        # id of dataset file was added to
        datasetid = body.get('datasetId', '')
        # name of file that was added (only on file messages, NOT dataset messages)
        filename = body.get('filename', '')
        # get & clean clowder connection details
        secret_key = body['secretKey']
        host = body['host']
        if not host.endswith('/'):
            host += '/'
            body['host'] = host
        # TODO CATS-583 error with metadata only
        if host == "":
            return

        # determine what kind of extraction this is (i.e. what to download) and add relevant data to body
        # TODO: Can this be improved by simply checking rabbitmq_key of extractor?
        if filename == '':
            extractor_type = "DATASET"
            # get dataset details and contents so extractor check_message can evaluate
            body['dataset_info'] = pyclowder.datasets.get_info(self, host, secret_key, datasetid)
            body['filelist'] = pyclowder.datasets.get_file_list(self, host, secret_key, datasetid)
            # populate filename field with the file that triggered this message
            for f in body['filelist']:
                if f['id'] == fileid:
                    filename = f['filename']
                    body['filename'] = filename
        else:
            extractor_type = "FILE"
            ext = os.path.splitext(filename)[1]
            body['ext'] = ext

        # register extractor
        url = "%sapi/extractors" % host
        if url not in Connector.registered_clowder:
            Connector.registered_clowder.append(url)
            self.register_extractor("%s?key=%s" % (url, secret_key))

        # tell everybody we are starting to process the file
        # TODO: How to handle these for datasets?
        self.status_update(fileid=fileid, status="Started processing file")


        # checks whether to process the file in this message or not
        # pylint: disable=too-many-nested-blocks
        try:
            check_result = CheckMessage.download
            if self.check_message:
                check_result = self.check_message(self, body)
            if check_result:
                if self.process_message:
                    # PREPARE THE FILE FOR PROCESSING
                    if extractor_type == "FILE":
                        inputfile = None
                        try:
                            if check_result != CheckMessage.bypass:
                                # download file
                                inputfile = pyclowder.files.download(self, host, secret_key,
                                                                     fileid, intermediatefileid, ext)
                                body['inputfile'] = inputfile

                            if self.process_message:
                                self.process_message(self, body)
                        finally:
                            if inputfile is not None:
                                try:
                                    os.remove(inputfile)
                                except OSError:
                                    logger.exception("Error removing download file")
                    # PREPARE THE DATASET FOR PROCESSING
                    else:
                        inputzip = None
                        filelist = []
                        try:
                            if check_result != CheckMessage.bypass:
                                # download dataset
                                inputzip = pyclowder.datasets.download(self, host, secret_key,
                                                                       datasetid)
                                filelist = pyclowder.utils.extract_zip_contents(inputzip)

                            body['files'] = filelist
                            if self.process_message:
                                self.process_message(self, body)
                        finally:
                            if inputzip is not None:
                                try:
                                    os.remove(inputzip)
                                except OSError:
                                    logger.exception("Error removing download zip file")
                            for f in filelist:
                                try:
                                    os.remove(f)
                                except OSError:
                                    logger.exception("Error removing dataset file")
            else:
                self.status_update(fileid=fileid, status="Skipped in check_message")
        except SystemExit as exc:
            status = "sys.exit : " + exc.message
            logger.exception("[%s] %s", fileid, status)
            self.status_update(fileid=fileid, status=status)
            raise
        except SystemError as exc:
            status = "system error : " + exc.message
            logger.exception("[%s] %s", fileid, status)
            self.status_update(fileid=fileid, status=status)
            raise
        except KeyboardInterrupt:
            status = "keyboard interrupt"
            logger.exception("[%s] %s", fileid, status)
            self.status_update(fileid=fileid, status=status)
            raise
        except subprocess.CalledProcessError as exc:
            status = str.format("Error processing [exit code={}]\n{}", exc.returncode, exc.output)
            logger.exception("[%s] %s", fileid, status)
            self.status_update(fileid=fileid, status=status)
        except Exception as exc:  # pylint: disable=broad-except
            status = "Error processing : " + exc.message
            logger.exception("[%s] %s", fileid, status)
            self.status_update(fileid=fileid, status=status)
        finally:
            self.status_update(fileid=fileid, status="Done")

    def register_extractor(self, endpoints):
        """Register extractor info with Clowder.

        This assumes a file called extractor_info.json to be located in either the
        current working directory, or the folder where the main program is started.
        """

        # don't do any work if we wont register the endpoint
        if not endpoints or endpoints == "":
            return

        logger = logging.getLogger(__name__)
        filename = 'extractor_info.json'
        if not os.path.isfile(filename):
            pathname = os.path.abspath(os.path.dirname(sys.argv[0]))
            filename = os.path.join(pathname, 'extractor_info.json')

        headers = {'Content-Type': 'application/json'}
        try:
            with open(filename) as info_file:
                info = json.load(info_file)
                info["name"] = self.extractor_name
                for url in endpoints.split(','):
                    if url not in Connector.registered_clowder:
                        Connector.registered_clowder.append(url)
                        result = requests.post(url.strip(), headers=headers,
                                               data=json.dumps(info),
                                               verify=self.ssl_verify)
                        logger.debug("Registering extractor with %s : %s", url, result.text)
        except Exception as exc:  # pylint: disable=broad-except
            logger.error('Error in registering extractor: ' + str(exc))

    # pylint: disable=no-self-use
    def status_update(self, status, fileid):
        """Sends a status message.

        These messages, unlike logger messages, will often be send back to clowder to let
        the instance know the progress of the extractor.
        """
        logging.getLogger(__name__).info("[%s] : %s", fileid, status)


# pylint: disable=too-many-instance-attributes
class RabbitMQConnector(Connector):
    """Listens for messages on RabbitMQ.

    This will connect to rabbitmq and register the extractor with a queue. If the exchange
    and key are specified it will bind the exchange to the queue. If an exchange is
    specified it will always try to bind the special key extractors.<extractor_name> to the
    exchange and queue.
    """

    # pylint: disable=too-many-arguments
    def __init__(self, extractor_name, rabbitmq_uri, rabbitmq_exchange=None, rabbitmq_key=None,
                 check_message=None, process_message=None, ssl_verify=True):
        Connector.__init__(self, extractor_name, check_message, process_message, ssl_verify)
        self.rabbitmq_uri = rabbitmq_uri
        self.rabbitmq_exchange = rabbitmq_exchange
        self.rabbitmq_key = rabbitmq_key
        self.channel = None
        self.connection = None
        self.consumer_tag = None
        self.body = None
        self.method = None
        self.header = None

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
        self.channel.queue_declare(queue=self.extractor_name, durable=True)

        # register with an exchange
        if self.rabbitmq_exchange:
            # declare the exchange in case it does not exist
            self.channel.exchange_declare(exchange=self.rabbitmq_exchange, exchange_type='topic',
                                          durable=True)

            # connect queue and exchange
            if self.rabbitmq_key:
                if isinstance(self.rabbitmq_key, str):
                    self.channel.queue_bind(queue=self.extractor_name,
                                            exchange=self.rabbitmq_exchange,
                                            routing_key=self.rabbitmq_key)
                else:
                    for key in self.rabbitmq_key:
                        self.channel.queue_bind(queue=self.extractor_name,
                                                exchange=self.rabbitmq_exchange,
                                                routing_key=key)

            self.channel.queue_bind(queue=self.extractor_name,
                                    exchange=self.rabbitmq_exchange,
                                    routing_key="extractors." + self.extractor_name)

    def listen(self):
        """Listen for messages coming from RabbitMQ"""

        # check for connection
        if not self.channel:
            self.connect()

        # create listener
        self.consumer_tag = self.channel.basic_consume(self.on_message, queue=self.extractor_name,
                                                       no_ack=False)

        # start listening
        logging.getLogger(__name__).info("Starting to listen for messages.")
        try:
            # pylint: disable=protected-access
            while self.channel and self.channel._consumer_infos:
                self.channel.connection.process_data_events(time_limit=1)  # 1 second
        except KeyboardInterrupt:
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
        self.channel.stop_consuming(self.consumer_tag)

    def on_message(self, channel, method, header, body):
        """When the message is received this will call the generic _process_message in
        the connector class. Any message will only be acked if the message is processed,
        or there is an exception (except for SystemExit and SystemError exceptions).
        """

        # store arguments
        self.body = body
        self.method = method
        self.header = header

        try:
            self._process_message(json.loads(body))
            channel.basic_ack(method.delivery_tag)
        finally:
            self.body = None
            self.method = None
            self.header = None

    def status_update(self, status, fileid):
        """Send a status message back using RabbitMQ"""

        statusreport = dict()
        statusreport['file_id'] = fileid
        statusreport['extractor_id'] = self.extractor_name
        statusreport['status'] = status
        statusreport['start'] = time.strftime('%Y-%m-%dT%H:%M:%S')
        properties = pika.BasicProperties(correlation_id=self.header.correlation_id)
        self.channel.basic_publish(exchange='',
                                   routing_key=self.header.reply_to,
                                   properties=properties,
                                   body=json.dumps(statusreport))


class HPCConnector(Connector):
    """Takes pickle files and processes them."""

    # pylint: disable=too-many-arguments
    def __init__(self, extractor_name, picklefile,
                 check_message=None, process_message=None, ssl_verify=True):
        Connector.__init__(self, extractor_name, check_message, process_message, ssl_verify)
        self.picklefile = picklefile
        self.logfile = None
        self.body = None
        self.method = None
        self.header = None

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

    def status_update(self, status, fileid):
        """Store notification on log file with update"""

        logger = logging.getLogger(__name__)
        logger.debug("[%s] : %s", fileid, status)

        if self.logfile and os.path.isfile(self.logfile) is True:
            try:
                with open(self.logfile, 'a') as log:
                    statusreport = dict()
                    statusreport['file_id'] = fileid
                    statusreport['extractor_id'] = self.extractor_name
                    statusreport['status'] = status
                    statusreport['start'] = time.strftime('%Y-%m-%dT%H:%M:%S')
                    log.write(json.dumps(statusreport) + '\n')
            except:
                logger.exception("Error: unable to write extractor status to log file")
                raise
