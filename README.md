[![DOI](https://zenodo.org/badge/126513159.svg)](https://zenodo.org/badge/latestdoi/126513159)

This repository contains the next generation of pyClowder. This library makes it easier to interact with clowder and
to create extractors.

# Extractor creation

One of the most interesting aspects of Clowder is the ability to extract metadata from any file. This ability is
created using extractors. To make it easy to create these extractors in python we have created a module called clowder.
Besides wrapping often used api calls in convenient python calls, we have also added some code to make it easy to
create new extractors.

## Setup

It is recommended to create a Python virtual environment using the following commands before installing PyClowder.

```shell
python3 -m venv venv
source venv/bin/activate
pip install --upgrade pip
```

Install using pip (for most recent versions see: https://pypi.org/project/pyclowder/):

```
pip install pyclowder==2.7.0
```

Install pyClowder on your system by cloning this repo:

```
git clone https://github.com/clowder-framework/pyclowder.git
cd pyclowder
pip install -r requirements.txt
python setup.py install
```

or directly from GitHub:

```
pip install -r https://raw.githubusercontent.com/clowder-framework/pyclowder/master/requirements.txt git+https://github.com/clowder-framework/pyclowder.git
```

## Quickstart example

See the [README](https://github.com/clowder-framework/pyclowder/tree/master/sample-extractors/wordcount#readme)
in `sample-extractors/wordcount`. Using Docker, no install is required.

## Example Extractor

Following is an example of the WordCount extractor. This example will allow the user to specify from the command line
what connector to use, the number of connectors, you can get a list of the options by using the -h flag on the command
line. This will also read some environment variables to initialize the defaults allowing for easy use of this extractor
in a Docker container.

```
#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import subprocess

from pyclowder.extractors import Extractor
import pyclowder.files


class WordCount(Extractor):
    """Count the number of characters, words and lines in a text file."""
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the extractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def process_message(self, connector, parameters):
        # Process the file and upload the results

        logger = logging.getLogger(__name__)
        inputfile = parameters['inputfile']
        host = parameters['host']
        secret_key = parameters['secretKey']
        file_id = parameters['fileid']

        # call actual program
        result = subprocess.check_output(['wc', inputfile], stderr=subprocess.STDOUT)
        (lines, words, characters, _) = result.split()

        # store results as metadata
        result = {
            'lines': lines,
            'words': words,
            'characters': characters
        }
        metadata = self.get_metadata(result, 'file', file_id, host)
        logger.debug(metadata)

        # upload metadata
        pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)


if __name__ == "__main__":
    extractor = WordCount()
    extractor.start()
```

## Initialization

To create a new extractor you should create a new class based on clowder.Extractor. The extractor should call the super
method from the constructor. This super method will try and load the extractor_info.json, in this file it will find
the extractor name, description as well as the key how to register. The init method will also create a command line
parser that the user can extend. Once the command line arguments are parsed you can call setup, which will initialize
the logger.

```
class WordCount(Extractor):
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the extractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)
```

## Message Processing

Next the extractor should implement one or both of the check_message and process message functions. The check_message
should return one of the values in CheckMessage.

```
    def check_message(self, connector, parameters):
        logging.getLogger(__name__).debug("default check message : " + str(parameters))
        return CheckMessage.download

    def process_message(self, debug, parameters):
        logging.getLogger(__name__).debug("default process message : " + str(parameters))
        pass
```

If you want to send JSON-LD back as metadata you can use the convenience function get_metadata. This will take the
information from the contexts field in extractor_info.json and create a metadata document. It will also do some simple
checks and print a warning if information is in the content that is not part of the context.

## Starting the Extractor

Once the extractor is configured, you can start it using the start method. Based on the command line arguments this
will configure the connectors and start listening/processing messages. The code will only return when the connector
is finished.

**Using --no-bind**
By default, an extractor will bind itself to RabbitMQ queue using both its own name and any file/dataset types listed in
extractor_info.json. This will allow the extractor to be triggered by Clowder events, or directly using its own name.

The --no-bind flag will force the instance of the extractor you are starting to skip binding by the file type(s) in
extractor_info.json, and instead bind only by extractor name. Assuming no other instances overwrite this binding, your
extractor instance will then only be triggered via manual or direct messages (i.e. using extractor name), and not by
upload events in Clowder.

Note however that if any other instances of the extractor are running on the same RabbitMQ queue without --no-bind,
they will still bind by file type as normal regardless of previously existing instances with --no-bind, so use caution
when running multiple instances of one extractor while using --no-bind.

# Connectors

The system has two connectors defined by default. The connectors are used to star the extractors. The connector will
look for messages and call the check_message and process_message of the extractor. The two connectors are
RabbitMQConnector and HPCConnector. Both of these will call the check_message first, and based on the result of this
will ignore the message, download the file and call process_message or bypass the download and just call the
process_message.

## RabbitMQConnector

The RabbitMQ connector connects to a RabbitMQ instance, creates a queue and binds itself to that queue. Any message in
the queue will be fetched and passed to the check_message and process_message. This connector takes three parameters:

* rabbitmq_uri [REQUIRED] : the uri of the RabbitMQ server
* rabbitmq_exchange [OPTIONAL] : the exchange to which to bind the queue

## HPCConnector

The HPC connector will run extractions based on the pickle files that are passed in to the constructor as an argument.
Once all pickle files are processed the extractor will stop. The pickle file is assumed to have one additional
argument, the logfile that is being monitored to send feedback back to clowder. This connector takes a single argument
(which can be list):

* picklefile [REQUIRED] : a single file, or list of files that are the pickled messages to be processed.

## LocalConnector

The Local connector will execute an extractor as a standalone program. This can be used to process files that are
present in a local hard drive. After extracting the metadata, it stores the generated metadata in an output file in the
local drive. This connector takes two arguments:

* --input-file-path [REQUIRED] : Full path of the local input file that needs to be processed.
* --output-file-path [OPTIONAL] : Full path of the output file (.json) to store the generated metadata. If no output
  file path is provided, it will create a new file with the name <input_file_with_extension>.json in the same directory
  as that of the input file.

# Clowder API wrappers

Besides code to create extractors there are also functions that wrap the clowder API. They are broken up into modules
that map to the routes endpoint of clowder, for example /api/files/:id/download will be in the `clowder.files` package.

## utils

The clowder.utils package contains some utility functions that should make it easier to create new code that works as
an extractor or code that interacts with clowder. One of these functions is `setup_logging`, which will initialize the
logging system for you. The logging function takes a single argument that can be None. The argument is either a pointer
to a file that is read with the configuration options.

## files

# Dockerfile

We recommend following the instructions at [clowder/generator](https://github.com/clowder-framework/generator) to build
a Docker image from your Simple Extractor.

You can also use the pyclowder:onbuild Docker image to easily convert your extractor into a docker container. This image
is no longer maintained, so it is recommended to either use the clowder/generator linked above or build your own
Dockerfile by choosing your own base image and installing pyClowder as described below.

**This is deprecated and the onbuild image is no longer maintained**
If you build the extractor as using the pyclowder:onbuild image, you will only need the following Dockerfile

```
FROM clowder/pyclowder:onbuild

ENV MAIN_SCRIPT="wordcount.py"
```

The main piece is the MAIN_SCRIPT which should point to the python file that holds your main function.

If you need additional packages installed, you will need a file called `packages.apt` with in this file a list of all
packages that need to be installed. The docker build process will use this file to install those packages first in
the docker container.

If you need any python packages installed you will need to create file called requirements.txt. If this file exists
the docker build process will use `pip install -r requirements.txt` to install these packages.

To use the latest version of pyClowder we recommend choosing a base image of your choice and install pyClowder by adding
it to the requirements.txt file. An example Dockerfile is below:

```
# Base image
FROM python:3-slim

# Creating workdir
WORKDIR /home/clowder

# Install pyClowder and any other python dependencies
COPY requirements.txt .
RUN pip3 install -r requirements.txt

# Adding necessary code to container under workdir
COPY <MY.CODE>.py extractor_info.json /home/clowder/

# Command to be run when container is run
CMD python3 <MY.CODE>.py
```

## SimpleExtractor

Motivation: design and implement a simple extractor to bridge Python developer and knowledge of PyClowder library. It
requires little effort for Python developers to wrap their python code into Clowder's extractors.

Simple extractors take developer defined main function as input parameter to do extraction and then parse and pack
extraction's output into Simple extractor defined metadata data-struct and submit back to Clowder.

Users' function must have to return a ``dict'' object containing metdata and previews.

```markdown
result = {
'metadata': {},
'previews': [
'filename',
{'file': 'filename'},
{'file': 'filename', 'metadata': {}, 'mimetype': 'image/jpeg'}
]}
```

### Example:

`wordcount-simple-extractor` is the simplest example to illustrate how to wrap existing Python code as a Simple
Extractor.

wordcount.py is regular python file which is defined and provided by Python developers. In the code, wordcount
invoke `wc` command to process input file to extract lines, words, characters. It packs metadata into python dict.

```markdown
import subprocess

def wordcount(input_file):
result = subprocess.check_output(['wc', input_file], stderr=subprocess.STDOUT)
(lines, words, characters, _) = result.split()
metadata = {
'lines': lines,
'words': words,
'characters': characters
}
result = {
'metadata': metadata
}
return result
```

To build wordcount as an extractor docker image, users just simply assign two environment variables in Dockerfile shown
below. EXTRACTION_FUNC is environment variable and has to be assigned as extraction function, where in wordcount.py, the
extraction function is `wordcount`. Environment variable EXTRACTION_MODULE is the name of module file containing the
definition of extraction function.

```markdown
FROM clowder/extractors-simple-extractor:onbuild

ENV EXTRACTION_FUNC="wordcount"
ENV EXTRACTION_MODULE="wordcount"
```


