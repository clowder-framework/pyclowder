#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import subprocess
import json
from typing import Dict

from pyclowder.extractors import Extractor
import pyclowder.files


class ImageAnnotator(Extractor):
    """Count the number of characters, words and lines in a text file."""
    def __init__(self):
        Extractor.__init__(self)

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                          help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def process_message(self, connector, host, secret_key, resource, parameters):
        # Process the file and upload the results

        logger = logging.getLogger(__name__)
        inputfile = resource["local_paths"][0]
        file_id = resource['id']
        print(f"Parameters: {parameters}")

        if 'parameters' in parameters:
            params = None
            logging.info("Received parameters")
            try:
                params = json.loads(parameters['parameters'])
            except TypeError as e:
                print(f"Failed to load parameters, it's not compatible with json.loads().\nError:{e}")
                if type(parameters == Dict):
                    params = parameters['parameters']
        if "IMAGE_ANNOTATIONS" in params:
            image_annotations = params["IMAGE_ANNOTATIONS"]
            logging.info(f"Image annotations: {image_annotations}")

            result = json.loads(image_annotations)

            metadata = self.get_metadata(result, 'file', file_id, host)

            # Normal logs will appear in the extractor log, but NOT in the Clowder UI.
            logger.debug(metadata)

            # Upload metadata to original file
            pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)

if __name__ == "__main__":
    extractor = ImageAnnotator()
    extractor.start()
