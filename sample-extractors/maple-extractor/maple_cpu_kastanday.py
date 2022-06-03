#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import subprocess

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files

# workflow specific
from datetime import datetime
import os
import sys

# Maple's main
# sys.path.append('MAPLE') -- not necessary in docker container
import maple_workflow


class WordCount(Extractor):
    """Count the number of characters, words and lines in a text file."""
    def __init__(self):
        Extractor.__init__(self)

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def check_message(self, connector, host, secret_key, resource, parameters):
            # check if tif image 
            # download, bypass, ignore
            return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        # Process the file and upload the results

        logger = logging.getLogger(__name__)
        inputfile = resource["local_paths"][0]
        file_id = resource['id']

        # These process messages will appear in the Clowder UI under Extractions.
        connector.message_process(resource, "Loading contents of file...")
        
        print("Loading contents of file..., file_id: ", file_id) # 61a41c945e5855829e5923d8
        print("Loading contents of file..., path: ", inputfile) # /tmp/tmpj9he6836.tif
        #  move that file to 'app/data/input_img'
        filename = inputfile.split('/')[-1]
        os.replace(inputfile, '/app/data/input_img/' + filename)
        
        
        # ---------------------- run workflow ----------------------
        connector.message_process(resource, "Starting main MAPLE processing step...")
        maple_workflow.main(filename)        
        connector.message_process(resource, "Completed MAPLE!")
        finalOutputFiles = os.listdir('/app/data/final_shp')
        
        # setup workflow
        current_time = datetime.now().time() # get time for unique Workflow ID
        

        # Store results as metadata
        value = datetime.now()
        ray_completed = str(value.strftime('%h %d, %Y @ %H:%M'))
        
        result = {
                'Completed at (datetime)': ray_completed,
                'output filenames': str(finalOutputFiles),
        }

        # post metadata to Clowder
        metadata = self.get_metadata(result, 'file', file_id, host)

        # Normal logs will appear in the extractor log, but NOT in the Clowder UI.
        logger.debug(metadata)

        # Upload metadata to original file
        pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)

if __name__ == "__main__":
    extractor = WordCount()
    extractor.start()
