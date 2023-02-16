#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import subprocess

from pyclowder.extractors import Extractor
import pyclowder.files


class TestFileExtractor(Extractor):
    """Test the functionalities of an extractor."""
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
        file_id = resource['id']


        # Sample metadata
        sample_metadata = {
            'lines': 10,
            'words': 20,
            'characters': 30
        }
        metadata = self.get_metadata(sample_metadata, 'file', file_id, host)

        # Normal logs will appear in the extractor log, but NOT in the Clowder UI.
        logger.debug(metadata)

        # Upload metadata to original file
        pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)

        # Download metadata of file
        downloaded_metadata = pyclowder.files.download_metadata(connector, host, secret_key, file_id)
        logger.info("Downloaded metadata : %s", downloaded_metadata)
        if sample_metadata in (metadata['contents']for metadata in downloaded_metadata):
            logger.info("Success in uploading and downloading file metadata")
        else:
            logger.error("Error in uploading/downloading file metadata")

        # Download file summary
        file = pyclowder.files.download_summary(connector, host, secret_key, file_id)
        logger.info("File summary: %s", file)
        if file_id == file['id']:
            logger.info("Success in downloading file summary")
        else:
            logger.error("Error in downloading file summary")



if __name__ == "__main__":
    extractor = TestFileExtractor()
    extractor.start()
