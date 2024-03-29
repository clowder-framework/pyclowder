#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import subprocess
import os

from pyclowder.extractors import Extractor
import pyclowder.files


class TestDatasetExtractor(Extractor):
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
        dataset_id = resource['id']

        # Local file path to file which you want to upload to dataset
        file_path = os.path.join(os.getcwd(), 'test_dataset_extractor_file.txt')

        # Upload a new file to dataset
        file_id = pyclowder.files.upload_to_dataset(connector, host, secret_key, dataset_id, file_path, True)
        if file_id is None:
            logger.error("Error uploading file")
        else:
            logger.info("File uploaded successfully")

        # Get file list under dataset
        file_list = pyclowder.datasets.get_file_list(connector, host, secret_key, dataset_id)
        logger.info("File list : %s", file_list)
        if file_id in list(map(lambda file: file['id'], file_list)):
            logger.info("File uploading and retrieving file list succeeded")
        else:
            logger.error("File uploading/retrieving file list didn't succeed")

        # Download info of dataset
        dataset_info = pyclowder.datasets.get_info(connector, host, secret_key, dataset_id)
        logger.info("Dataset info: %s", dataset_info)
        if dataset_id == dataset_info['id']:
            logger.info("Success in downloading dataset info")
        else:
            logger.error("Error in downloading dataset info")

        # Downloading metadata of dataset
        dataset_metadata = pyclowder.datasets.download_metadata(connector, host, secret_key, dataset_id)
        if dataset_metadata is None:
            logger.info("No metadata found for dataset %s", dataset_id)
        else:
            logger.info("Metadata: %s", dataset_metadata)

        # Upload a preview to dataset
        # Local file path to file which you want to upload to dataset for preview
        preview_file_path = os.path.join(os.getcwd(), 'preview_file.jpeg')
        preview_id = pyclowder.datasets.upload_preview(connector, host, secret_key, dataset_id, preview_file_path, None,
                                                       "image/jpeg", visualization_name="test-dataset-extractor",
                                                       visualization_component_id="basic-image-component")
        if preview_id is None:
            logger.info("Preview upload failed")
        else:
            logger.info("Preview %s uploaded to dataset successfully ", preview_id)

        # Uploaing thumbnail
        thumbnail = "thumbnail.jpeg"
        thumbnail_id = pyclowder.datasets.upload_thumbnail(connector, host, secret_key, dataset_id, thumbnail)
        if thumbnail_id is None:
            logger.info("Error in uploading thumbnail to dataset")
        else:
            logger.error("Success in uploading thumbnail to dataset")




if __name__ == "__main__":
    extractor = TestDatasetExtractor()
    extractor.start()
