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

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)

    def process_message(self, client, resource, parameters):
        # Process the file and upload the results

        logger = logging.getLogger(__name__)
        inputfile = resource["local_paths"][0]
        file_id = resource['id']

        # call actual program
        result = subprocess.check_output(['wc', inputfile], stderr=subprocess.STDOUT)
        result = result.decode('utf-8')
        (lines, words, characters, _) = result.split()

        # store results as metadata
        result = {
            'lines': lines,
            'words': words,
            'characters': characters
        }
        metadata = self.generate_metadata(result, 'file', file_id, client.host)
        logger.debug(metadata)

        # upload metadata
        api = pyclowder.files.FilesApi(client)
        api.add_metadata(file_id, metadata)


if __name__ == "__main__":
    extractor = WordCount()
    extractor.start()
