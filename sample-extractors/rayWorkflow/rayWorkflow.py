#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import subprocess

from pyclowder.extractors import Extractor
import pyclowder.files

# workflow specific
from ray import workflow
from typing import List, Tuple
from datetime import datetime
import random

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

  @workflow.step(catch_exceptions=True)
  def faulty_function() -> str:
      if random.random() > 0.3:
          raise RuntimeError("Sometimes this function times out")
      return "OK"

  @workflow.step
  def handle_errors(result: Tuple[str, Exception]):
      # The exception field will be None on success.
      err = result[1]
      if err:
          return "There was an error: {}".format(err)
      else:
          return "Workflow completed successfully"

  def process_message(self, connector, host, secret_key, resource, parameters):
    # Process the file and upload the results

    logger = logging.getLogger(__name__)
    inputfile = resource["local_paths"][0]
    file_id = resource['id']

    # These process messages will appear in the Clowder UI under Extractions.
    connector.message_process(resource, "Loading contents of file...")

    # get time for unique Workflow ID
    current_time = datetime.now().time()
    print(current_time)

    workflow.init(storage="/tmp/data")
    # `handle_errors` receives a tuple of (result, exception).
    faultyFunc = self.faulty_function.step()
    workflow = self.handle_errors.step(faultyFunc)
    output = workflow.run(f"rayWorkflow-{current_time}")

    connector.message_process(resource, "Workflow completed with message: " % (output))


    # Call actual program
    result = subprocess.check_output(['wc', inputfile], stderr=subprocess.STDOUT)
    result = result.decode('utf-8')
    (lines, words, characters, _) = result.split()

    connector.message_process(resource, "Found %s lines and %s words..." % (lines, words))

    # Store results as metadata
    result = {
        'output': output,
        'lines': lines,
        'words': words,
        'characters': int(characters) - 1
    }
    metadata = self.get_metadata(result, 'file', file_id, host)

    # Normal logs will appear in the extractor log, but NOT in the Clowder UI.
    logger.debug(metadata)

    # Upload metadata to original file
    pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)

if __name__ == "__main__":
  extractor = WordCount()
  extractor.start()
