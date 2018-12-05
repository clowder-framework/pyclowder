#!/usr/bin/env python

import json
import subprocess
import tempfile

from pyclowder.extractors import SimpleExtractor


class RExtractor(SimpleExtractor):
    def process_file(self, input_file):
        with tempfile.NamedTemporaryFile(suffix=".json") as json_file:
            subprocess.check_call(['/home/clowder/launcher.R', input_file, json_file.name])
            return json.load(json_file.file)


RExtractor().start()
