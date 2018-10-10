#!/usr/bin/env python

from pyclowder.extractors import SimpleExtractor


class SimplePythonExtractor(SimpleExtractor):
    def __init__(self, extraction):
        SimpleExtractor.__init__(self)
        self.extraction = extraction

    def process_file(self, input_file):
        return self.extraction(input_file)
