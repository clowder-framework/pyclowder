#!/usr/bin/env python

import json
import os

from pyclowder.extractors import SimpleExtractor
import rpy2.robjects as robjects

r_script = os.getenv("R_SCRIPT")
r_function = os.getenv("R_FUNCTION")


class RExtractor(SimpleExtractor):
    def process_file(self, input_file):
        r_result = robjects.r('''
                    if ("%s" != "") {
                        source("%s")
                    }
                    result <- do.call("%s", list("%s"))
                    jsonlite::toJSON(result, auto_unbox=TRUE)
                ''' % (r_script, r_script, r_function, input_file))
        return json.loads(str(r_result))


RExtractor().start()
