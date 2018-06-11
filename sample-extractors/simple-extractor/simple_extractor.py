#!/usr/bin/env python

import logging
from pyclowder.extractors import Extractor
import pyclowder.files


class SimpleExtractor(Extractor):
    def __init__(self, extraction):
        Extractor.__init__(self)
        self.extraction = extraction
        self.setup()
        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.INFO)
        self.logger = logging.getLogger('__main__')
        self.logger.setLevel(logging.INFO)

    def process_message(self, connector, host, secret_key, resource, parameters):
        input_file = resource["local_paths"][0]
        file_id = resource['id']
        result = self.extraction(input_file)
        if 'metadata' in result.keys():
            metadata = self.get_metadata(result.get('metadata'), 'file', file_id, host)
            self.logger.info("upload metadata")
            self.logger.debug(metadata)
            pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)
        if 'previews' in result.keys():
            self.logger.info("upload previews")
            for preview in result['previews']:
                if isinstance(preview, basestring):
                    preview = {'file': preview}
                else:
                    continue
                self.logger.info("upload preview")
                pyclowder.files.upload_preview(connector, host, secret_key, file_id, preview.get('file'),
                                               preview.get('metadata'), preview.get('mimetype'))
