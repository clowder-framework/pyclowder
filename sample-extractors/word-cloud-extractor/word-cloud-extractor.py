#!/usr/bin/env python

"""Example extractor based on the clowder code."""
import csv
import logging
import json

from pyclowder.extractors import Extractor
import pyclowder.files


class WordCloudExtractor(Extractor):
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
        file_path = resource['local_paths'][0]

        # Initialize a list to store the JSON objects
        json_objects = []

        with open(file_path, 'r') as file:
            # Create a CSV reader object
            csv_reader = csv.reader(file)

            # Skip the header row if it exists
            next(csv_reader, None)

            # Iterate through the rows in the CSV file
            for row_index, row in enumerate(csv_reader):
                if row_index >= 1000:
                    break

                # Create a dictionary with 'terms' and 'count' as keys
                data = {
                    'text': row[0],  # Assuming the term is in the first column
                    'count': int(row[1])  # Assuming the count is in the second column
                }

                # Append the dictionary to the list
                json_objects.append(data)

        # Convert the list of dictionaries to a JSON object
        json_data = json.dumps(json_objects, indent=2)

        # Print the JSON object
        #print(json_data)

        spec = {
          "$schema": "https://vega.github.io/schema/vega/v5.json",
          "description": "A word cloud visualization depicting Vega research paper abstracts.",
          "width": 350,
          "height": 400,
          "padding": 0,
          "data": [
            {
              "name": "table",
              "values": json_data,
              "transform": [
                {
                  "type": "formula",
                  "as": "angle",
                  "expr": "[-45, 0, 45][~~(random() * 3)]"
                },
                {
                  "type": "formula",
                  "as": "text2",
                  "expr": "[datum.text]"
                },
                {
                  "type": "formula",
                  "as": "weight",
                  "expr": "if(datum.text=='VEGA', 600, 300)"
                },
                {
                  "type": "wordcloud",
                  "size": [350, 400],
                  "text": {"field": "text2"},
                  "rotate": {"field": "angle"},
                  "font": "Helvetica Neue, Arial",
                  "fontSize": {"field": "count"},
                  "fontWeight": {"field": "weight"},
                  "fontSizeRange": [12, 56],
                  "padding": 2
                }
              ]
            }
          ],
          "scales": [
            {
              "name": "color",
              "type": "ordinal",
              "domain": {"data": "table", "field": "text"},
              "range": ["#d5a928", "#652c90", "#939597"]
            }
          ],
          "marks": [
            {
              "type": "text",
              "from": {"data": "table"},
              "encode": {
                "enter": {
                  "text": {"field": "text2"},
                  "align": {"value": "center"},
                  "baseline": {"value": "alphabetic"},
                  "fill": {"scale": "color", "field": "text"}
                },
                "update": {
                  "x": {"field": "x"},
                  "y": {"field": "y"},
                  "angle": {"field": "angle"},
                  "fontSize": {"field": "fontSize"},
                  "fillOpacity": {"value": 1}
                },
                "hover": {"fillOpacity": {"value": 0.5}}
              }
            },

          ]
        }

        # Define the path to the text file where you want to save the JSON
        output_file_path = 'spec.json'

        # Write the formatted JSON data to the text file
        with open(output_file_path, 'w') as file:
            file.write(json.dumps(spec, indent=2))

        pyclowder.files.upload_preview(connector, host, secret_key, file_id, output_file_path, "application/json",
                                       "spec.json",
                                       visualization_name="word-cloud-extractor",
                                       visualization_component_id="word-cloud")





if __name__ == "__main__":
    extractor = WordCloudExtractor()
    extractor.start()
