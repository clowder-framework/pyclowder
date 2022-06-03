#!/usr/bin/env python

"""Example extractor based on the clowder code."""

import logging
import subprocess

from pyclowder.extractors import Extractor
from pyclowder.utils import CheckMessage
import pyclowder.files

# workflow specific
from ray import workflow
import ray
import codeflare.pipelines.Datamodel as dm
from typing import List, Tuple
from datetime import datetime
import os
import random
import numpy as np

# simple
from sklearn import datasets
from sklearn import svm


# from sklearn.metrics import accuracy_score, log_loss
# from sklearn.neighbors import KNeighborsClassifier
# from sklearn.svm import SVC, LinearSVC, NuSVC
# from sklearn.tree import DecisionTreeClassifier
# from sklearn.ensemble import RandomForestClassifier, AdaBoostClassifier, GradientBoostingClassifier
# from sklearn.discriminant_analysis import LinearDiscriminantAnalysis
# from sklearn.discriminant_analysis import QuadraticDiscriminantAnalysis

from sklearn.compose import ColumnTransformer


class RaySklearnExtractor(Extractor):
    """Count the number of characters, words and lines in a text file."""
    def __init__(self):
        Extractor.__init__(self)
        
        # print("EXTRACTOR STARTED ************************************************")

        # add any additional arguments to parser
        # self.parser.add_argument('--max', '-m', type=int, nargs='?', default=-1,
        #                                                    help='maximum number (default=-1)')

        # parse command line and load default logging configuration
        self.setup()

        # setup logging for the exctractor
        logging.getLogger('pyclowder').setLevel(logging.DEBUG)
        logging.getLogger('__main__').setLevel(logging.DEBUG)
    
    @workflow.step(catch_exceptions=True, name="load_SVM")
    def load_SVM(self):
        print("IN load")
        iris = datasets.load_iris()
        clf = svm.SVC(gamma=0.001, C=100.)
        params = {"clf": clf, "iris": iris}
        return params

    @workflow.step(catch_exceptions=True)
    def fit_SVM(self, params):
        print("IN fit")
        clf = params[0]["clf"]
        iris = params[0]["iris"]
        clf.fit(iris.data[:-1], iris.target[:-1])
        params = {"clf": clf, "iris": iris}
        return params

    @workflow.step(catch_exceptions=True)
    def pred_SVM(self, params):
        print("IN pred")
        clf = params[0]["clf"]
        iris = params[0]["iris"]
        preds = clf.predict(iris.data[-1:])
        return preds
    
    @workflow.step(catch_exceptions=True)
    def setup_pipeline():
                preprocessor = ColumnTransformer(
                transformers=[
                        ('num', numeric_transformer, numeric_features),
                        ('cat', categorical_transformer, categorical_features)])

                pipeline = dm.Pipeline()
                node_a = dm.EstimatorNode('preprocess', preprocessor)

                k_nn = KNeighborsClassifier(3)
                svc = SVC(kernel="rbf", C=0.025, probability=True)
                nu_svc = NuSVC(probability=True)
                rf = RandomForestClassifier()
                gbc = GradientBoostingClassifier()

                node_0 = dm.EstimatorNode('node_0', k_nn)
                node_1 = dm.EstimatorNode('node_1', svc)
                node_2 = dm.EstimatorNode('node_2', nu_svc)
                node_3 = dm.EstimatorNode('node_3', rf)
                node_4 = dm.EstimatorNode('node_4', gbc)

                pipeline.add_edge(node_a, node_0)
                pipeline.add_edge(node_a, node_1)
                pipeline.add_edge(node_a, node_2)
                pipeline.add_edge(node_a, node_3)
                pipeline.add_edge(node_a, node_4)

    @workflow.step(catch_exceptions=True)
    def faulty_function() -> str:
            if random.random() > 0.01:
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
    
    def make_np_serializable(self, obj):
        '''
        Make Numpy objects JSON Serializable
        '''
        if isinstance(obj, np.integer):
            return int(obj)
        elif isinstance(obj, np.floating):
            return float(obj)
        elif isinstance(obj, np.ndarray):
            return obj.tolist()
        elif isinstance(obj, datetime.datetime):
            return obj.__str__()

    def check_message(self, connector, host, secret_key, resource, parameters):
            return CheckMessage.download

    def process_message(self, connector, host, secret_key, resource, parameters):
        # Process the file and upload the results
        # print("$$$$$$$$$$$$$$$$$$$$$$ IN PROCESS MESSAGE  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

        logger = logging.getLogger(__name__)
        inputfile = resource["local_paths"][0]
        file_id = resource['id']

        # These process messages will appear in the Clowder UI under Extractions.
        connector.message_process(resource, "Loading contents of file...")
        
        
        clfIrisDict = self.load_SVM.step(self)
        clfIrisDict = self.fit_SVM.step(self, clfIrisDict)
        preds = self.pred_SVM.step(self, clfIrisDict)

        # setup workflow
        current_time = datetime.now().time() # get time for unique Workflow ID
        workflowID = f"rayWorkflow-scikitlearn-{current_time}"
        
        # run workflow
        workflow.init() # storage="/extractor"
        preds.run(workflowID)

        # get result
        preds = ray.get(workflow.get_output(workflowID))
        preds = self.make_np_serializable(preds[0][0])


        # Call actual program
        result = subprocess.check_output(['wc', inputfile], stderr=subprocess.STDOUT)
        result = result.decode('utf-8')
        (lines, words, characters, _) = result.split()

        connector.message_process(resource, "Found %s lines and %s words..." % (lines, words))

        # Store results as metadata
        value = datetime.now()
        ray_completed = str(value.strftime('%h %d, %Y @ %H:%M'))
        
        result = {
                'Ray workfloat completed at (datetime)': ray_completed,
                'lines': lines,
                'words': words,
                'characters': int(characters) - 1
        }

        # post metadata to Clowder
        metadata = self.get_metadata(result, 'file', file_id, host)

        # Normal logs will appear in the extractor log, but NOT in the Clowder UI.
        logger.debug(metadata)

        # Upload metadata to original file
        pyclowder.files.upload_metadata(connector, host, secret_key, file_id, metadata)
        # print("$$$$$$$$$$$$$$$$$$$$$$ END OF RAY EXTRACTOR -- PROCESS_MESSAGE()  $$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$$")

if __name__ == "__main__":
    extractor = RaySklearnExtractor()
    extractor.start()
