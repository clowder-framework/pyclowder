from datetime import datetime
from ray import workflow
import ray

# simple
from sklearn import datasets
from sklearn import svm

class WordCount():
    """Count the number of characters, words and lines in a text file."""
    def __init__(self):
        # ray.shutdown() # ensure not running
        # ray.init()
        # workflow.init(storage="/tmp/data") 
        pass
        
        

    @workflow.step(catch_exceptions=True, name="load_SVM")
    def load_SVM(self):
        print("IN load")
        iris = datasets.load_iris()
        clf = svm.SVC(gamma=0.001, C=100.)
        params = {"clf": clf, "iris": iris}
        return params

    @workflow.step(catch_exceptions=True)
    def fit_SVM(self, something):
        print("IN fit")
        clf = something[0]["clf"]
        iris = something[0]["iris"]
        clf.fit(iris.data[:-1], iris.target[:-1])
        params = {"clf": clf, "iris": iris}
        return params

    @workflow.step(catch_exceptions=True)
    def pred_SVM(self, something):
        print("IN pred")
        clf = something[0]["clf"]
        iris = something[0]["iris"]
        preds = clf.predict(iris.data[-1:])
        return preds
    
    @workflow.step
    def one(self) -> int:
        return 1

    @workflow.step
    def add(self, a: int, b: int) -> int:
        return a + b
    
    
    def process_message(self):
        # Process the file and upload the results

        # get time for unique Workflow ID
        current_time = datetime.now().time()
        workflowID = f"samplePipeline-scikitlearn-{current_time}"
        
        
        # Simple workflow
        # someInt = self.one.step(self)
        # res = self.add.step(self, 100, someInt)
        # assert res.run(workflow_id="kas_run_4") == 101
        # self.add.step(self, 100, self.one.step(self)).run(workflow_id="kas_run_2")
        # output.run(workflow_id="kas_run_1")
        
        # One liner
        # preds = self.pred_SVM.step(self, self.fit_SVM.step(self, self.load_SVM.step(self)))
        # workflow.init(storage="/tmp/data") 
        # preds.run(workflowID)

        # workflow
        clfIrisDict = self.load_SVM.step(self)
        clfIrisDict = self.fit_SVM.step(self, clfIrisDict)
        preds = self.pred_SVM.step(self, clfIrisDict)
        
        workflow.init(storage="/tmp/data") 
        preds.run(workflowID)
        
        preds = ray.get(workflow.get_output(workflowID))
        
        return preds
        
        # after workflow

if __name__ == "__main__":
    extractor = WordCount()
    preds = extractor.process_message()
    
    print("-----------------------HERE ARE THE PREDS FINALLY-----------------------")
    print(preds[0][0])
    
