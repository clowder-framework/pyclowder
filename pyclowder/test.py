import pyclowder.datasets
import pyclowder.files
import requests

CLOWDER_HOST = 'https://pdg.clowderframework.org/'
userkey = 'b81d4590-cd89-416e-8ee4-d0dade8b0c95'

base_headers = {'X-API-key': userkey}
headers = {**base_headers, 'Content-type': 'application/json',
           'accept': 'application/json'}

def find_dataset_if_exists(url, key, dataset_name):
    matching_datasets = []
    url = '%sapi/search?' % (url)

    search_results_request = requests.get(url, headers=headers, params={'query': dataset_name, 'resource_type': 'dataset'})
    search_results = search_results_request.json()
    results = search_results['results']
    print(len(results))
    if len(results) > 0:
        for result in results:
            print(list(result.keys()))
            try:
                current_name = result['name']
                if current_name == dataset_name:
                    matching_datasets.append(result)
            except Exception as e:
                print(e)

    return matching_datasets

dataset_id = '654eb32be4b0db2741710ec9'
datset = find_dataset_if_exists(CLOWDER_HOST, userkey, 'ClientTest')
dataset = pyclowder.datasets.get_info(None, CLOWDER_HOST, userkey, datasetid=dataset_id)
new_dataset = pyclowder.datasets.create_empty(None, CLOWDER_HOST, userkey,"TEST", "JUST A TEST" )
print('here')