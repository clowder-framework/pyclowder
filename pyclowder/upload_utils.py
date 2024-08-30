import requests
import pyclowder
import pyclowder.datasets
import pyclowder.files
from pyclowder.client import ClowderClient
from requests_toolbelt.multipart.encoder import MultipartEncoder
from datetime import datetime
import sys
import json
import os
from dotenv import load_dotenv, dotenv_values
from pathlib import Path

env_path = Path('.') / '.env'

config = dotenv_values(dotenv_path=env_path)
userkey = config['key']
clowder_url = 'https://pdg.clowderframework.org'


base_headers = {'X-API-key': userkey}
headers = {**base_headers, 'Content-type': 'application/json',
           'accept': 'application/json'}


def print_report_path():
    report_path = os.getenv('REPORT_PATH')
    print(report_path)

def get_datasets_in_space(space_id):
    all_datasets = pyclowder.datasets.get_datasets(connector=None, host=clowder_url, key=userkey, collectionid=None)
    url = '%s/api/spaces/%s/datasets?key=%s' % (clowder_url, space_id, userkey)
    datasets = requests.get(url)
    return datasets

def get_dataset_files(dataset_id):
    url = '%s/api/datasets/%s/files?key=%s' % (clowder_url, dataset_id, userkey)
    files = requests.get(url)
    return files

def is_file_in_dataset(filename, dataset_id):
    url = '%s/api/datasets/%s/files?key=%s' % (clowder_url, dataset_id, userkey)
    dataset_files = requests.get(url)
    if '/' in filename:
        filename = filename.split('/')[-1]
    for f in dataset_files:
        if f['filename'] == filename:
            return True
    return False

def find_dataset_if_exists(url, key, dataset_name):
    matching_datasets = []
    url = '%s/api/search/' % (clowder_url)
    search_results = requests.get(url, headers=headers, params={'query': dataset_name, 'resource_type': 'dataset'})
    results = search_results['results']
    if len(results) > 0:
        return results
    else:
        return None

def find_collection_if_exists(collection_name, space_id=""):
    url = '%s/api/search?key=%s' % (clowder_url, userkey)
    search_results = requests.get(url, params={'query': collection_name, 'resource_type': 'collection'})
    results = search_results['results']
    if len(results) > 0:
        return results[0]
    else:
        return None

def find_collection_if_exists_for_timespan(timespan, space_id=""):
    timespan_clean = timespan.replace('-', ' ')
    url = '%sapi/search?key=%s' % (clowder_url, userkey)
    search_results = requests.get(url, params={'query': timespan_clean, 'resource_type': 'collection'})
    results = search_results['results']
    if len(results) > 0:
        return results[0]
    else:
        return None

def search_dataset_folders(dataset_id, folder_name, url):
    folder_url = f"{url}/api/datasets/{dataset_id}/folders"
    dataset_folders = requests.get(folder_url, headers=headers)
    result = dataset_folders.json()
    for folder in result:
        current_folder_name = folder['name'].lstrip('/')
        if current_folder_name == folder_name:
            return folder
    return None

def create_or_get_dataset(dataset_name, space_id):
    url = f"{clowder_url}/api/spaces/{space_id}/datasets"
    datasets_in_space = requests.get(url, headers=headers).json()

    ds = None

    if len(datasets_in_space) > 0:
        for dataset in datasets_in_space:
            if dataset["name"] == dataset_name:
                dataset_spaces = dataset["spaces"]
                if space_id in dataset_spaces:
                    return dataset
                else:
                    data = dict()
                    data['space'] = space_id
                    dataset_id = dataset["id"]
                    url = f"{clowder_url}api/spaces/{space_id}/addDatasetsToSpace/{dataset_id}"
                    result = requests.post(url, headers=headers, params=data)
                    return dataset
    if ds is None:
        data = dict()
        data["name"] = dataset_name
        data["description"] = ''
        if space_id:
            data["space"] = [space_id]
        url = f"{clowder_url}/api/datasets/createempty"
        result = requests.post(url, headers=headers, params=data)
        return result

def get_datasets_in_collection(collection_id):
    url = f"{clowder_url}/api/collections/{collection_id}/datasets"
    result = requests.get(url, headers=headers)
    return result

def upload_a_file_to_dataset_with_folder(filepath, dataset_id, folder_name, url):
    uploadedfileid = None
    folder = search_dataset_folders(dataset_id=dataset_id, folder_name=folder_name, url=url)
    print("result of search dataset folders is", folder)
    if folder is None:
        folder_post_url = f"{url}/api/datasets/{dataset_id}/newFolder?key={userkey}"
        payload = json.dumps({'name': folder_name,
                              'parentId': dataset_id,
                              'parentType': "dataset"})
        r = requests.post(folder_post_url, data=payload, headers=base_headers)
        r.raise_for_status()
        folder_id = r.json()["id"]
    else:
        folder_id = folder['id']

    url = '%s/api/uploadToDataset/%s?key=%s&folder_id=%s' % (url, dataset_id, userkey, folder_id)
    if os.path.exists(filepath):
            filename = os.path.basename(filepath)
            m = MultipartEncoder(
                fields={'file': (filename, open(filepath, 'rb')),
                        'folder_id':folder_id}
            )
            try:
                headers = {**base_headers, 'Content-type': m.content_type,
                           'accept': 'application/json'}
                result = requests.post(url, data=m, headers=headers,
                                        verify=False)
                print('upload result is', result)
                uploadedfileid = result.json()['id']
            except Exception as e:
                print('failed to upload file, error')
                print(e)
    else:
        print("unable to upload file %s (not found)", filepath)
    return uploadedfileid

def get_dataset_collections(dataset_id):
    url = f"{clowder_url}/api/datasets/{dataset_id}/collections"
    result = requests.get(url, headers=headers)
    return result

def create_collection(collection_name, space_id):
    data = dict()
    data["name"] = collection_name
    data["description"] = 'data for years'
    if space_id:
        data["space"] = space_id
    url = f"{clowder_url}/api/collections"
    result = requests.post(url, headers=headers, params=data)
    return result

def create_or_get_collection(collection_name, parent_col_id=None, space_id=None):
    col = None
    # TODO fix here
    url = '%s/api/search?key=%s' % (clowder_url, userkey)
    collection_results = requests.get(url, headers=headers, params={'query': collection_name, 'resource_type': 'collection'})

    results = collection_results['results']

    if len(results) > 0:
        for collection in results:
            if collection['collectionname'] == collection_name:
                collection_id = collection['id']
                collection_url = f"{clowder_url}/api/collections/{collection_id}/getParentCollectionIds"
                parent_ids = requests.get(collection_url, headers=headers)
                collection_spaces_url = f"{clowder_url}/api/collections/{collection_id}/spaces"
                spaces = requests.get(collection_spaces_url)['spaces']
                if parent_col_id:
                    if space_id in spaces and parent_col_id in parent_ids:
                        col = collection
                        return col
                else:
                    if space_id in spaces:
                        col = collection
                        return col
    if col is None:
        data = dict()
        data["name"] = collection_name
        data["description"] = ''
        if space_id:
            data["space"] = space_id
        create_url = f"{clowder_url}/api/collections"
        result = requests.post(create_url, headers=headers, params=data)
        result_id = result['id']
        if parent_col_id:
            sub_collection_url = f"{clowder_url}/api/collections/{parent_col_id}/addSubCollection/{result_id}"
            subcollection_result = requests.post(sub_collection_url,headers=headers, params=data)
        return result
    else:
        data = dict()
        data["name"] = collection_name
        data["description"] = ''
        if space_id:
            data["space"] = [space_id]
        create_col_url = f"{clowder_url}/api/collections"
        result = requests.post(create_col_url, headers=headers, params=data)
        result_id = result['id']
        if parent_col_id:
            sub_collection_url = f"{clowder_url}/api/collections/{parent_col_id}/addSubCollection/{result_id}"

            subcollection_result = requests.post(sub_collection_url,
                                               headers=headers, params=data)
        return result

def add_dataset_to_collection(dataset_id, collection_id):
    url = '%s/api/collections/%s/datasets/%s' (clowder_url, collection_id, dataset_id)
    result = requests.post(url, headers=headers)
    return result

def upload_a_file_to_dataset(filepath, dataset_id, clowder_url, user_api):
    url = '%s/api/uploadToDataset/%s?key=%s' % (clowder_url, dataset_id, user_api)
    print('upload URL')
    print(url)
    file_exists = os.path.exists(filepath)
    before = datetime.now()
    if os.path.exists(filepath):
            filename = os.path.basename(filepath)
            m = MultipartEncoder(
                fields={'file': (filename, open(filepath, 'rb'))}
            )
            try:
                print('data',m)
                headers = {**base_headers, 'Content-type': m.content_type,}
                result = requests.post(url, data=m, headers=headers,
                                        verify=False)

                uploadedfileid = result.json()['id']
                return uploadedfileid
            except Exception as e:
                print('failed to upload file, error')
                print(e)
                print(str(datetime.now()))
    else:
        print("unable to upload file %s (not found)", filepath)
    return None