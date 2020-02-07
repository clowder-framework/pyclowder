import argparse
import os
import sys

from pyclowder.client import ClowderClient
from pyclowder.datasets import DatasetsApi

clowder_host = "https://clowder.ncsa.illinois.edu/clowder/"

parser = argparse.ArgumentParser(description='Program to create a new dataset and add folders to it in Clowder')
parser.add_argument('--username', '-u', type=str, default=None, help='username(email address) for Clowder')
parser.add_argument('--password', '-p', type=str, default=None, help='password for Clowder')
parser.add_argument('--key', '-k', type=str, default=None, help='API key for Clowder')
parser.add_argument('--folder', '-f', type=str, required=True, nargs='*', help='folders to be uploaded to Clowder')

args = parser.parse_args()

if args.key is None and args.username is None or args.password is None:
    print('Must input username/password combination or key')
    sys.exit(1)

client = ClowderClient(host=clowder_host, key=args.key, username=args.username, password=args.password)
dataset_api = DatasetsApi(client)

response = client.post('/datasets/createempty', {'name': 'new dataset'})
dataset_id = response.get('id')

for folder in args.folder:
    response = client.post('/datasets/%s/newFolder' % dataset_id,
                           {'name': folder, 'parentId': dataset_id, 'parentType': 'dataset'})
    folder_id = response.get('id')

    files = os.listdir(path=folder)

    for file in files:
        response = dataset_api.add_file(dataset_id, folder + '/' + file)
        file_id = response.get('id')
        client.post('/datasets/%s/moveFile/%s/%s' % (dataset_id, folder_id, file_id), {'folderId/': folder_id})
