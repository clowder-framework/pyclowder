import argparse
import os
import sys

sys.path.append('../../')

from pyclowder.client import ClowderClient
from pyclowder.datasets import DatasetsApi

clowder_host = "https://clowder.ncsa.illinois.edu/clowder/"

parser = argparse.ArgumentParser(description='Program to create a new dataset and add folders to it in Clowder')
parser.add_argument('--username', '-u', type=str, default=None, help='username(email address) for Clowder')
parser.add_argument('--password', '-p', type=str, default=None, help='password for Clowder')
parser.add_argument('--key', '-k', type=str, default=None, help='API key for Clowder')
parser.add_argument('--folder', '-f', type=str, required=True, nargs='*', help='folders to be uploaded to Clowder')

args = parser.parse_args()

if args.key is None and (args.username is None or args.password is None):
    print('Must input username/password combination or key')
    sys.exit(1)

client = ClowderClient(host=clowder_host, key=args.key, username=args.username, password=args.password)
dataset_api = DatasetsApi(client)
start_dir = os.getcwd()


def add_subfolder(parent_id, folder, parent_type, dataset_id):
    """
    Add a subfolder and it's contents to dataset.

    :param parent_id: id of parent. Called with a dataset id
    :param folder: path of folder being added
    :param parent_type: can be dataset or folder. Called with dataset
    :param dataset_id: id of dataset that folder is in
    """
    try:
        os.chdir(os.path.abspath(folder))
    except FileNotFoundError:
        print("ERROR")
        return

    response = dataset_api.add_folder(dataset_id, os.path.basename(folder), parent_type, parent_id)
    folder_id = response['id']

    files = os.listdir(path=os.getcwd())
    cwd = os.getcwd()

    print("uploading files into %s folder..." % folder)
    for file in files:
        if os.path.isfile(file):
            response = dataset_api.add_file(dataset_id, file)
            file_id = response['id']
            dataset_api.move_file_to_folder(dataset_id, folder_id, file_id)
        elif os.path.isdir(file):
            add_subfolder(folder_id, file, 'folder', dataset_id)
            os.chdir(cwd)


def main():
    """
    Start of program. Creates a dataset for each folder argument and fills in all files and folders by calling
    the add_subfolder function.
    """
    for folder in args.folder:
        os.chdir(start_dir)
        dataset_id = dataset_api.create(os.path.basename(folder))

        for file in os.listdir(path=folder):
            os.chdir(start_dir)
            if os.path.isfile(folder + '/' + file):
                os.chdir(folder)
                dataset_api.add_file(dataset_id, file)
                os.chdir('../../..')
            else:
                add_subfolder(dataset_id, folder + '/' + file, 'dataset', dataset_id=dataset_id)


main()