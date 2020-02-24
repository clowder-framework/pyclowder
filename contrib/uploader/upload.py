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
parser.add_argument('--rerun', '-r', type=str, required=False, help='dataset id for rerun. Adds new files.')

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
    :param parent_type: can be dataset or folder
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


def rerun():
    """
    If rerun option is used this function will walk the filesystem from the top down starting at the folder
    specified by the user. Any new files and folders will be added to the dataset.

    """
    clowder_files = dict()
    clowder_folders = dict()

    for folder in dataset_api.get_folder_list(args.rerun):
        clowder_folders[(folder['name']).split('/').pop()] = folder['id']

    for file in dataset_api.get_file_list(args.rerun):
        clowder_files[file['filename']] = int(file['size'])

    for folder in args.folder:
        os.chdir(start_dir)
        for root, dirs, files in os.walk(folder, topdown=True):
            for name in dirs:
                if name not in clowder_folders:
                    print('Uploading new folder: %s' % name)
                    if root == folder:
                        add_subfolder(args.rerun, os.path.join(root, name), 'dataset', args.rerun)
                    else:
                        if root.split('/').pop() in clowder_folders:
                            parent_id = clowder_folders[root.split('/').pop()]
                            add_subfolder(parent_id, os.path.join(root, name), 'folder', args.rerun)

            for name in files:
                path = os.path.join(root, name)
                size = os.stat(path).st_size
                if (name not in clowder_files) or (name in clowder_files and clowder_files[name] != size) and \
                        name != ".DS_Store":
                    print('Adding new file: %s' % name)
                    pl = path.split('/')
                    pl.pop()
                    local_folder = pl[-1]
                    dir_path = '/'
                    dir_path = dir_path.join(pl)
                    os.chdir(dir_path)
                    response = dataset_api.add_file(args.rerun, name)
                    if local_folder in clowder_folders:
                        dataset_api.move_file_to_folder(args.rerun, clowder_folders[local_folder], response['id'])


if args.rerun:
    rerun()
else:
    main()