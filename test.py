import requests

clowder_url = 'http://localhost:8000/api/v2/'

api_key = 'eyJ1c2VyIjoiYUBhLmNvbSIsImtleSI6Ii02QXVlZVdDekh2NEJUX1NaWEMtQmcifQ.MEHnlKYVz72ku_Z53w1XtX7cAtc'
headers = {'X-API-KEY':api_key}
new_dataset = {'name':'new new dataset', 'description':'new dataset'}

new_dataset_endpoint = clowder_url + 'datasets?license_id="CC BY"'

r = requests.post(new_dataset_endpoint, headers=headers, json=new_dataset)
result = r.json()
dataset_id = result['id']

file_data = {"file": open('test.txt', "rb")}
response = requests.post(
    f"{clowder_url}datasets/{dataset_id}/files",
    headers=headers,
    files=file_data,
)

print(r)