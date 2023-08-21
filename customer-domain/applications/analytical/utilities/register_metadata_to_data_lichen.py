import requests
from utilities import fetch_data_from_minio_and_save

def register_metadata_to_data_lichen(storage_info):
    metadata = fetch_data_from_minio_and_save.fetch_data_from_minio_and_save(storage_info)
    # Send metadata to Data Lichen
    response = requests.post('http://localhost:3000/register', json=metadata)
    if response.status_code == 200:
        print(response.json()['message'])
    else:
        print("Failed to register metadata with Data Lichen.")