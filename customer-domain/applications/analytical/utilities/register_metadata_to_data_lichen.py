import requests

def register_metadata_to_data_lichen(metadata):
    # Send metadata to Data Lichen
    response = requests.post('http://localhost:3000/register', json=metadata)
    if response.status_code == 200:
        print(response.json()['message'])
    else:
        print("Failed to register metadata with Data Lichen.")