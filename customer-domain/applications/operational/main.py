from fastapi import FastAPI
import requests
from minio import Minio
from io import BytesIO
import json
import os
import time

app = FastAPI()

# Constants
cluster_id = None 
kafka_rest_proxy_base_url = "http://localhost/kafka-rest-proxy"
minio_url = "localhost:9001"
minio_acces_key = "minioadmin"
minio_secret_key = "minioadmin"

# Initialize the Minio client
minio_client = Minio(
    minio_url,
    access_key=minio_acces_key,
    secret_key=minio_secret_key,
    secure=False
)

@app.on_event('startup')
async def startup_event():
    global cluster_id
    response = requests.get(f"{kafka_rest_proxy_base_url}/v3/clusters")
    serializedData = response.json()
    cluster_id = serializedData["data"][0]["cluster_id"]
    return cluster_id

@app.post('/')
def main():
    return "Operational App"

@app.get('/get-cluster-id')
async def get_cluster_id(): 
    global cluster_id
    response = requests.get(f"{kafka_rest_proxy_base_url}/v3/clusters")
    serializedData = response.json()
    cluster_id = serializedData["data"][0]["cluster_id"]
    return cluster_id

@app.get("/")
async def main_function(): 
    return "welcome to the customer domain operational service"

def list_json_files(directory='.'):
    """Return a list of all JSON files in the directory."""
    return [f for f in os.listdir(directory) if f.endswith('.json')]

@app.get('/store-operational-data')
async def produce_to_kafka(topic: str = 'domain-customer-operational-data'):
    url = f"{kafka_rest_proxy_base_url}/topics/" + topic
    headers = {
        'Content-Type': 'application/vnd.kafka.json.v2+json',
    }

    # Dispatch the "Data loading started" event
    payload_start = {
        "records": [
            {
                "key": "customer-domain-operational-data-stored",
                "value": {
                    "message": "Data loading started"
                }
            }
        ]
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload_start))
    if response.status_code != 200:
        return {"error": response.text}

    # List all JSON files
    json_files = list_json_files()
    min_io_bucket_name = "customer-domain-operational-data"

    # Check if bucket exists, if not, create one
    bucketExists = minio_client.bucket_exists(min_io_bucket_name)
    if not bucketExists:
        minio_client.make_bucket(min_io_bucket_name)
    else: 
        print("bucket already exists")

    # Iterate through each JSON file and store it in Minio
    for json_file in json_files:
        with open(json_file, 'r') as f:
            data = json.load(f)
        
        # Convert the data to JSON string and encode it
        json_data = json.dumps(data).encode('utf-8')
        json_bytes = BytesIO(json_data)
        min_io_object_name = json_file  # Using the filename as object name in Minio

        minio_client.put_object(
            bucket_name=min_io_bucket_name,
            object_name=min_io_object_name,
            data=json_bytes,
            length=len(json_data),
            content_type='application/json',
        )

    # Dispatch the "Data loading finished" event
    payload_end = {
        "records": [
            {   
                "key" : "customer-domain-operational-data-stored",
                "value": {
                    "message": "Data loading finished",
                     "distributedStorageAddress" : minio_url,
                     "minio_access_key": minio_acces_key,
                     "minio_secret_key" : minio_secret_key,
                     "bucket_name": min_io_bucket_name
                }
            }
        ]
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload_end))
    if response.status_code != 200:
        return {"error": response.text}

    return {"status": "Data loaded and events dispatched"}
