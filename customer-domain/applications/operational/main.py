from fastapi import FastAPI
import requests
from minio import Minio
from io import BytesIO
import json
import os
import logging

app = FastAPI()

# Setting up logging
logging.basicConfig(level=logging.INFO)


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
    logging.info("Starting to process and store operational data...")

    # List all JSON files in current directory
    json_files = [f for f in os.listdir() if f.endswith('.json')]
    total_files = len(json_files)
    processed_files = 0

    print(total_files)
    print(json_files)

    # Loop over all JSON files and store them in Minio
    for json_file in json_files:
        try:
            # Read the entire JSON file
            with open(json_file, 'r') as f:
                data = f.read()

            # Convert data to bytes and store in Minio
            data_bytes = data.encode('utf-8')
            data_io = BytesIO(data_bytes)
            minio_bucket_name = "customer-domain-operational-data"
            
            # Check bucket existence and create if not
            if not minio_client.bucket_exists(minio_bucket_name):
                minio_client.make_bucket(minio_bucket_name)
            
            # Put object in Minio
            minio_client.put_object(
                bucket_name=minio_bucket_name,
                object_name=json_file,  # Store with the original filename
                data=data_io,
                length=len(data_bytes),
                content_type='application/json',
            )

            processed_files += 1
            logging.info(f"Processed {processed_files}/{total_files} files.")
        except Exception as e:
            logging.error(f"Error processing file {json_file}. Details: {str(e)}")

    logging.info("Finished processing and storing operational data.")
    return {"status": f"Data loaded from {processed_files}/{total_files} files."}

