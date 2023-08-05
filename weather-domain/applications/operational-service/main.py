from fastapi import FastAPI
import pandas as pd
import requests
from minio import Minio
from io import BytesIO
import json

app = FastAPI()

# Load your CSV data
df1 = pd.read_csv('temperature.csv')
df2 = pd.read_csv('precipitation.csv')

# Merge the dataframes on the 'date' column
merged_df = pd.merge(df1, df2, on='date')
cluster_id = None 
base_url = "http://localhost/kafka-rest-proxy"

# Initialize the Minio client
minio_client = Minio(
    "localhost:9001",
    access_key="minioadmin",
    secret_key="minioadmin",
    secure=False
)

# Running this on startup 
@app.on_event('startup')
async def startup_event():
    global cluster_id
    response = requests.get(f"{base_url}/v3/clusters")
    serializedData = response.json()
    # set the global cluster id
    cluster_id = serializedData["data"][0]["cluster_id"]
    # return the value to the agent
    return cluster_id


@app.post('/')
def main():
    return "Operational App"


@app.get('/getClusterId')
async def get_cluster_id(): 
    global cluster_id
    response = requests.get(f"{base_url}/v3/clusters")
    serializedData = response.json()
    # set the global cluster id
    cluster_id = serializedData["data"][0]["cluster_id"]
    # return the value to the agent
    return cluster_id


@app.get('/store-operational-data')
async def produce_to_kafka(topic: str = 'domain-weather-operational-data'):
    # Kafka REST Proxy URL for producing messages to a topic
    url = f"{base_url}/topics/" + topic
    headers = {
        'Content-Type': 'application/vnd.kafka.json.v2+json',
    }

    # Dispatch the "Data loading started" event
    payload_start = {
        "records": [
            {
                "value": {
                    "message": "Data loading started"
                }
            }
        ]
    }

    print(url)
    response = requests.post(url, headers=headers, data=json.dumps(payload_start))

    if response.status_code != 200:
        return {"error": response.text}

    # Store the merged data in Minio
    csv_data = merged_df.to_csv(index=False).encode('utf-8')
    csv_bytes = BytesIO(csv_data)

    bucketExists = minio_client.bucket_exists("weather-domain-operational-data")
    if not bucketExists:
        minio_client.make_bucket("weather-domain-operational-data")
    else: 
        print("bucket already exist")

    minio_client.put_object(
        bucket_name='weather-domain-operational-data',
        object_name='merged_data-v2.csv',
        data=csv_bytes,
        length=len(csv_data),
        content_type='text/csv',
    )

    # Dispatch the "Data loading finished" event
    payload_end = {
        "records": [
            {
                "value": {
                    "message": "Data loading finished"
                }
            }
        ]
    }

    response = requests.post(url, headers=headers, data=json.dumps(payload_end))
    if response.status_code != 200:
        return {"error": response.text}

    return {"status": "Data loaded and events dispatched"}
