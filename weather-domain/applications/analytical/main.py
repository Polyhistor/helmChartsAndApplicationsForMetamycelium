from http.client import HTTPException
from xmlrpc.client import ResponseError
from fastapi import FastAPI, BackgroundTasks
from minio import Minio
import requests
import json
import base64
import sqlite3
from sqlite3 import Error
from io import StringIO
import csv
import time 
from utilities import ensure_table_exists, insert_into_db, fetch_data_from_minio, fetch_data_from_minio_and_save, create_metadata, save_data_to_sqlite
from datetime import datetime

app = FastAPI()

SERVICE_ADDRESS = "http://localhost:8000"

consumer_base_url = None
# Storage info dictionary
storage_info = {}
MINIO_URL = "localhost:9001"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Initialize the Minio client
minio_client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


@app.on_event("startup")
async def startup_event():
    url = "http://localhost/kafka-rest-proxy/consumers/weather-domain-operational-data-consumer/"
    headers = {
        'Content-Type': 'application/vnd.kafka.v2+json',
    }
    data = {
        "name": "operational-data-consumer",
        "format": "binary",
        "auto.offset.reset": "earliest",
        "auto.commit.enable": "false"
    }
    try:
        # attemping to create a consumer 
        response = requests.post(url, headers=headers, data=json.dumps(data))
        # will raise an HTTPError if the status code is 4xx or 5xx
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 409:
            print("Kafka consumer already exists. Proceeding...")
        else:
            raise Exception("Failed to create Kafka consumer: " + str(e))
    else:
        print("Kafka consumer created successfully")
        print(response.json())
        consumer_info = response.json()
        print("Consumer instance URI: " + consumer_info['base_uri'])
        global consumer_base_url
        consumer_base_url = consumer_info['base_uri'].replace(
            'http://', 'http://localhost/')

        # Subscribe the consumer to the topic
        url = consumer_base_url + "/subscription"
        headers = {'Content-Type': 'application/vnd.kafka.v2+json'}
        data = {"topics": ["domain-weather-operational-data"]}  # Adjusted the topic name here
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 204:
            raise Exception(
                "Failed to subscribe consumer to topic: " + response.text)


@app.on_event("shutdown")
async def shutdown_event():
    url = consumer_base_url
    response = requests.delete(url)
    print(f"Consumer deleted with status code {response.status_code}")


@app.get("/")
async def main_function(): 
    return "welcome to the weather domain analytical service"

@app.get("/subscribe-to-operational-data")
async def consume_kafka_message(background_tasks: BackgroundTasks):
    if consumer_base_url is None:
        return {"status": "Consumer has not been initialized. Please try again later."}

    print(consumer_base_url)
    url = consumer_base_url + "/records"
    headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

    ensure_table_exists.ensure_table_exists()

    def consume_records():
        global storage_info
        
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"GET /records/ did not succeed: {response.text}")
        else:
            records = response.json()
            for record in records:
                decoded_key = base64.b64decode(record['key']).decode('utf-8') if record['key'] else None
                decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
                value_obj = json.loads(decoded_value_json)

                storage_info = {
                "distributedStorageAddress": value_obj.get('distributedStorageAddress', ''),
                "minio_access_key": value_obj.get('minio_access_key', ''),
                "minio_secret_key": value_obj.get('minio_secret_key', ''),
                "bucket_name": value_obj.get('bucket_name', ''),
                "object_name": value_obj.get('object_name', '')
                }

                # Insert the storage info into the SQLite database
                insert_into_db.insert_into_db(storage_info)

                print(f"Consumed record with key {decoded_key} and value {value_obj['message']} from topic {record['topic']}")
                if 'distributedStorageAddress' in value_obj:
                    print(f"Distributed storage address: {value_obj['distributedStorageAddress']}")
                    print(f"Minio access key: {value_obj['minio_access_key']}")
                    print(f"Minio secret key: {value_obj['minio_secret_key']}")
                    print(f"Bucket name: {value_obj['bucket_name']}")
                    print(f"Object name: {value_obj['object_name']}")


    background_tasks.add_task(consume_records)
    return {"status": "Consuming records in the background"}


@app.get("/retrieve_and_save_data")
async def retrieve_and_save_data():
    global storage_info
    print(storage_info)
    # Determine the actual time as the current timestamp
    actual_time = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    if not storage_info:
        raise HTTPException(404, "Storage info not found")

    try:
        fetch_data_from_minio_and_save.fetch_data_from_minio_and_save(actual_time)
        return {"status": "Data successfully retrieved and saved to 'weather_data.db'"}
    except ResponseError as err:
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching the data: {err}")



@app.get("/retrieve-data-from-customer-domain")
async def retrieve_data_from_customer_domain(background_tasks: BackgroundTasks):

    def process_records_from_kafka_topic():
        # 1. Listen to the Kafka topic for a new message
        headers = {"Accept": "application/vnd.kafka.binary.v2+json"}
        response = requests.get(consumer_base_url + "/records", headers=headers)
        if response.status_code != 200:
            print(f"Failed to retrieve records from Kafka topic: {response.text}")
            return

        records = response.json()
        for record in records:
            decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
            value_obj = json.loads(decoded_value_json)

            # 2. Extract Minio storage information
            distributed_storage_address = value_obj.get('data_location')
            minio_access_key = MINIO_ACCESS_KEY
            minio_secret_key = MINIO_SECRET_KEY
            bucket_name = value_obj.get('bucket_name', 'custom-domain-analytical-data')
            object_name = value_obj.get('object_name', f"data_object_{value_obj.get('object_id')}.json")

            # 3. Retrieve data from Minio using the storage info
            data_str = fetch_data_from_minio.fetch_data_from_minio(
                distributed_storage_address,
                minio_access_key,
                minio_secret_key,
                bucket_name,
                object_name
            )

            # 4. Save this data to SQLite
            save_data_to_sqlite.save_data_to_sqlite(data_str)
        
        print(f"Processed {len(records)} records from Kafka topic and stored in SQLite.")

    # Use background tasks to process records
    background_tasks.add_task(process_records_from_kafka_topic)
    return {"status": "Started processing records from Kafka topic in the background."}
