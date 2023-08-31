from http.client import HTTPException
from xmlrpc.client import ResponseError
from fastapi import FastAPI, BackgroundTasks
from minio import Minio
import requests
import json
import base64
from sqlite3 import Error
from io import StringIO
from utilities import ensure_table_exists, insert_into_db, fetch_data_from_minio, fetch_data_from_minio_and_save, create_metadata, save_data_to_sqlite, subscribe_to_kafka_consumer, create_kafka_consumer
from datetime import datetime

app = FastAPI()

SERVICE_ADDRESS = "http://localhost:8005"

# Create two global variables to store the base URLs of each consumer
operational_data_consumer_base_url = None
customer_domain_data_consumer_base_url = None

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
    # Shared configurations
    headers = {
        'Content-Type': 'application/vnd.kafka.v2+json',
    }
    
    data = {
        "format": "binary",
        "auto.offset.reset": "earliest",
        "auto.commit.enable": "false"
    }

    # Consumer for domain-weather-operational-data
    url = "http://localhost/kafka-rest-proxy/consumers/weather-domain-operational-data-consumer/"
    data["name"] = "weather-domain-operational-data-consumer-instance"
    response = create_kafka_consumer.create_kafka_consumer(url, headers, data)
    
    global operational_data_consumer_base_url
    
    operational_data_consumer_base_url = response['base_uri'].replace('http://', 'http://localhost/')
    
    subscribe_to_kafka_consumer.subscribe_to_kafka_consumer(operational_data_consumer_base_url, ["domain-weather-operational-data"])

    # Consumer for customer-domain-data
    url = "http://localhost/kafka-rest-proxy/consumers/customer-domain-data-consumer/"
    data["name"] = "customer-domain-data-consumer-instance"
    response = create_kafka_consumer.create_kafka_consumer(url, headers, data)

    global customer_domain_data_consumer_base_url
    customer_domain_data_consumer_base_url = response['base_uri'].replace('http://', 'http://localhost/')

    subscribe_to_kafka_consumer.subscribe_to_kafka_consumer(customer_domain_data_consumer_base_url, ["customer-domain-data"])


@app.on_event("shutdown")
async def shutdown_event():
    global operational_data_consumer_base_url
    operational_data_consumer_url = operational_data_consumer_base_url
    response = requests.delete(operational_data_consumer_url)
    print(f"Operational data consumer deleted with status code {response.status_code}")

    global customer_domain_data_consumer_base_url
    customer_domain_data_url = customer_domain_data_consumer_base_url
    response = requests.delete(customer_domain_data_url)
    print(f"Domain data consumer deleted with status code {response.status_code}")


@app.get("/")
async def main_function(): 
    return "welcome to the weather domain analytical service"

@app.get("/subscribe-to-operational-data")
async def consume_kafka_message(background_tasks: BackgroundTasks):
    global operational_data_consumer_base_url

    if operational_data_consumer_base_url is None:
        return {"status": "Consumer has not been initialized. Please try again later."}

    url = operational_data_consumer_base_url + "/records"
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


@app.get("/retrieve-data-from-customer-domain")
async def retrieve_data_from_customer_domain(background_tasks: BackgroundTasks):

    def process_records_from_kafka_topic():
        # 1. Listen to the Kafka topic for a new message
        headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

        response = requests.get(customer_domain_data_consumer_base_url + "/records", headers=headers)
        
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
            save_data_to_sqlite.save_data_to_sqlite(data_str, 'weather_domain.db')
        
        print(f"Processed {len(records)} records from Kafka topic and stored in SQLite.")

    # Use background tasks to process records
    background_tasks.add_task(process_records_from_kafka_topic)
    return {"status": "Started processing records from Kafka topic in the background."}





