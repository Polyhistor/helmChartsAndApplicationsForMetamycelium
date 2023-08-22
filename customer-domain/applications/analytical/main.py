from http.client import HTTPException
from xmlrpc.client import ResponseError
from fastapi import FastAPI, BackgroundTasks
from minio import Minio
import requests
import json
import base64
import sqlite3
import time 
from utilities import ensure_table_exists, insert_into_db, register_metadata_to_data_lichen
from utilities.kafka_rest_proxy_exporter import KafkaRESTProxyExporter
from datetime import datetime
import uuid
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor


app = FastAPI()
FastAPIInstrumentor.instrument_app(app)

# Global variables
SERVICE_NAME = "CUSTOMER_DOMAIN_ANALYTICAL_SERVICE"
SERVICE_ADDRESS = "http://localhost:8000"
consumer_base_url = None

# Setting up the trace provider
trace.set_tracer_provider(TracerProvider())

kafka_exporter = KafkaRESTProxyExporter(topic_name="telemetry-data", rest_proxy_url="http://localhost/kafka-rest-proxy", service_name=SERVICE_NAME, service_address=SERVICE_ADDRESS)
span_processor = BatchSpanProcessor(kafka_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Setting up OpenTelemetry
tracer = trace.get_tracer(__name__)

# Storage info dictionary
storage_info = {}
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


@app.on_event("startup")
async def startup_event():
    url = "http://localhost/kafka-rest-proxy/consumers/customer-domain-operational-data-consumer/"
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
        data = {"topics": ["domain-customer-operational-data"]}  # Adjusted the topic name here
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 204:
            raise Exception(
                "Failed to subscribe consumer to topic: " + response.text)


@app.on_event("shutdown")
async def shutdown_event():
    url = consumer_base_url
    response = requests.delete(url)
    print(f"Consumer deleted with status code {response.status_code}")

    # Shutdown OpenTelemetry
    trace.get_tracer_provider().shutdown()


@app.get("/")
async def main_function(): 
    return "welcome to the customer domain analytical service"  

@app.get("/subscribe-to-operational-data")
async def consume_kafka_message(background_tasks: BackgroundTasks):
    tracer = trace.get_tracer(__name__)

    # Start a new span for this endpoint
    with tracer.start_as_current_span("consume_kafka_message"):
        if consumer_base_url is None:
            return {"status": "Consumer has not been initialized. Please try again later."}

        print(consumer_base_url)
        url = consumer_base_url + "/records"
        headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

        ensure_table_exists.ensure_table_exists('object_storage_address.db')

        # You can use the tracer within the consume_records function to instrument finer details.
        def consume_records():
            with tracer.start_as_current_span("consume_records"):
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


@app.get("/register-data-to-data-lichen")
async def retrieve_and_save_data():
    global storage_info
    print(storage_info)

    if not storage_info:
        raise HTTPException(404, "Storage info not found")

    try:
        register_metadata_to_data_lichen.register_metadata_to_data_lichen()
        return {"status": "Data successfully retrieved and saved to 'customer_data.db'"}
    except ResponseError as err:
        raise HTTPException(status_code=500, detail=f"An error occurred while fetching the data: {err}")


@app.get('/publish-domains-data')
async def publish_domains_data():
    return 'yo'
