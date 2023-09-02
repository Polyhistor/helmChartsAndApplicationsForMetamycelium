from http.client import HTTPException
from xmlrpc.client import ResponseError
from fastapi import FastAPI, BackgroundTasks, Response
from minio import Minio
import time
import requests
import json
import base64
from prometheus_client import Counter, start_http_server, generate_latest, CONTENT_TYPE_LATEST, Histogram, Gauge
import psutil


# Global variables
SERVICE_NAME = "TELEMETRY_PROCESSOR_SERVICE"
SERVICE_ADDRESS = "http://localhost:8008"

# Definining Metrics
kafka_records_consumed = Counter('kafka_records_consumed_total', 'Total Kafka records consumed')
kafka_data_ingested_records = Counter('kafka_data_ingested_records_total', 'Number of Kafka records ingested')
kafka_data_ingested_bytes = Counter('kafka_data_ingested_bytes_total', 'Number of bytes ingested from Kafka records')
kafka_ingestion_errors = Counter('kafka_ingestion_errors_total', 'Number of errors while ingesting data')
cpu_utilization_gauge = Gauge('service_cpu_utilization_percentage', 'CPU Utilization of the Service')
memory_utilization_gauge = Gauge('service_memory_utilization_bytes', 'Memory (RAM) Utilization of the Service')

KAFKA_PROCESSING_TIME = Histogram('kafka_processing_duration_seconds', 'Time taken for processing kafka messages')
ingestion_latency = Histogram('kafka_ingestion_latency_seconds', 'Time taken from data creation to ingestion in seconds')


app = FastAPI()

consumer_base_url = None 

@app.on_event("startup")
async def startup_event():
    url = "http://localhost/kafka-rest-proxy/consumers/telemetry-data-consumer/"
    headers = {
        'Content-Type': 'application/vnd.kafka.v2+json',
    }
    data = {
        "name": "telemetry-data-consumer",
        "format": "binary",
        "auto.offset.reset": "earliest",
        "auto.commit.enable": "true"
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
        data = {"topics": ["telemetry-data"]} 
        response = requests.post(url, headers=headers, data=json.dumps(data))
        if response.status_code != 204:
            raise Exception(
                "Failed to subscribe consumer to topic: " + response.text)
        
    start_http_server(8001)


@app.on_event("shutdown")
async def shutdown_event():
    url = consumer_base_url
    response = requests.delete(url)
    print(f"Consumer deleted with status code {response.status_code}")


@app.get("/")
async def main_function(): 
    return "welcome to the telemetry processing service"


@app.get("/subscribe-to-telemetry-data")
async def consume_kafka_message(background_tasks: BackgroundTasks):
    if consumer_base_url is None:
        return {"status": "Consumer has not been initialized. Please try again later."}

    url = consumer_base_url + "/records"
    headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

    @KAFKA_PROCESSING_TIME.time()
    def consume_records():
        while True:
            try:
                response = requests.get(url, headers=headers)
                response.raise_for_status()
                records = response.json()

                if not records:  # No more records left
                    break

                for record in records:
                    publish_time = record.get("timestamp", time.time())  # defaulting to current time if no timestamp
                    current_time = time.time()

                    # Calculating ingestion latency
                    latency = current_time - publish_time
                    ingestion_latency.observe(latency)

                    decoded_key = base64.b64decode(record['key']).decode('utf-8') if record['key'] else None
                    decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
                    value_obj = json.loads(decoded_value_json)

                    # Data ingestion metrics
                    kafka_records_consumed.inc()
                    kafka_data_ingested_records.inc()
                    kafka_data_ingested_bytes.inc(len(json.dumps(record)))

                    print(f"Consumed record with key {decoded_key} and value {value_obj}")

            except Exception as e:
                kafka_ingestion_errors.inc()
                raise Exception(f"Error while consuming data: {str(e)}")

            # CPU & Memory Utilization
            cpu_utilization_gauge.set(psutil.cpu_percent())
            memory_utilization_gauge.set(psutil.virtual_memory().used)

    background_tasks.add_task(consume_records)
    return {"status": "Consuming records in the background"}


@app.get("/metrics")
async def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)


