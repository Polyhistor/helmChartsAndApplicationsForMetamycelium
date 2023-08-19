from http.client import HTTPException
from xmlrpc.client import ResponseError
from fastapi import FastAPI, BackgroundTasks
from minio import Minio
import requests
import json
import base64
from prometheus_client import Counter, start_http_server, generate_latest, CONTENT_TYPE_LATEST


# Define a counter metric
kafka_records_consumed = Counter('kafka_records_consumed_total', 'Total Kafka records consumed')

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

    print(consumer_base_url)
    url = consumer_base_url + "/records"
    headers = {"Accept": "application/vnd.kafka.binary.v2+json"}


    def consume_records():
        
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"GET /records/ did not succeed: {response.text}")
        else:
            records = response.json()
            for record in records:
                decoded_key = base64.b64decode(record['key']).decode('utf-8') if record['key'] else None
                decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
                value_obj = json.loads(decoded_value_json)
                print(decoded_key)
                print(value_obj)
                print(f"Consumed record with key {decoded_key} and value ")

                # Increment the counter for each record consumed
                kafka_records_consumed.inc()

    background_tasks.add_task(consume_records)
    return {"status": "Consuming records in the background"}


@app.get("/metrics")
async def get_metrics():
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)