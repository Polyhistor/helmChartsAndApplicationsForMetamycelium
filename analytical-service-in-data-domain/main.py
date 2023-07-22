from fastapi import FastAPI
import requests
import json

app = FastAPI()


@app.on_event("startup")
async def startup_event():
    url = "http://localhost/kafka-rest-proxy/consumers/testgroup/"
    headers = {
        'Content-Type': 'application/vnd.kafka.v2+json',
    }
    data = {
        "name": "my_consumer",
        "format": "binary",
        "auto.offset.reset": "earliest",
        "auto.commit.enable": "false"
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))

    if response.status_code != 200:
        raise Exception("Failed to create Kafka consumer: " + response.text)
    else:
        print("Kafka consumer created successfully")

    # The response will contain the base_uri for this consumer instance
    consumer_info = response.json()
    print("Consumer instance URI: " + consumer_info['base_uri'])
