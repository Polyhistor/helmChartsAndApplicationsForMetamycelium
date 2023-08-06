from fastapi import FastAPI, BackgroundTasks
import requests
import json
import base64

app = FastAPI()

consumer_base_url = None


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



import base64
import json

@app.get("/subscribe-to-operational-data")
async def consume_kafka_message(background_tasks: BackgroundTasks):
    if consumer_base_url is None:
        return {"status": "Consumer has not been initialized. Please try again later."}

    print(consumer_base_url)
    url = consumer_base_url + "/records"
    headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

    def decode_base64(data):
        """Decodes base64, padding being optional.

        :param data: Base64 data as an ASCII byte string
        :returns: The decoded byte string.
        """
        missing_padding = len(data) % 4
        if missing_padding != 0:
            data += '=' * (4 - missing_padding)
        return base64.b64decode(data)

    def consume_records():
        response = requests.get(url, headers=headers)
        if response.status_code != 200:
            raise Exception(f"GET /records/ did not succeed: {response.text}")
        else:
            records = response.json()
            for record in records:
                decoded_key = None
                # Decoding the base64 message for the key
                if record['key']:
                    base64_key = record['key']
                    decoded_key = decode_base64(base64_key).decode('utf-8')

                # Decoding the base64 message for the value
                base64_value = record['value']
                decoded_value = decode_base64(base64_value).decode('utf-8')

                value_obj = json.loads(decoded_value)

                print(
                    f"Consumed record with key {decoded_key} and value {value_obj['message']} from topic {record['topic']}")

    background_tasks.add_task(consume_records)
    return {"status": "Consuming records in the background"}






