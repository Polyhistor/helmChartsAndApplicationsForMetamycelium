import requests 
import json

def create_kafka_consumer(url, headers, data):
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 409:
            print("Kafka consumer already exists. Proceeding...")
        else:
            raise Exception("Failed to create Kafka consumer: " + str(e))
    else:
        print("Kafka consumer created successfully")
        print(response.json())
        return response.json()