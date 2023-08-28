import requests 
import json

def create_kafka_consumer(url, headers, data):
    try:
        response = requests.post(url, headers=headers, data=json.dumps(data))
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if response.status_code == 409:
            print("Kafka consumer already exists. Fetching its details...")
            # Assuming you have an endpoint to fetch details of an existing consumer.
            # You might need to adjust this to match your setup.
            consumer_detail_response = requests.get(url + data["name"], headers=headers)
            return consumer_detail_response.json()
        else:
            raise Exception("Failed to create Kafka consumer: " + str(e))
    else:
        print("Kafka consumer created successfully")
        print(response.json())
        return response.json()
