import requests
import json

def create_kafka_consumer(url, headers, data):
    response = requests.post(url, headers=headers, data=json.dumps(data))
    
    if response.status_code == 200:  # Successfully created
        return {
            'base_uri': response.json()['base_uri'],
            'message': 'Kafka consumer created successfully'
        }
    elif response.status_code == 409:  # Already exists
        # Construct base_uri for the existing consumer
        base_uri = f"{url}/instances/{data['name']}"
        return {
            'base_uri': base_uri,
            'message': 'Kafka consumer already exists.'
        }
    else:
        raise Exception(f"Failed to create Kafka consumer. Status Code: {response.status_code}. Error: {response.text}")
