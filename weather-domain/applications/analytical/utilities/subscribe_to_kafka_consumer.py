import requests 
import json

def subscribe_to_kafka_consumer(base_url, topics):
    url = base_url + "/subscription"
    headers = {'Content-Type': 'application/vnd.kafka.v2+json'}
    data = {"topics": topics}
    
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code != 204:
        raise Exception("Failed to subscribe consumer to topic: " + response.text)