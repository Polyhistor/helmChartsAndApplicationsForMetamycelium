from fastapi import FastAPI
import pandas as pd
import requests
import json

app = FastAPI()

# Load your CSV data
df1 = pd.read_csv('temperature.csv')
df2 = pd.read_csv('precipitation.csv')

# Merge the dataframes on the 'date' column
merged_df = pd.merge(df1, df2, on='date')
cluster_id = None 
base_url = "http://localhost/kafka-rest-proxy"

# Running this on startup 
@app.on_event('startup')
async def startup_event():
    global cluster_id
    response = requests.get(f"{base_url}/v3/clusters")
    serializedData = response.json()
    # set the global cluster id
    cluster_id = serializedData["data"][0]["cluster_id"]
    # return the value to the agent
    return cluster_id


@app.post('/')
def main():
    return "Operational App"

@app.get('/getClusterId')
async def get_cluster_id(): 
    global cluster_id
    response = requests.get(f"{base_url}/v3/clusters")
    serializedData = response.json()
    # set the global cluster id
    cluster_id = serializedData["data"][0]["cluster_id"]
    # return the value to the agent
    return cluster_id


@app.get('/store-operational-data')
async def produce_to_kafka(topic: str = 'domain-weather-operational-data'):
    # Kafka REST Proxy URL for producing messages to a topic
    url = f"${base_url}/kafka-rest-proxy/topics/" + topic
    headers = {
        'Content-Type': 'application/vnd.kafka.json.v2+json',
    }



@app.get('/test')
def produce_test():
    url = "http://localhost/kafka-rest-proxy/topics/test"
    headers = {
        'Content-Type': 'application/vnd.kafka.json.v2+json',
    }
    data = {
        "records": [
            {"value": 'I am a test event'}
        ]
    }
    response = requests.post(url, headers=headers, data=json.dumps(data))
    if response.status_code != 200:
        raise Exception(f"POST /topics/test did not succeed: {response.text}")
