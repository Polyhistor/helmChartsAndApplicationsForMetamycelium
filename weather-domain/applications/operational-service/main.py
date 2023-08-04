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

# This code below is commented because it's not mandatory to create a topic before the dispatch of the event. If the topic does not exist, the proxy will create it itself. 

# @app.get('/createTopic')  # Change get to post as you are creating a resource.
# async def create_topic(topicName: str= "operational-domain-data"):
#     payload = {
#         "topic_name": topicName,
#     }

#     requestHeader = {
#         'Content-Type': 'application/json'
#     }
    
#     print(f"{base_url}/clusters/{cluster_id}/topics")
#     response = requests.post(f"{base_url}/clusters/{cluster_id}/topics", headers=requestHeader, json=payload)

#     if response.status_code != 200: 
#         return {"error": response.text}

#     return response.status_code # It's usually better to return the response rather than print it


@app.post('/produce')
async def produce_to_kafka(topic: str = 'test'):
    # Kafka REST Proxy URL for producing messages to a topic
    url = f"${base_url}/kafka-rest-proxy/topics/" + topic
    headers = {
        'Content-Type': 'application/vnd.kafka.json.v2+json',
    }

    # Iterate over each row in your dataframe
    for index, row in merged_df.iterrows():
        # Convert the row to a dictionary and then to a string
        message = row.to_dict()
        message_str = str(message)

        # Prepare the data as per the REST Proxy requirements
        data = {
            "records": [
                {"value": message_str}
            ]
        }

        # Produce the message to your Kafka topic
        response = requests.post(url, headers=headers, data=json.dumps(data))

        # If the request failed, raise an exception
        if response.status_code != 200:
            raise Exception(
                f"POST /topics/{topic} did not succeed: {response.text}")

    return {"status": "Data produced successfully!"}


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
