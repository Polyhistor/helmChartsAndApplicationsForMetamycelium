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


@app.post('/')
def main():
    return "Operational App"


@app.post('/produce')
async def produce_to_kafka(topic: str = 'test'):
    # Kafka REST Proxy URL for producing messages to a topic
    url = "http://localhost/kafka-rest-proxy/topics/" + topic
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
