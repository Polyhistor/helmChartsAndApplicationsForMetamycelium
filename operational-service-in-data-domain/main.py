from fastapi import FastAPI
import pandas as pd
from confluent_kafka import Producer

app = FastAPI()

# Configure your Kafka producer
# replace with your Kafka bootstrap servers
conf = {'bootstrap.servers': '127.0.0.1:9092/kafka'}
producer = Producer(conf)

# Load your CSV data
df1 = pd.read_csv('temperature.csv')
df2 = pd.read_csv('precipitation.csv')

# Merge the dataframes on the 'date' column
merged_df = pd.merge(df1, df2, on='date')


@app.post('/produce')
async def produce_to_kafka(topic: str):
    # Iterate over each row in your dataframe
    for index, row in merged_df.iterrows():
        # Convert the row to a dictionary and then to a string
        message = row.to_dict()
        message_str = str(message)

        # Produce the message to your Kafka topic
        producer.produce(topic, message_str)

    # Wait for any outstanding messages to be delivered and delivery reports to be received.
    producer.flush()

    return {"status": "Data produced successfully!"}


@app.get('/test')
def produce_test():
    producer.produce('test', 'I am a test event')
