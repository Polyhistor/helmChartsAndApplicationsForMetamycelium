from fastapi import FastAPI, BackgroundTasks
from minio import Minio
import requests
import json
import time 
import base64
from utilities import ensure_table_exists, insert_into_db, fetch_data_from_minio, save_data_to_sqlite, subscribe_to_kafka_consumer, create_kafka_consumer
from utilities.kafka_rest_proxy_exporter import KafkaRESTProxyExporter
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.trace import SpanKind

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)

# Global variables
SERVICE_ADDRESS = "http://localhost:8005"
SERVICE_NAME = "WEATHER_DOMAIN_ANALYTICAL_SERVICE"
SERVICE_VERSION = "1.0.0"
ENVIRONMENT = "production"
KAFKA_REST_PROXY_URL = "http://localhost/kafka-rest-proxy"
buffered_data = ""

# Create two global variables to store the base URLs of each consumer
operational_data_consumer_base_url = None
customer_domain_data_consumer_base_url = None
data_discovery_consumer_base_url = None

# Setting up the trace provider
trace.set_tracer_provider(TracerProvider())

kafka_exporter = KafkaRESTProxyExporter(topic_name="telemetry-data", rest_proxy_url=KAFKA_REST_PROXY_URL, service_name=SERVICE_NAME, service_address=SERVICE_ADDRESS)
span_processor = BatchSpanProcessor(kafka_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Setting up OpenTelemetry
tracer = trace.get_tracer(__name__)

# Storage info dictionary
storage_info = {}
MINIO_URL = "localhost:9001"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# Initialize the Minio client
minio_client = Minio(
    MINIO_URL,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)


@app.on_event("startup")
async def startup_event():
    # Shared configurations
    headers = {
        'Content-Type': 'application/vnd.kafka.v2+json',
    }
    
    data = {
        "format": "binary",
        "auto.offset.reset": "earliest",
        "auto.commit.enable": "false"
    }

    # Consumer for domain-weather-operational-data
    url = "http://localhost/kafka-rest-proxy/consumers/weather-domain-operational-data-consumer/"
    data["name"] = "weather-domain-operational-data-consumer-instance"
    response = create_kafka_consumer.create_kafka_consumer(url, headers, data)
    
    global operational_data_consumer_base_url
    
    operational_data_consumer_base_url = response['base_uri'].replace('http://', 'http://localhost/')
    
    subscribe_to_kafka_consumer.subscribe_to_kafka_consumer(operational_data_consumer_base_url, ["domain-weather-operational-data"])

    # Consumer for customer-domain-data
    url = "http://localhost/kafka-rest-proxy/consumers/customer-domain-data-consumer/"
    data["name"] = "customer-domain-data-consumer-instance"
    response = create_kafka_consumer.create_kafka_consumer(url, headers, data)

    global customer_domain_data_consumer_base_url
    customer_domain_data_consumer_base_url = response['base_uri'].replace('http://', 'http://localhost/')

    subscribe_to_kafka_consumer.subscribe_to_kafka_consumer(customer_domain_data_consumer_base_url, ["customer-domain-data"])
    
     # Consumer for data-discovery
    url = "http://localhost/kafka-rest-proxy/consumers/data-discovery-consumer/"
    data["name"] = "data-discovery-consumer-instance"
    response = create_kafka_consumer.create_kafka_consumer(url, headers, data)

    global data_discovery_consumer_base_url
    data_discovery_consumer_base_url = response['base_uri'].replace('http://', 'http://localhost/')

    subscribe_to_kafka_consumer.subscribe_to_kafka_consumer(data_discovery_consumer_base_url, ["data-discovery"])

    # Consumer for customer-domain-stream
    url = "http://localhost/kafka-rest-proxy/consumers/customer-domain-stream-consumer/"
    data["name"] = "customer-domain-stream-consumer-instance"
    response = create_kafka_consumer.create_kafka_consumer(url, headers, data)

    global customer_domain_stream_consumer_base_url
    customer_domain_stream_consumer_base_url = response['base_uri'].replace('http://', 'http://localhost/')

    subscribe_to_kafka_consumer.subscribe_to_kafka_consumer(customer_domain_stream_consumer_base_url, ["customer-domain-stream-data"])


@app.on_event("shutdown")
async def shutdown_event():
    global operational_data_consumer_base_url
    operational_data_consumer_url = operational_data_consumer_base_url
    response = requests.delete(operational_data_consumer_url)
    print(f"Operational data consumer deleted with status code {response.status_code}")

    global customer_domain_data_consumer_base_url
    customer_domain_data_url = customer_domain_data_consumer_base_url
    response = requests.delete(customer_domain_data_url)
    print(f"Domain data consumer deleted with status code {response.status_code}")

    global data_discovery_consumer_base_url
    data_discovery_consumer_url = data_discovery_consumer_base_url
    response = requests.delete(data_discovery_consumer_url)
    print(f"Data discovery consumer deleted with status code {response.status_code}")

    global customer_domain_stream_consumer_base_url
    customer_domain_stream_consumer_url = customer_domain_stream_consumer_base_url
    response = requests.delete(customer_domain_stream_consumer_url)
    print(f"Customer domain stream consumer deleted with status code {response.status_code}")


@app.get("/")
async def main_function(): 
    return "welcome to the weather domain analytical service"

@app.get("/subscribe-to-operational-data")
async def consume_kafka_message(background_tasks: BackgroundTasks):
    global operational_data_consumer_base_url

    with tracer.start_as_current_span("consume-kafka-message", kind=SpanKind.SERVER) as span:

        if operational_data_consumer_base_url is None:
            span.set_attribute("error", True)
            span.set_attribute("error_details", "Consumer has not been initialized")
            return {"status": "Consumer has not been initialized. Please try again later."}

        url = operational_data_consumer_base_url + "/records"
        headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

        ensure_table_exists.ensure_table_exists()

        def consume_records():
            global storage_info
            
            response = requests.get(url, headers=headers)
            if response.status_code != 200:
                span.set_attribute("error", True)
                span.set_attribute("error_details", f"GET /records/ did not succeed: {response.text}")
                raise Exception(f"GET /records/ did not succeed: {response.text}")
            else:
                records = response.json()
                for record in records:
                    decoded_key = base64.b64decode(record['key']).decode('utf-8') if record['key'] else None
                    decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
                    value_obj = json.loads(decoded_value_json)

                    storage_info = {
                    "distributedStorageAddress": value_obj.get('distributedStorageAddress', ''),
                    "minio_access_key": value_obj.get('minio_access_key', ''),
                    "minio_secret_key": value_obj.get('minio_secret_key', ''),
                    "bucket_name": value_obj.get('bucket_name', ''),
                    "object_name": value_obj.get('object_name', '')
                    }

                    # Insert the storage info into the SQLite database
                    insert_into_db.insert_into_db(storage_info)

                    span.add_event(f"Consumed record with key {decoded_key} and value {value_obj['message']} from topic {record['topic']}")
                    if 'distributedStorageAddress' in value_obj:
                        span.add_event(f"Distributed storage address: {value_obj['distributedStorageAddress']}")
                        span.add_event(f"Minio access key: {value_obj['minio_access_key']}")
                        span.add_event(f"Minio secret key: {value_obj['minio_secret_key']}")
                        span.add_event(f"Bucket name: {value_obj['bucket_name']}")
                        span.add_event(f"Object name: {value_obj['object_name']}")

        background_tasks.add_task(consume_records)
        span.add_event("Started consuming records in the background")
        
    return {"status": "Consuming records in the background"}

@app.get("/retrieve-data-from-customer-domain")
async def retrieve_data_from_customer_domain(background_tasks: BackgroundTasks):
    
    with tracer.start_as_current_span("retrieve-data-from-customer-domain", kind=SpanKind.SERVER) as span:
        def process_records_from_kafka_topic():
            # 1. Listen to the Kafka topic for a new message
            headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

            response = requests.get(customer_domain_data_consumer_base_url + "/records", headers=headers)
            
            if response.status_code != 200:
                print(f"Failed to retrieve records from Kafka topic: {response.text}")
                span.set_attribute("error", True)
                span.set_attribute("error_details", response.text)
                return

            records = response.json()
            for record in records:
                decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
                value_obj = json.loads(decoded_value_json)

                # 2. Extract Minio storage information
                distributed_storage_address = value_obj.get('data_location')
                minio_access_key = MINIO_ACCESS_KEY
                minio_secret_key = MINIO_SECRET_KEY
                bucket_name = value_obj.get('bucket_name', 'custom-domain-analytical-data')
                object_name = value_obj.get('object_name', f"data_object_{value_obj.get('object_id')}.json")

                # 3. Retrieve data from Minio using the storage info
                data_str = fetch_data_from_minio.fetch_data_from_minio(
                    distributed_storage_address,
                    minio_access_key,
                    minio_secret_key,
                    bucket_name,
                    object_name
                )

                # 4. Save this data to SQLite
                save_data_to_sqlite.save_data_to_sqlite(data_str, 'weather_domain.db')
            
            span.set_attribute("records_processed", len(records))
            print(f"Processed {len(records)} records from Kafka topic and stored in SQLite.")
        
        # Use background tasks to process records
        background_tasks.add_task(process_records_from_kafka_topic)
        span.add_event("Started processing records from Kafka topic in the background")
        return {"status": "Started processing records from Kafka topic in the background."}


@app.get("/retrieve-metadata-from-data-discovery")
async def retrieve_metadata_from_data_discovery(background_tasks: BackgroundTasks):
    
    with tracer.start_as_current_span("retrieve-metadata-from-data-discovery", kind=SpanKind.SERVER) as span:
        
        def process_records_from_data_discovery_topic():
            # 1. Listen to the Kafka topic for new messages
            headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

            response = requests.get(data_discovery_consumer_base_url + "/records", headers=headers)
            
            if response.status_code != 200:
                print(f"Failed to retrieve records from data-discovery Kafka topic: {response.text}")
                span.set_attribute("error", True)
                span.set_attribute("error_details", response.text)
                return

            records = response.json()
            for record in records:
                decoded_value_json = base64.b64decode(record['value']).decode('utf-8')
                value_obj = json.loads(decoded_value_json)
                
                span.add_event(f"Consumed record with value {value_obj} from topic data-discovery")
                
                # Additional processing can be done here if necessary...

            span.set_attribute("records_processed", len(records))
            print(f"Processed {len(records)} records from data-discovery Kafka topic.")
        
        # Use background tasks to process records
        background_tasks.add_task(process_records_from_data_discovery_topic)
        span.add_event("Started processing records from data-discovery Kafka topic in the background")
        
    return {"status": "Started processing records from data-discovery Kafka topic in the background."}


@app.get("/consume-customer-domain-stream")
async def consume_customer_domain_stream(background_tasks: BackgroundTasks):
    
    with tracer.start_as_current_span("consume-customer-domain-stream", kind=SpanKind.SERVER) as span:

        if customer_domain_stream_consumer_base_url is None:
            span.set_attribute("error", True)
            span.set_attribute("error_details", "Consumer has not been initialized")
            return {"status": "Consumer has not been initialized. Please try again later."}

        url = customer_domain_stream_consumer_base_url + "/records"
        headers = {"Accept": "application/vnd.kafka.binary.v2+json"}

        ensure_table_exists.ensure_table_exists()

        def consume_customer_domain_records():
            global buffered_data
            while True:  # Continuously consume messages
                response = requests.get(url, headers=headers)
                
                if response.status_code != 200:
                    span.set_attribute("error", True)
                    span.set_attribute("error_details", f"GET /records/ did not succeed: {response.text}")
                    raise Exception(f"GET /records/ did not succeed: {response.text}")

                records = response.json()
                if not records:  # No new records to process
                    time.sleep(5)  # Sleep for a short duration before checking again
                    continue

                print(f"Processing {len(records)} records from the stream...")
                
                for record in records:
                    # Check for the presence of 'data' key in the record
                    if 'data' not in record:
                        print(f"Skipped a record without a 'data' key: {record}")
                        continue
                    
                    outer_data = record['data']
                    inner_data = json.loads(outer_data)
                    chunk = inner_data['chunk']
                    buffered_data += base64.b64decode(chunk).decode('utf-8')  # Add chunk to buffer

                    try:
                        data = json.loads(buffered_data)  # Try parsing the buffered data
                        save_data_to_sqlite.save_data_to_sqlite(buffered_data, 'weather_domain_stream_data.db')
                        buffered_data = ""  # Clear the buffer since data was successfully parsed and saved
                    except json.JSONDecodeError:
                        continue  # If parsing fails, continue buffering until a valid JSON object/array is received

        background_tasks.add_task(consume_customer_domain_records)
        span.add_event("Started consuming records from customer domain stream in the background")
        
    return {"status": "Consuming records from customer domain stream in the background"}