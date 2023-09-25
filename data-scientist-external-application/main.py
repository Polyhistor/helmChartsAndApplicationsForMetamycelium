from fastapi import FastAPI, HTTPException
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.trace import SpanKind
from utilities import KafkaRESTProxyExporter
from concurrent.futures import ThreadPoolExecutor
from minio import Minio
import threading
import time
from hvac import Client

app = FastAPI()
FastAPIInstrumentor.instrument_app(app)

# Global variables
SERVICE_ADDRESS = "http://localhost:8010"
SERVICE_NAME = "DATA_SCIENTIST_APPLICATION"
SERVICE_VERSION = "1.0.0"
ENVIRONMENT = "production"
KAFKA_REST_PROXY_URL = "http://localhost/kafka-rest-proxy"

# Setting up the trace provider
trace.set_tracer_provider(TracerProvider())

kafka_exporter = KafkaRESTProxyExporter(topic_name="telemetry-data", rest_proxy_url=KAFKA_REST_PROXY_URL, service_name=SERVICE_NAME, service_address=SERVICE_ADDRESS)
span_processor = BatchSpanProcessor(kafka_exporter)
trace.get_tracer_provider().add_span_processor(span_processor)

# Setting up OpenTelemetry
tracer = trace.get_tracer(__name__)

# MinIO fetch utilities
minio_lock = threading.Lock()
MINIO_POOL_SIZE = 10
executor = ThreadPoolExecutor(max_workers=MINIO_POOL_SIZE)

def minio_fetch(storage_info):
    with minio_lock:
        minio_client = Minio(
            storage_info["distributedStorageAddress"],
            access_key=storage_info["minio_access_key"],
            secret_key=storage_info["minio_secret_key"],
            secure=False
        )

        data = minio_client.get_object(storage_info["bucket_name"], storage_info["object_name"])
        chunks = []
        for d in data.stream(32*1024):
            chunks.append(d)
        data_str = b''.join(chunks).decode('utf-8')
        return data_str

@app.get("/")
async def welcome():
    return "Welcome to the Data Scientist Query Service!"

@app.get("/query-data/{data_location}")
async def query_data(data_location: str):
    valid_data_locations = ["custom-domain-analytical-data", "weather-domain-analytical-data"]
    if data_location not in valid_data_locations:
        raise HTTPException(status_code=400, detail="Invalid data location")

    with tracer.start_as_current_span("retrieve_secrets") as span:
        client = Client(url='http://localhost:8200')  # Assuming Vault is at localhost:8200
        # Add authentication method if required
        read_response = client.secrets.kv.read_secret_version(path='Data-Scientist-User-Pass')
        if 'data' not in read_response or 'data' not in read_response['data']:
            raise HTTPException(status_code=500, detail="Unable to retrieve secrets from Vault")
        secrets = read_response['data']['data']

    with tracer.start_as_current_span("query_processing") as span:
        storage_info = {
            "distributedStorageAddress": "http://localhost:9001",
            "minio_access_key": secrets["access_key"],
            "minio_secret_key": secrets["secret_key"],
            "bucket_name": data_location
        }

        minio_client = Minio(
            storage_info["distributedStorageAddress"],
            access_key=storage_info["minio_access_key"],
            secret_key=storage_info["minio_secret_key"],
            secure=False
        )

        objects_list = []
        for obj in minio_client.list_objects(storage_info["bucket_name"]):
            object_data = minio_fetch({
                **storage_info,
                "object_name": obj.object_name
            })
            objects_list.append({
                "object_name": obj.object_name,
                "data": object_data
            })

        return {"objects": objects_list}


