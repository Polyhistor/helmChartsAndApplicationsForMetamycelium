from fastapi import FastAPI, HTTPException
from utilities import fetch_data_from_sqlite
from opentelemetry import trace
from opentelemetry.sdk.trace import TracerProvider
from opentelemetry.sdk.trace.export import BatchSpanProcessor
from opentelemetry.instrumentation.fastapi import FastAPIInstrumentor
from opentelemetry.trace import SpanKind
import time
from hashlib import sha256

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

@app.get("/")
async def welcome():
    return "Welcome to the Data Scientist Query Service!"

@app.get("/query-data")
async def query_data(query: str):
    # Start a span to measure Secret Retrieval Latency (SRL)
    with tracer.start_as_current_span("retrieve_secrets") as span:
        span.set_attribute("operation", "retrieve_secrets")
        start_time = time.time()

        # Simulating a secret retrieval operation
        # (you'd replace this with the actual operation to fetch the secret if it wasn't in memory)
        time.sleep(0.01)
        span.set_endtime(start_time + time.time())
        span.set_status(trace.status.Status(StatusCode.OK))

    # Start a span to measure Query Processing Time (QPT)
    with tracer.start_as_current_span("query_processing") as span:
        span.set_attribute("operation", "query_data")
        start_time = time.time()

        # Replace with actual database fetch operation
        data = fetch_data_from_sqlite.fetch_data(query)
        span.set_endtime(start_time + time.time())
        span.set_status(trace.status.Status(StatusCode.OK))

        return data

@app.get("/verify-data-integrity")
async def verify_data_integrity(data_id: int):
    # This endpoint simulates the process of checking the integrity of a specific data entry by its ID.

    # Retrieve the data and its expected hash from the database
    # (the actual retrieval method would be more complex and likely involve SQL)
    data, expected_hash = fetch_data_from_sqlite.fetch_data_and_hash(data_id)

    # Compute the hash of the retrieved data
    computed_hash = sha256(data.encode('utf-8')).hexdigest()

    # Compare the computed hash with the expected hash to verify integrity
    if computed_hash == expected_hash:
        return {"status": "Data integrity verified"}
    else:
        raise HTTPException(status_code=400, detail="Data integrity verification failed")


