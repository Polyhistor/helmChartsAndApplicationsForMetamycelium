import requests
import json
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

class KafkaRESTProxyExporter(SpanExporter):
    def __init__(self, topic_name, rest_proxy_url, service_name, service_address):
        self.topic_name = topic_name
        self.rest_proxy_url = rest_proxy_url
        self.service_name = service_name
        self.service_address = service_address

    def export(self, spans):
        telemetry_data = [self.serialize_span(span) for span in spans]
        headers = {
            "Content-Type": "application/vnd.kafka.json.v2+json", 
            "Accept": "application/vnd.kafka.v2+json"
        }
        data = {
            "records": [{"value": span_data} for span_data in telemetry_data]
        }
        response = requests.post(f"{self.rest_proxy_url}/topics/{self.topic_name}", headers=headers, data=json.dumps(data))
        
        # handle the response as necessary
        if response.status_code == 200:
            return SpanExportResult.SUCCESS
        return SpanExportResult.FAILURE

    def serialize_span(self, span):
        try:
            span_context = span.get_span_context()

            # Convert the TraceState object to a string representation
            trace_state_str = str(span_context.trace_state)

            # Extract the dictionary from BoundedAttributes
            attributes_dict = dict(span.attributes)

            # Retrieve the parent span ID
            parent_span_id = span.parent.span_id if span.parent else None

            # Construct the serialized span
            serialized_span = {
                "name": span.name,
                "context": {
                    "trace_id": span_context.trace_id,
                    "span_id": span_context.span_id,
                    "parent_span_id": parent_span_id,  # Include the parent span ID
                    "is_remote": span_context.is_remote,
                    "trace_flags": span_context.trace_flags,
                    "trace_state": trace_state_str  
                },
                "start_time": span.start_time,  # Start time of the span
                "end_time": span.end_time,  # End time of the span
                "span_kind": span.kind.name,  # Span kind (e.g., CLIENT, SERVER)
                "status": span.status.status_code.name,  # Status of the span (e.g., OK, ERROR)
                "events": [{"name": event.name, "timestamp": event.timestamp, "attributes": dict(event.attributes)} for event in span.events],  # Events associated with the span
                "attributes": attributes_dict, 
                "service_name": self.service_name,  # Add the service name
                "service_address": self.service_address  # Add the service address
            }

            # This is a check to identify the non-serializable part
            json.dumps(serialized_span)
            return serialized_span

        except TypeError as e:
            # Handle serialization errors
            print(e)
            for key, value in serialized_span.items():
                try:
                    json.dumps({key: value})
                except TypeError:
                    print(f"Key '{key}' with value '{value}' is causing the error")
            raise

    def shutdown(self):
        pass
