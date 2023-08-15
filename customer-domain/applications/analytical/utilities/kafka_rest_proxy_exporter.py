import requests
import json
from opentelemetry.sdk.trace.export import SpanExporter, SpanExportResult

class KafkaRESTProxyExporter(SpanExporter):
    def __init__(self, topic_name, rest_proxy_url):
        self.topic_name = topic_name
        self.rest_proxy_url = rest_proxy_url

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

            # Convert the TraceState object to a string representation or another serializable format
            trace_state_str = str(span_context.trace_state)

            # Extract the dictionary from BoundedAttributes
            attributes_dict = dict(span.attributes)

            serialized_span = {
                "name": span.name,
                "context": {
                    "trace_id": span_context.trace_id,
                    "span_id": span_context.span_id,
                    "is_remote": span_context.is_remote,
                    "trace_flags": span_context.trace_flags,
                    "trace_state": trace_state_str  # Use the serialized TraceState
                },
                "attributes": attributes_dict,  # Use the extracted dictionary
                # add any other necessary fields
            }

            # This is a check to identify the non-serializable part
            json.dumps(serialized_span)
            return serialized_span

        except TypeError as e:
            # This will print the problematic portion of the serialized_span
            print(e)
            for key, value in serialized_span.items():
                try:
                    json.dumps({key: value})
                except TypeError:
                    print(f"Key '{key}' with value '{value}' is causing the error")
            raise



    def shutdown(self):
        pass
