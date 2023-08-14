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
        # Convert the span to a dictionary or JSON format as desired
        return {
            "name": span.name,
            "context": span.get_span_context(),
            "attributes": span.attributes,
            # add any other necessary fields
        }

    def shutdown(self):
        pass
