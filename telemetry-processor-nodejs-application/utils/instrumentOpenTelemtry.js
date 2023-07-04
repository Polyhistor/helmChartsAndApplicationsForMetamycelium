const { MeterProvider } = require('@opentelemetry/metrics');
const { PrometheusExporter } = require('@opentelemetry/exporter-prometheus');

// Create a Prometheus exporter instance
const exporter = new PrometheusExporter(
  {
    startServer: true,
  },
  () => {
    console.log('Prometheus server started');
  }
);

// Create a meter provider with Prometheus exporter
const meter = new MeterProvider({
  exporter: exporter,
  interval: 2000,
}).getMeter('express-service');
