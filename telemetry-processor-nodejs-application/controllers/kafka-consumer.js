const kafka = require('kafka-node');

const client = new kafka.KafkaClient({ kafkaHost: 'localhost:9092' });
const consumer = new kafka.Consumer(
  client,
  [{ topic: 'telemetry-topic', partition: 0 }],
  {}
);

consumer.on('message', function (message) {
  const telemetryData = JSON.parse(message.value);
  // Process telemetry data
});
``;
