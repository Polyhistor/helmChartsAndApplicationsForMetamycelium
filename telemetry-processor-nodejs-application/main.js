const axios = require('axios');

const REST_PROXY_URL = 'http://localhost/kafka-rest-proxy';  
const CONSUMER_GROUP = 'telemetry-consumer-group';
const TOPIC = 'telemetry';

async function createConsumerInstance() {
    try {
        const consumerConfig = {
            name: "telemetry-consumer",  // This can be any unique name for the consumer instance
            format: "json",
            "auto.offset.reset": "earliest"
        };

        // Create a consumer instance under the consumer group
        const response = await axios.post(`${REST_PROXY_URL}/consumers/${CONSUMER_GROUP}`, consumerConfig);
        
        const consumerBaseUri = response.data.base_uri;
        console.log(`Consumer instance created with base URI: ${consumerBaseUri}`);
        
        return consumerBaseUri;
    } catch (error) {
        console.error("Error creating consumer instance:", error.response?.data || error.message);
    }
}

async function subscribeToTopic(consumerBaseUri) {
    try {
        const subscribeConfig = {
            topics: [TOPIC]
        };

        await axios.post(`${consumerBaseUri}/subscription`, subscribeConfig);
        console.log(`Subscribed to topic: ${TOPIC}`);
    } catch (error) {
        console.error("Error subscribing to topic:", error.response?.data || error.message);
    }
}

async function consumeMessages(consumerBaseUri) {
    try {
        const response = await axios.get(`${consumerBaseUri}/records?timeout=10000`);
        const messages = response.data;
        for (let message of messages) {
            console.log(`Received message: ${JSON.stringify(message.value)}`);
        }
    } catch (error) {
        console.error("Error consuming messages:", error.response?.data || error.message);
    }
}

async function startService() {
    const consumerBaseUri = await createConsumerInstance();
    await subscribeToTopic(consumerBaseUri);

    console.log("Starting to consume messages...");
    setInterval(() => {
        consumeMessages(consumerBaseUri);
    }, 10000);  // Poll every 10 seconds
}

startService();
