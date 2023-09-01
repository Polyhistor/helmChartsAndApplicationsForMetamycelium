const express = require('express');
const bodyParser = require('body-parser');
const sqlite3 = require('sqlite3').verbose();
const app = express();
const path = require('path')
const axios = require('axios');

const KAFKA_PROXY_URL = 'http://localhost/kafka-rest-proxy';
const KAFKA_TOPIC = 'data-discovery';

app.use(bodyParser.json());
app.use(express.static('public'));

// Define your template engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Initialize SQLite database
let db = new sqlite3.Database('./metadata.db', (err) => {
    if (err) {
        console.error(err.message);
    }
    console.log('Connected to the metadata database.');
});

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS metadata(
            uniqueIdentifier TEXT PRIMARY KEY,
            serviceName TEXT,
            serviceAddress TEXT,
            completeness REAL,
            validity INTEGER,
            accuracy REAL,
            processingTime TEXT,
            actualTime TEXT,
            processingDuration TEXT
            )`);
});

app.post('/register', async (req, res) => {
    const data = req.body;
    const updateSql = `REPLACE INTO metadata(uniqueIdentifier, serviceName, serviceAddress, completeness, validity, accuracy, processingTime, actualTime, processingDuration)
        VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)`; // Added the processingDuration

    db.run(updateSql,
        [data.uniqueIdentifier, data.serviceName, data.serviceAddress, data.completeness, data.validity, data.accuracy, data.processingTime, data.actualTime, data.processingDuration],  // Added the processingDuration
        function(err) {
            if (err) {
                return console.log(err.message);
            }
            console.log(`A row has been inserted/updated with UUID ${data.uniqueIdentifier}`);
        });

    res.json({ message: 'Metadata registered successfully.' });
});

app.get("/", (req,res) => {
    res.json("Welcome to Datalichen")
})

app.get('/metadata', async (req, res) => {
    let sql = `SELECT DISTINCT * FROM metadata`;  // Ensure unique services
    db.all(sql, [], (err, rows) => {
        if (err) {
            throw err;
        }
        console.log(rows)
        res.render('metadata', { metadata: rows });
    });
});

app.listen(3001, () => {
    console.log('Server is running on port 3001');
});


app.get('/dispatch-metadata', async (req, res) => {
    try {
        // 1. Consume messages from the Kafka topic using Kafka REST Proxy.
        const consumeResponse = await axios.post(`${KAFKA_PROXY_URL}/consumers/my_consumer_group/instances/my_consumer/records`, {
            name: 'my_consumer',
            format: 'json',
            auto.offset.reset: 'earliest'
        });

        // Here, you can parse the consumeResponse if needed. For this example, we'll assume 
        // that any message in the topic indicates that metadata should be fetched.

        if (consumeResponse.data && consumeResponse.data.length > 0) {
            // 2. Fetch metadata from SQLite
            let sql = `SELECT DISTINCT * FROM metadata`;  // Ensure unique services
            db.all(sql, [], (err, rows) => {
                if (err) {
                    throw err;
                }
                // 3. Send the metadata back as a response
                res.json(rows);
            });
        } else {
            res.json({ message: 'No new messages in the Kafka topic' });
        }

    } catch (error) {
        console.error('Error fetching from Kafka or database:', error.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});