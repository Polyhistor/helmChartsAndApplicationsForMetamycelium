import express from 'express';
import bodyParser from 'body-parser';
import sqlitePackage from 'sqlite3';
const { verbose } = sqlitePackage;
const sqlite3 = verbose();
import path from 'path';
import axios from 'axios';
import KcAdminClient from '@keycloak/keycloak-admin-client';
import { fileURLToPath } from 'url';

const __dirname = path.dirname(fileURLToPath(import.meta.url));


const app = express();

const KAFKA_PROXY_URL = 'http://localhost/kafka-rest-proxy';
const KAFKA_TOPIC = 'data-discovery';

app.use(bodyParser.json());
app.use(express.static('public'));

// Define your template engine
app.set('view engine', 'ejs');
app.set('views', path.join(__dirname, 'views'));

// Initialising Keycloak
const adminClient = new KcAdminClient({
    baseUrl: 'http://localhost/keycloak/', 
    realmName: 'master'
})

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


app.get('/publish-metadata', async (req, res) => {
    try {
        // 1. Fetch metadata from SQLite
        let sql = `SELECT DISTINCT * FROM metadata`;  // Ensure unique services
        let metadata = await new Promise((resolve, reject) => {
            db.all(sql, [], (err, rows) => {
                if (err) {
                    reject(err);
                } else {
                    resolve(rows);
                }
            });
        });

        // 2. Produce metadata to the Kafka topic via Kafka REST Proxy
        const produceData = metadata.map(item => ({
            value: item
        }));

        await axios.post(`${KAFKA_PROXY_URL}/topics/${KAFKA_TOPIC}`, {
            records: produceData
        }, {
            headers: {
                'Content-Type': 'application/vnd.kafka.json.v2+json'
            }
        });

        res.json({ message: 'Metadata fetched and sent to Kafka successfully' });

    } catch (error) {
        console.error('Error fetching from database or sending to Kafka:', error.message);
        res.status(500).json({ error: 'Internal server error' });
    }
});

app.get('/keycloak-users', async (req, res) => {
    await adminClient.auth({
        username: 'admin-lichen', 
        password: 'admin123', 
        grantType: 'password', 
        clientId: 'admin-lichen'
    })

    const users = await adminClient.users.find()

    console.log();

})