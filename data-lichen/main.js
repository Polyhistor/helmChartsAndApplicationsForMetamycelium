import express from 'express';
import bodyParser from 'body-parser';
import sqlitePackage from 'sqlite3';
const { verbose } = sqlitePackage;
const sqlite3 = verbose();
import path from 'path';
import axios from 'axios';
import Keycloak from 'keycloak-connect';


const app = express();

// setting the session
app.use(session({
    secret: 'someSecret',
    resave: false,
    saveUninitialized: true
  }));
  
// Initialising Keycloak
const keycloakConfig = {
    clientId: 'yourClientID',
    bearerOnly: true,
    serverUrl: 'http://localhost/keycloak/',
    realm: 'master',
    credentials: {
        secret: 'yourClientSecret'
    }
};

const keycloak = new Keycloak({}, keycloakConfig);

// Middleware to protect routes
app.use(keycloak.middleware());

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

app.get('/some-protected-route', keycloak.protect(), (req, res) => {
    res.send('This is protected!');
  });