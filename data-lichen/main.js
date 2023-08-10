const express = require('express');
const bodyParser = require('body-parser');
const sqlite3 = require('sqlite3').verbose();
const app = express();
const path = require('path')


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
            processingTime TEXT,  // Changed this from REAL to TEXT as it's now a datetime
            actualTime TEXT,
            processingDuration TEXT  // New field
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
        res.render('metadata', { metadata: rows });
    });
});

app.listen(3000, () => {
    console.log('Server is running on port 3000');
});
