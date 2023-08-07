const express = require('express');
const bodyParser = require('body-parser');
const sqlite3 = require('sqlite3').verbose();
const app = express();

app.use(bodyParser.json());

// Define your template engine
app.set('view engine', 'ejs');

// Initialize SQLite database
let db = new sqlite3.Database('./metadata.db', (err) => {
    if (err) {
        console.error(err.message);
    }
    console.log('Connected to the metadata database.');
});

db.serialize(() => {
    db.run(`CREATE TABLE IF NOT EXISTS metadata(
            serviceAddress TEXT,
            completeness REAL,
            validity INTEGER,
            accuracy REAL,
            processingTime REAL,
            actualTime TEXT
            )`);
});

app.post('/register', async (req, res) => {
    const data = req.body;
    db.run(`INSERT INTO metadata(serviceAddress, completeness, validity, accuracy, processingTime, actualTime) VALUES(?, ?, ?, ?, ?, ?)`,
        [data.serviceAddress, data.completeness, data.validity, data.accuracy, data.processingTime, data.actualTime], 
        function(err) {
            if (err) {
                return console.log(err.message);
            }
            console.log(`A row has been inserted with rowid ${this.lastID}`);
        });
    res.json({ message: 'Metadata registered successfully.' });
});

app.get("/", (req,res) => {
    res.json("Welcome to Datalichen")
})

app.get('/metadata', async (req, res) => {
    let sql = `SELECT * FROM metadata`;
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
