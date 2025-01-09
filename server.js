const express = require('express');
const fs = require('fs');
const BSON = require('bson');
const path = require('path');

const app = express();
let schemas = require('./schemas.js');
const PORT = 1122;

app.use(express.json());

const initializeDatabase = (dbName) => {
    const filePath = path.join(__dirname, 'data', `${dbName}.bson`);
    if (!fs.existsSync(filePath)) {
        const initialData = BSON.serialize({ records: [] });
        fs.writeFileSync(filePath, initialData);
        console.log(`Initialized empty database for ${dbName}.`);
    }
};

const initializeDatabases = () => {
    // Gehe durch jedes Schema und stelle sicher, dass die Datenbankdateien existieren
    Object.keys(schemas).forEach(db => {
        initializeDatabase(db);
    });
};

initializeDatabases();

const validateData = (db, data) => {
    const schema = schemas[db];

    if (!schema) {
        throw new Error(`Schema for ${db} not found.`);
    }

    for (let key in schema) {
        if (!data.hasOwnProperty(key)) {
            throw new Error(`Missing field: ${key}`);
        }
        if (typeof data[key] !== schema[key]) {
            throw new Error(`Field ${key} must be of type ${schema[key]}`);
        }
    }
};

const readDataFromBson = (dbName) => {
    const filePath = path.join(__dirname, 'data', `${dbName}.bson`);
    if (fs.existsSync(filePath)) {
        const bsonData = fs.readFileSync(filePath);
        const deserialized = BSON.deserialize(bsonData);
        return deserialized.records || [];
    }
    return [];
};

const saveDataAsBson = (dbName, data) => {
    const filePath = path.join(__dirname, 'data', `${dbName}.bson`);
    const bsonData = BSON.serialize({ records: data }); // Array in ein Objekt einbetten

    fs.writeFileSync(filePath, bsonData);
    console.log(`Data for ${dbName} saved in BSON format.`);
};

app.post('/insert/:db', (req, res) => {
    const { db } = req.params;
    const newRecord = req.body;

    try {
        validateData(db, newRecord);

        const currentData = readDataFromBson(db);
        currentData.push(newRecord);

        saveDataAsBson(db, currentData);
        res.status(201).send('Record inserted');
    } catch (err) {
        res.status(400).send(`Error: ${err.message}`);
    }
});

app.get('/select/:db', (req, res) => {
    const { db } = req.params;
    const currentData = readDataFromBson(db);
    res.json(currentData);
});

app.get('/select/:db/:key', (req, res) => {
    const { db, key } = req.params;
    const currentData = readDataFromBson(db);

    const record = currentData.find(record =>
        Object.keys(record).some(field => record[field] === key)
    );

    if (record) {
        res.json(record);
    } else {
        res.status(404).send(`Record with key ${key} not found in ${db}`);
    }
});

app.get('/select/:db/:key/:value', (req, res) => {
    const { db, key, value } = req.params;
    const currentData = readDataFromBson(db);

    const record = currentData.find(record =>
        Object.keys(record).some(field => record[field] === key)
    );

    if (record) {
        if (record.hasOwnProperty(value)) {
            res.json({ [value]: record[value] });
        } else {
            res.status(404).send(`Field ${value} not found for key ${key}`);
        }
    } else {
        res.status(404).send(`Record with key ${key} not found in ${db}`);
    }
});


app.listen(PORT, () => {
    console.log(`Server running on port ${PORT}`);
});