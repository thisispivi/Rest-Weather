const express = require('express');
const cors = require('cors');
const rp = require('request-promise');
const { MongoClient, MongoNetworkTimeoutError } = require('mongodb');
var client = new MongoClient('mongodb://localhost:27017');
const app = express();
const body_parser = require("body-parser");
app.use(body_parser.urlencoded({ extended: false }));
app.use(cors());
var json_parser = body_parser.json();


// Variables
const port = 8000; //Port of the rest api
const minutes = 3; // Minutes last to perform data update in the cache
const appid = '87cedcb849db1469dfb20dc6650a5266'; // The key of open weather


// Listening
app.listen(port, () => {
    console.log('Listening on port ' + port);
});


/**
 * Insert a new location in the mongodb database
 * @param {MongoClient} client 
 * @param {JSON} data The data to insert
 */
async function insertLocation(client, data) {
    try {
        await client.connect();
        const database = client.db("cache");
        const locations = database.collection("locations");
        var result = await locations.updateOne({ "location": new RegExp(data.location, "i") }, {
            $push: { data: { $each: data.data, $position: 0 } }
        });
        console.log('A document was inserted with the _id: ' + result.insertedId);
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}


/**
 * Create a new location in the mongodb database
 * @param {MongoClient} client 
 * @param {JSON} data The data to insert
 */
async function createLocation(client, data) {
    try {
        await client.connect();
        const database = client.db("cache");
        const locations = database.collection("locations");
        const result = await locations.insertOne(data);
        console.log('A document was inserted with the _id: ' + result.insertedId);
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}


/**
 * Check if there is a location that has been added in the last 15 minutes
 * @param {MongoClient} client 
 * @param {String} location
 * @param {Integer} minutes Minutes last to perform data update in the cache
 * @returns {JSON} If there is a location added less than 15 min ago it returns it, otherwise the value is null
 */
async function checkLocationMinutes(client, location, minutes) {
    try {
        await client.connect();
        const database = client.db("cache");
        const locations = database.collection("locations");
        var time = Date.now();
        // First compute the difference between the time and the time of the entry
        // Then take the entry of the database that has been added less than 15 minutes ago and that matches the location
        const pipeline = [{
                $project: {
                    location: 1,
                    temp: { $first: "$data.temp" },
                    humidity: { $first: "$data.humidity" },
                    wind: { $first: "$data.wind" },
                    pressure: { $first: "$data.pressure" },
                    timestamp: { $first: "$data.timestamp" }
                }
            },
            {
                $project: {
                    location: 1,
                    temp: 1,
                    humidity: 1,
                    wind: 1,
                    pressure: 1,
                    timestamp: { $divide: [{ $subtract: [time, "$timestamp"] }, 60000] }
                }
            },
            { $match: { "location": new RegExp(location, "ig"), "timestamp": { $lt: minutes } } }
        ];
        const agg = locations.aggregate(pipeline);
        var result = null;
        for await (const doc of agg) {
            result = doc;
        }
        if (result == null) {
            console.log("No results found");
        } else {
            result = {
                "location": result.location,
                "data": [{
                    "temp": result.temp,
                    "humidity": result.humidity,
                    "wind": result.wind,
                    "pressure": result.pressure,
                    "timestamp": result.timestamp
                }]
            }
            console.log("Results found");
        }
        return result;
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}


/**
 * Check if there is already a location
 * @param {MongoClient} client 
 * @param {String} location
 * @returns {JSON} "result" is null if there's no location, if it is not null there is a location. Found represent if the location was found
 */
async function checkLocation(client, location) {
    try {
        var found;
        await client.connect();
        const database = client.db("cache");
        const locations = database.collection("locations");
        var result = null;
        result = await locations.findOne({ "location": new RegExp(location, "i") });
        if (result == null) {
            found = false;
            console.log("No results found");
        } else {
            found = true;
            console.log("Result found");
        }
        return { "result": result, "found": found };
    } catch (e) {
        console.error({ "result": e, "found": found });
    } finally {
        await client.close();
    }
}


/**
 * Delete the location in the database if it is in the cache
 * @param {MongoClient} client 
 * @param {String} location The location to delete
 * @returns {boolean} True if the location was deleted, false otherwise
 */
async function deleteLocation(client, location) {
    try {
        await client.connect();
        const database = client.db("cache");
        const locations = database.collection("locations");
        var result = await locations.deleteOne({ "location": new RegExp(location, "i") });
        return result.acknowledged;
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}


/**
 * Update some fields of a particular location
 * @param {MongoClient} client 
 * @param {JSON} data The updated data
 * @returns {boolean} True if data was updated, false otherwise
 */
async function updateLocation(client, data) {
    try {
        await client.connect();
        const database = client.db("cache");
        const locations = database.collection("locations");
        var result = await locations.updateOne({ "location": new RegExp(data.location, "i") }, { $set: data });
        return result.acknowledged;
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}


/**
 * Get all the locations in the database
 * @param {MongoClient} client 
 * @returns {JSON} The json with all the locations
 */
async function getAll(client) {
    try {
        await client.connect();
        const database = client.db("cache");
        const locations = database.collection("locations");
        const result = await locations.find().toArray();
        return result;
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}


/**
 * Get all the locations in the database
 * @param {MongoClient} client 
 * @returns {JSON} The json with all the locations
 */
async function getSingle(client, location) {
    try {
        await client.connect();
        const database = client.db("cache");
        const locations = database.collection("locations");
        const result = await locations.findOne({ "location": new RegExp(location, "i") });
        return result;
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}


/**
 * Process the data obtained from openweather and insert it in the cache
 * @param {JSON} body The body of the open weather response
 * @param {boolean} found if there is the location in the cache
 * @returns {JSON} The data processed
 */
function processData(body, found) {
    let result = {
        "location": body.name,
        "data": [{
            "temp": parseFloat((body.main.temp - 273.15).toFixed(1)),
            "humidity": body.main.humidity,
            "wind": body.wind.speed,
            "pressure": body.main.pressure,
            "timestamp": Date.now()
        }]
    };

    if (found) {
        insertLocation(client, result);
    } else {
        createLocation(client, result);
    }
    return result;
}


/**
 * Add timestamp to a post data and add it to the cache
 * @param {MongoClient} client 
 * @param {JSON} data The data to add
 * @returns {JSON} The data with the timestamp added
 */
function addTimestamp(client, data, found) {
    data.data[0]["timestamp"] = Date.now();
    if (found) {
        insertLocation(client, data);
    } else {
        createLocation(client, data);
    }
    return data;
}


/**
 * Filter the data. Parameters showed: Location, Temp, Humidity, Wind, Pressure
 * @param {JSON} data 
 * @returns {JSON} The data filtered
 */
function filter(data) {
    let result = {
        "location": data.location,
        "temp": data.data[0].temp,
        "humidity": data.data[0].humidity,
        "wind": data.data[0].wind,
        "pressure": data.data[0].pressure,
    };
    return result;
}


// Get the weather of a particular location
app.get('/weather/:location', (req, res) => {
    try {
        location = req.params.location;
        console.log("\nGetting the weather of: " + location);
        // If there is an entry of the specified location that has been added less than 15 minutes ago then show it
        // Else delete an antry of the specified location (if there is one) and create a new one
        rp('https://api.openweathermap.org/data/2.5/weather?q=' + location + '&appid=' + appid, { json: true }).then(body => {
            location = body.name;
            checkLocationMinutes(client, location, minutes).then(function(result) {
                client = new MongoClient('mongodb://localhost:27017');
                checkLocation(client, location).then(function(r) {
                    if (result != null) {
                        res.send(filter(result));
                    } else {
                        res.send(filter(processData(body, r["found"])));
                    }
                });
            });
        });
    } catch (e) {
        res.send(e);
    }
});


// Get all documents from cache
app.get('/', (req, res) => {
    try {
        console.log("\nGet Request");
        getAll(client).then(function(result) {
            res.send(result);
        });
    } catch (e) {
        res.send(e);
    }
});


// Get single location from cache
app.get('/get-single/:location', (req, res) => {
    try {
        location = req.params.location;
        console.log("\nGet Single Request: " + location);
        getSingle(client, location).then(function(result) {
            res.send(result);
        });
    } catch (e) {
        res.send(e);
    }
});


// Create a new document in the cache
app.post('/', json_parser, (req, res) => {
    try {
        location = String(req.body.location);
        message = req.body;
        checkLocationMinutes(client, location, minutes).then(function(result) {
            client = new MongoClient('mongodb://localhost:27017');
            checkLocation(client, location).then(function(r) {
                if (result != null) {
                    res.send({ "status": 1, "message": filter(result) });
                } else {
                    client = new MongoClient('mongodb://localhost:27017');
                    addTimestamp(client, message, r["found"]);
                    res.send({ "status": 0, "message": message });
                }
            });
        });
    } catch (e) {
        res.send({ "status": 2, "message": e });
    }
});


// Delete a location from cache
app.delete('/', json_parser, (req, res) => {
    try {
        console.log("\nDelete: " + req.body.location);
        location = String(req.body.location);
        deleteLocation(client, location);
        res.send({ "status": true, "message": "Deleted " + location });
    } catch (e) {
        res.send({ "status": false, "message": e });
    }
});


// Update a location
app.put('/', json_parser, (req, res) => {
    try {
        data = req.body;
        if (req.body.location == null) {
            throw "No location inserted";
        }
        console.log("\nUpdating city: " + data.location);
        checkLocation(client, req.body.location).then(function(result) {
            if (result != null) {
                client = new MongoClient('mongodb://localhost:27017');
                var status = updateLocation(client, data);
                if (status) console.log("Updated");
                res.send({ "status": true, "message": "Updated" });
            }
        });
    } catch (e) {
        res.send({ "status": false, "message": e });
    }
});