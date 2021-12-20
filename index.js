const jwt = require('jsonwebtoken')
const express = require('express');
const cors = require('cors');
const rp = require('request-promise');
const { MongoClient, MongoNetworkTimeoutError } = require('mongodb');
var client = new MongoClient('mongodb://localhost:27017');
const app = express();
const body_parser = require("body-parser");
app.use(body_parser.urlencoded({ extended: false }));
const corsConfig = {
    credentials: true,
    origin: true,
};
app.use(cors(corsConfig));
var json_parser = body_parser.json();
var JwtStrategy = require('passport-jwt').Strategy,
    ExtractJwt = require('passport-jwt').ExtractJwt;
const passport = require('passport');
var crypto = require('crypto');
const mongoose = require("mongoose");
const MONGODB_URL = "mongodb://localhost:27017/cache";
var cookieParser = require('cookie-parser');
const { reset } = require('nodemon');
mongoose.connect(
    MONGODB_URL, {
        useNewUrlParser: true,
        useUnifiedTopology: true
    }
);
const secret_key = 'de3546d0a3aa8be473415dca2455c046104a3d94fa42cf3ef36b5b96c444c91beef37eb8e35f829e6a88703557ea15666a0510a38ca8cc4d9061903a1536d6e3';
app.use(passport.initialize());
app.use(cookieParser());





// Mongoose models
const user_schema = new mongoose.Schema({
    _id: Number,
    username: String,
    password: String,
    token: String
});

const User = mongoose.model('users', user_schema);

const counter_schema = new mongoose.Schema({
    _id: String,
    seq: Number
});

const Counter = mongoose.model('counters', counter_schema);





// Variables
const port = 8000; //Port of the rest api
const minutes = 3; // Minutes last to perform data update in the cache
const appid = '87cedcb849db1469dfb20dc6650a5266'; // The key of open weather





// Listening
app.listen(port, () => {
    console.log('Listening on port ' + port);
});





// Passport
var cookieExtractor = function(req) {
    var token = null;
    if (req && req.cookies) {
        token = req.cookies['jwt'];
    }
    return token;
};

var opts = {}
opts.jwtFromRequest = cookieExtractor;
opts.secretOrKey = secret_key;
passport.use(new JwtStrategy(opts, function(jwt_payload, done) {
    User.findOne({ id: jwt_payload.sub }, function(err, user) {
        if (err) {
            return done(err, false);
        }
        if (user) {
            return done(null, user);
        } else {
            return done(null, false);
        }
    });
}));


// Locations
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
        await locations.updateOne({ "location": new RegExp(data.location, "i") }, {
            $push: { data: { $each: data.data, $position: 0 } }
        });
        console.log('A document was inserted');
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
        await locations.insertOne(data);
        console.log('A document was inserted');
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





// Authentication

/**
 * Return the next id of the counter collection
 * @param {MongoClient} client 
 * @param {String} name name of the _id field
 * @returns The incremental id
 */
async function getNextSequence(name) {
    try {
        var id = await Counter.findOneAndUpdate({ _id: name }, { $inc: { seq: 1 } });
        return id.seq;
    } catch (e) {
        console.error(e);
    } finally {
        await client.close();
    }
}


/**
 * Check if the user already exists
 * @param {String} username 
 * @returns {JSON} The user if the user exists, null otherwise
 */
async function userExist(username) {
    try {
        return await User.findOne({ "username": username });
    } catch (e) {
        console.error(e);
    }
}


/**
 * Create the sha265 of the password
 * @param {String} password 
 * @returns {string} The sha 256 of the password
 */
function generateSha(password) {
    return crypto.createHash('sha256').update(password).digest('hex');
}


/**
 * Create the token
 * @returns {String} The token
 */
function generateToken() {
    return crypto.randomBytes(64).toString('base64url');
}


/**
 * Insert new user in the database
 * @param {JSON} user User data
 * @returns {String} a message if the user has been inserted
 */
async function insertNewUser(user) {
    try {
        var new_user = new User(user);
        new_user.save(function(err, user) {
            if (err) return console.error(err);
            console.log(user.name);
        });
        return "User inserted";
    } catch (e) {
        console.error(e);
    }
}


/**
 * Check if the user already exists and if the password is correct
 * @param {String} username 
 * @param {String} password 
 * @returns {boolean} True if the user exists and the password is correct false otherwise
 */
async function isPasswordRight(username, password) {
    try {
        var result = await User.findOne({ "username": username, "password": password });
        if (result == null) {
            return false;
        }
        return true;;
    } catch (e) {
        console.error(e);
    }
}


/**
 * Edit an user
 * @param {Number} id The id of the user
 * @param {String} username The new username
 * @param {String} password The new password
 * @returns The status
 */
async function editUser(id, username, password) {
    try {
        return User.findOneAndUpdate({ "_id": id }, { "username": username, "password": password })
    } catch (e) {
        console.error(e);
    }
}


/**
 * Delete user
 * @param {Number} id The id of the suer
 * @returns The status
 */
async function deleteUser(id) {
    try {
        return User.deleteOne({ "_id": id })
    } catch (e) {
        console.error(e);
    }
}





// Endpoints

// Get the weather of a particular location
app.get('/weather/:location', passport.authenticate('jwt', { session: false }), (req, res) => {
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
app.get('/', passport.authenticate('jwt', { session: false }), (req, res) => {
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
app.get('/get-single/:location', passport.authenticate('jwt', { session: false }), (req, res) => {
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
app.post('/', passport.authenticate('jwt', { session: false }), json_parser, (req, res) => {
    try {
        console.log("\nCreating new document")
        location = String(req.body.location);
        message = req.body;
        checkLocationMinutes(client, location, minutes).then(function(result) {
            client = new MongoClient('mongodb://localhost:27017');
            checkLocation(client, location).then(function(r) {
                if (result != null) {
                    console.log("Wait 15 min to update")
                    res.send({ "status": 1, "message": filter(result) });
                } else {
                    console.log("Success");
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
app.delete('/', passport.authenticate('jwt', { session: false }), json_parser, (req, res) => {
    try {
        console.log("\nDelete: " + req.body.location);
        location = String(req.body.location);
        deleteLocation(client, location);
        console.log("Success");
        res.send({ "status": true, "message": "Deleted " + location });
    } catch (e) {
        res.send({ "status": false, "message": e });
    }
});


// Update a location
app.put('/', passport.authenticate('jwt', { session: false }), json_parser, (req, res) => {
    try {
        data = req.body;
        if (req.body.location == null || req.body.timestamp) {
            throw "No location or timestamp inserted";
        }
        console.log(req.body)
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


// Register
app.post('/signup', json_parser, (req, res) => {
    try {
        data = req.body;
        console.log("\nCreating user");
        userExist(data.username).then(result => {
            if (result == null) {
                getNextSequence("user_id").then(auto_id => {
                    var username = data.username;
                    var token = jwt.sign({ username }, secret_key, { expiresIn: 1000000, });
                    var user = {
                        "_id": auto_id,
                        "username": username,
                        "password": generateSha(data.password + ""),
                        "token": generateToken()
                    };
                    insertNewUser(user).then(data => {
                        console.log("User inserted");
                        res.send({
                            "access_token": token,
                            "expires_in": 1000000
                        });
                    })
                });
            } else {
                console.log("User already inserted");
                res.send("User already inserted");
            }
        });
    } catch (e) {
        res.send(e);
    }
});


// Login
app.post('/login', json_parser, function(req, res) {
    try {
        const usr = req.body.username;
        const psw = req.body.password;
        console.log("\nUser Login");
        userExist(usr).then(result => {
            if (result != null) {
                isPasswordRight(usr, psw).then(right => {
                    if (right) {
                        var token = jwt.sign({ usr }, secret_key, { expiresIn: 1000000, });
                        console.log("Success");
                        res.send({
                            "access_token": token,
                            "expires_in": 1000000
                        });
                    } else {
                        console.log("Wrong Password");
                        res.send({ "error": "Wrong Password" });
                    }
                })
            } else {
                console.log("Username not found");
                res.send({ "error": "Username not found" });
            }
        })
    } catch (e) {
        res.send(e);
    }
});


// Update user
app.put('/update', passport.authenticate('jwt', { session: false }), json_parser, function(req, res) {
    try {
        const id = req.body._id;
        const usr = req.body.username;
        const psw = req.body.password + "";
        console.log("\nUpdate user");
        editUser(id, usr, psw).then(end => {
            var token = jwt.sign({ usr }, secret_key, { expiresIn: 1000000, });
            console.log("Success")
            res.send({
                "access_token": token,
                "expires_in": 1000000
            });
        });
    } catch (e) {
        res.send(e);
    }
});


// Delete user
app.delete('/delete', passport.authenticate('jwt', { session: false }), json_parser, function(req, res) {
    try {
        console.log("\nDelete User");
        const id = req.body._id;
        deleteUser(id).then(result => {
            console.log("Success");
            res.send(result);
        })
    } catch (e) {
        res.send(e);
    }
});


// Get user data
app.get('/get_user/:username', passport.authenticate('jwt', { session: false }), json_parser, function(req, res) {
    try {
        const username = req.params.username;
        console.log("\nGet data of single user");
        userExist(username).then(result => {
            if (result != null) {
                res.send(result);
                console.log("Success");
            } else {
                console.log("No user found");
                res.send({ "error": "No user found" });
            }
        })
    } catch (e) {
        res.send(e);
    }
});


// Check username
app.get('/check_username/:username', json_parser, function(req, res) {
    try {
        const username = req.params.username;
        console.log("\nCheck username");
        userExist(username).then(result => {
            if (result == null) {
                res.send({ "message": "No user found" });
                console.log("No user found");
            } else {
                console.log("User found");
                res.send({ "error": "User found" });
            }
        })
    } catch (e) {
        res.send(e);
    }
});