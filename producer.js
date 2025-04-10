const express = require('express');
const http = require('http');
const socketIo = require('socket.io');

const app = express();
const server = http.createServer(app);
const io = socketIo(server, {
    cors: {
        origin: '*',
    },
});



const Kafka = require("node-rdkafka");

const TOPIC_NAME = "energymeter";
const SASL_MECHANISM = "SCRAM-SHA-256";

const producer = new Kafka.Producer({
    "metadata.broker.list": "kafka-energyhive-shukla24abhi-059a.f.aivencloud.com:22198",
    "security.protocol": "sasl_ssl",
    "sasl.mechanism": SASL_MECHANISM,
    "sasl.username": "avnadmin",
    "sasl.password": "AVNS_cdQZKsD-XEDTlQuvdao",
    "ssl.ca.location": "ca.pem",
    "dr_cb": true
});

// Connect producer once at startup
const initProducer = async () => {
    try {
        producer.connect();
        console.log("Kafka Producer connected successfully");
    } catch (error) {
        console.error("Kafka Producer connection failed:", error);
    }
};

// we are connectign thee kafka producer to our websocket  bcz kafka was not directly integrating with our frontend part it was taking it as a backend component 
// hence we used websockets to connect the kafka producer to our frontend part
io.on('connection', (socket) => {
    console.log('New client connected');
    socket.on('newdata', (data) => {
        //producer logiv
        const { userId, production, consumption, balance, time } = data;
        sendMessage(userId, production, consumption, balance, time);
    });
    socket.on('disconnect', () => {
        console.log('Client dissconnected');
    });
});

// Function to send messages
const sendMessage = (userId, production, consumption, balance, time) => {
    const messageValue = JSON.stringify({ userId, production, consumption, balance, time });
    try {
        producer.produce(
            TOPIC_NAME,
            null, // Partition (null for auto-assign)
            Buffer.from(messageValue),
            userId // Key
        );
        console.log(`Message sent for user ${userId}and the messgae is ${messageValue}`);
    } catch (error) {
        console.error(`Error sending message for user ${userId}:`, error);
    }
};


server.listen(8080, () => {
    console.log('Server is running on port 8080');
});

initProducer();

// require('dotenv').config(); // Load environment variables from .env

// const express = require('express');
// const http = require('http');
// const socketIo = require('socket.io');
// const Kafka = require("node-rdkafka");

// const app = express();
// const server = http.createServer(app);
// const io = socketIo(server, {
//     cors: {
//         origin: '*',
//     },
// });

// // Load Kafka configs from environment
// const TOPIC_NAME = process.env.KAFKA_TOPIC;
// const SASL_MECHANISM = process.env.KAFKA_MECHANISM;

// const producer = new Kafka.Producer({
//     "metadata.broker.list": process.env.KAFKA_BROKER,
//     "security.protocol": "sasl_ssl",
//     "sasl.mechanism": SASL_MECHANISM,
//     "sasl.username": process.env.KAFKA_USERNAME,
//     "sasl.password": process.env.KAFKA_PASSWORD,
//     "ssl.ca.location": process.env.KAFKA_SSL_CA_LOCATION,
//     "dr_cb": true
// });

// // Connect producer at startup
// const initProducer = async () => {
//     try {
//         producer.connect();

//         producer.on('ready', () => {
//             console.log(" Kafka Producer connected successfully");
//         });

//         producer.on('event.error', (err) => {
//             console.error(" Kafka Producer error:", err);
//         });

//     } catch (error) {
//         console.error(" Kafka Producer connection failed:", error);
//     }
// };

// // Socket.io to send Kafka messages via frontend
// io.on('connection', (socket) => {
//     console.log('🔌 New client connected');

//     socket.on('newdata', (data) => {
//         const { userId, production, consumption, balance, time } = data;
//         sendMessage(userId, production, consumption, balance, time);
//     });

//     socket.on('disconnect', () => {
//         console.log(' Client disconnected');
//     });
// });

// // Function to send message to Kafka
// const sendMessage = (userId, production, consumption, balance, time) => {
//     const messageValue = JSON.stringify({ userId, production, consumption, balance, time });

//     try {
//         producer.produce(
//             TOPIC_NAME,
//             null,
//             Buffer.from(messageValue),
//             userId
//         );
//         console.log(`Message sent for user ${userId}: ${messageValue}`);
//     } catch (error) {
//         console.error(` Error sending message for user ${userId}:`, error);
//     }
// };

// server.listen(8080, () => {
//     console.log(' Server is running on port 8080');
// });

// initProducer();
