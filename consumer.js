const Kafka = require("node-rdkafka");

const TOPIC_NAME = "energymeter";
const SASL_MECHANISM = "SCRAM-SHA-256";

const stream = new Kafka.createReadStream(
    {
        "metadata.broker.list": "kafka-3891f4e4-scalable-chat-app-redis-nextjs.k.aivencloud.com:22618",
        "group.id": "GROUP_ID",
        "security.protocol": "sasl_ssl",
        "sasl.mechanism": SASL_MECHANISM,
        "sasl.username": "avnadmin",
        "sasl.password": "AVNS_TF99GUGpRTJ5xvcjvCy",
        "ssl.ca.location": "ca.pem",
    },
    { "auto.offset.reset": "beginning" },
    { topics: [TOPIC_NAME] }
);

stream.on("data", (message) => {
    console.log("Got message using SASL:", message.value.toString());
});