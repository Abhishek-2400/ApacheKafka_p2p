const Kafka = require("node-rdkafka");
const axios = require("axios");

const TOPIC_NAME = "energymeter";
const SASL_MECHANISM = "SCRAM-SHA-256";



// Kafka Consumer Setup
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

const userBuffer = {}; // Stores data for each user

stream.on("data", async (message) => {
    try {
        const messageStr = message.value.toString();
        console.log("Raw message:", messageStr);

        // Parse the string as JSON
        const data = JSON.parse(messageStr);
        console.log("Parsed message:", data);

        const { userId, production, consumption, balance, time } = data; // Match producer's field names

        if (!userBuffer[userId]) {
            userBuffer[userId] = [];
        }

        userBuffer[userId].push({ production, consumption, balance, time });
    } catch (error) {
        console.error("Error processing message:", error);
    }
});

// Process data every 10 seconds
setInterval(async () => {
    const users = Object.keys(userBuffer);
    for (const userId of users) {
        await processUserData(userId);
    }
}, 10000);

async function processUserData(userId) {
    const data = userBuffer[userId];
    if (!data || data.length === 0) return;

    // Compute required analytics
    const totalProduction = data.reduce((sum, item) => sum + item.production, 0);
    const totalConsumption = data.reduce((sum, item) => sum + item.consumption, 0);
    const avgProduction = data.reduce((sum, item) => sum + item.production, 0) / data.length;
    const avgConsumption = data.reduce((sum, item) => sum + item.consumption, 0) / data.length;
    const peakProduction = Math.max(...data.map(item => item.production));
    const peakConsumption = Math.max(...data.map(item => item.consumption));
    const efficiencyRatio = avgConsumption !== 0 ? avgProduction / avgConsumption : 0;
    const surplusEnergy = data.reduce((sum, item) => sum + (item.production > item.consumption ? item.production - item.consumption : 0), 0);
    const energyDeficit = data.reduce((sum, item) => sum + (item.consumption > item.production ? item.consumption - item.production : 0), 0);

    // Save to MongoDB
    const energyPayload = {
        userId,
        totalProduction,
        totalConsumption,
        avgProduction,
        avgConsumption,
        peakProduction,
        peakConsumption,
        efficiencyRatio,
        surplusEnergy,
        energyDeficit,
        time: new Date(),
    };
    console.log("Energy payload:", energyPayload);
    const response = await axios.post('https://energytrading.vercel.app/api/products/addenergymetrics', energyPayload);
    if (response?.data?.message) {
        // alert(response.data.message);
        //console.log(response.data)
        console.log(`Data processed & saved for user ${userId} and data size is ${data.length}`);
    } else {
        alert(response?.data?.error || 'Failed to add product');
    }

    // Clear buffer for the user
    delete userBuffer[userId];
}

stream.on("error", (err) => console.error("Kafka Consumer Error:", err));
