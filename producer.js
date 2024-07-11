
import { kafka } from './client.js'
import readline from 'readline'


const rl = readline.createInterface({  //creating a readline cli interface 
    input: process.stdin,
    output: process.stdout,
});


const init = async () => {
    const producer = kafka.producer()
    await producer.connect()

    rl.setPrompt("Enter message>");  //setting promt 
    rl.prompt();          //asking user to enter prompt

    rl.on('line', async (line) => {
        const [user_name, user_location] = line.split(" ");
        await producer.send({
            topic: 'rider-updates',
            messages: [
                {
                    //north -->0 south -->1
                    partition: user_location.toLowerCase() === "north" ? 0 : 1,
                    key: 'location-update',
                    value: JSON.stringify({ name: user_name, location: user_location })
                }

            ],
        })
    }).on('close', async () => {
        await producer.disconnect();
    })


}

init()