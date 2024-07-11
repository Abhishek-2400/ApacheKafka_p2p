import { kafka } from './client'

const init = async () => {
    const producer = kafka.producer()
    await producer.connect()
}

inti()