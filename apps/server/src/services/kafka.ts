import { Kafka, Producer } from "kafkajs";
import fs from "fs";
import path from "path";
import prismaClient from "./prisma";
const kafka = new Kafka({
  brokers: ["kafka-baec5f2-redis-bansal.a.aivencloud.com:23758"],
  ssl: {
    ca: [fs.readFileSync("./ca.pem", "utf-8")],
  },
  sasl: {
    username: "avnadmin",
    password: "AVNS_hMGMdNBYow4QxJjQ9YN",
    mechanism: "plain",
  },
});
let producer: null | Producer = null;

export async function createProducer() {
  if (!producer) {
    const _producer = kafka.producer();
    await _producer.connect();
    producer = _producer;
  }
  return producer;
}

export async function produceMessage(message: string) {
  const producer = await createProducer();
  await producer.send({
    messages: [{ key: `message-${Date.now()}`, value: message }],
    topic: "MESSAGES",
  });
  return true;
}

export async function startMessageConsumer() {
  const consumer = kafka.consumer({ groupId: "default" });
  await consumer.connect();
  await consumer.subscribe({ topic: "MESSAGES", fromBeginning: true });

  await consumer.run({
    autoCommit: true,
    eachMessage: async ({ message, pause }) => {
      if (!message.value) return;
      console.log("New Message recieved");
      try {
        await prismaClient.message.create({
          data: {
            text: message.value?.toString(),
          },
        });
      } catch (err) {
        console.log(`Error ${err}`);
        pause();
        setTimeout(() => {
          consumer.resume([{ topic: "MESSAGES" }]);
        }, 60 * 1000);
      }
    },
  });
}

export default kafka;
