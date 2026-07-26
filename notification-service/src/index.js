import express from "express";
import { KafkaJS } from "@confluentinc/kafka-javascript";
import { createResetRouter } from "./reset.js";

const brokers = [process.env.KAFKA_BROKER ?? "localhost:9092"];

const kafka = new KafkaJS.Kafka({
  kafkaJS: { brokers, logLevel: KafkaJS.logLevel.NOTHING },
});
const consumer = kafka.consumer({
  kafkaJS: { groupId: "notification-service", fromBeginning: true },
});
await consumer.connect();
await consumer.subscribe({ topics: ["orders"] });

const sent = [];
// 📥 order events arrive here — order-service never calls this service
await consumer.run({
  eachMessage: async ({ topic, partition, message }) => {
    const order = JSON.parse(message.value);
    const text = `Your ${order.item} (₹${order.amount}) is confirmed!`;

    sent.push({ orderId: order.id, text });
    console.log(`📥 ${topic}[${partition}] offset ${message.offset} → ${text}`);
  },
});
const app = express();
app.get("/api/notifications", (_, res) => res.json(sent));

app.use(await createResetRouter(kafka, sent));
app.listen(3001, () => console.log("🔵 notification-service on :3001"));
