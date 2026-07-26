import express from "express";
import { KafkaJS } from "@confluentinc/kafka-javascript";

const brokers = [process.env.KAFKA_BROKER ?? "localhost:9092"];

const kafka = new KafkaJS.Kafka({
  kafkaJS: { brokers, logLevel: KafkaJS.logLevel.NOTHING },
});

const producer = kafka.producer();
await producer.connect();

const app = express();
app.use(express.json());

app.post("/api/order", async (req, res) => {
  const { item, amount } = req.body ?? {};
  const order = { id: crypto.randomUUID(), item, amount };

  // 📤 drop the event on Kafka and reply instantly — no waiting
  await producer.send({
    topic: "orders",
    messages: [{ key: order.id, value: JSON.stringify(order) }],
  });

  console.log("📤 order placed →", order.item);
  res.json({ status: "ORDER_PLACED", ...order });
});

app.listen(3000, () => console.log("🟢 order-service on :3000"));
