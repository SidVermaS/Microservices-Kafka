import { Router } from "express";

// 🧹 Demo helper — not part of the Kafka lesson.
// Wipes the "orders" topic and the in-memory store so each take starts clean.
export async function createResetRouter(kafka, sent) {
  const admin = kafka.admin();
  await admin.connect();

  const router = Router();

  router.delete("/api/notifications", async (_, res) => {
    sent.length = 0;

    const offsets = await admin.fetchTopicOffsets("orders");
    await admin.deleteTopicRecords({
      topic: "orders",
      partitions: offsets.map(({ partition }) => ({ partition, offset: "-1" })),
    });

    console.log("🧹 cleared topic + notifications");
    res.json({ status: "CLEARED" });
  });

  return router;
}
