# Microservices communication with Kafka

A tiny, runnable example of **two microservices that never call each other.**

`order-service` receives an order and publishes an event.
`notification-service` reacts to that event and sends the confirmation.

Neither service knows the other exists.

![How the order event flows through Kafka](docs/kafka-flow.png)

---

## The one idea

Open both services and search for the string `"orders"`:

```js
// order-service/src/index.js
await producer.send({ topic: "orders", messages: [...] });

// notification-service/src/index.js
await consumer.subscribe({ topics: ["orders"] });
```

**That string is the only connection between them.**

`order-service` contains no URL, no port number, and no mention of
`notification-service` anywhere. It writes an event to a topic named `orders`
and moves on. Whoever subscribes to that topic gets it.

This is the difference between *calling a service* and *publishing an event*.

---

## Why not just call the other service directly?

The obvious approach is for `order-service` to make an HTTP request:

```js
await axios.post("http://notification-service:3001/send", order);  // ❌
```

That works, until it doesn't:

| Problem | Direct HTTP call | Kafka event |
|---|---|---|
| Notification service is **down** | Order fails | Order is safe in the log |
| Notification service is **slow** | Your customer waits | Customer gets an instant reply |
| You add a **third** service (analytics, invoicing) | Change order-service, redeploy | Change nothing — it subscribes |
| Who knows about whom | order-service needs the URL | Neither knows the other |

The order API replies in **~10 ms** because it never waits for the
notification to be sent. It only waits for Kafka to accept the event.

> **When you *do* want a direct call:** if you need an answer back right now —
> checking stock, validating a payment — call the service directly.
> Kafka is for *"this happened, react whenever you can."*

---

## Vocabulary

You only need five words for this example.

| Term | What it means here |
|---|---|
| **Broker** | The Kafka server itself. Runs in Docker on port `9092`. Think of it like a database: separate server, own port, stores data on disk. |
| **Topic** | A named stream of events. Ours is called `orders`. |
| **Producer** | Anything that writes events to a topic. Here: `order-service`. |
| **Consumer** | Anything that reads events from a topic. Here: `notification-service`. |
| **Offset** | The position of an event in the topic — `0`, `1`, `2`… The broker remembers how far each consumer has read. |

**Kafka is not a queue where messages disappear after delivery.** It's a *log*.
Events are appended in order and stay on disk (7 days by default), and each
consumer just tracks its own position in that log.

---

## Run it

You need **Docker** installed. Nothing else.

```bash
docker compose up --build
```

That starts five things: the Kafka broker, a one-shot container that creates the
`orders` topic, both microservices, and a web UI.

| | |
|---|---|
| order-service | http://localhost:3000 |
| notification-service | http://localhost:3001 |
| Kafka UI | http://localhost:8080 |

### Place an order

```bash
curl -X POST localhost:3000/api/order \
  -H 'Content-Type: application/json' \
  -d '{"item":"Biryani","amount":249}'
```

```json
{ "status": "ORDER_PLACED", "id": "721d1a6b-…", "item": "Biryani", "amount": 249 }
```

Now look at the logs of **both** services:

```
order-service-1         | 📤 order placed → Biryani
notification-service-1  | 📥 orders[0] offset 0 → Your Biryani (₹249) is confirmed!
```

`order-service` published. `notification-service` reacted. No HTTP call between them.

### See what the other service received

```bash
curl localhost:3001/api/notifications
```

---

## Look inside the broker

This is the part that makes Kafka click. The event is not "in transit" — it is
**stored**, and you can go read it.

**Option 1 — the UI:** open http://localhost:8080 → **Topics** → **orders** →
**Messages**. Every order is a row with its offset, timestamp, key, and JSON value.

**Option 2 — tail the topic from the terminal:**

```bash
docker compose exec kafka /opt/kafka/bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic orders \
  --formatter-property print.offset=true
```

```
Offset:0  {"id":"721d1a6b-…","item":"Biryani","amount":249}
Offset:1  {"id":"82364813-…","item":"Masala Dosa","amount":120}
```

Add `--from-beginning` to replay everything already in the topic.

---

## Experiments worth doing

**1. Stop the consumer and keep ordering.**

```bash
docker compose stop notification-service
curl -X POST localhost:3000/api/order -H 'Content-Type: application/json' -d '{"item":"Dosa","amount":120}'
```

The order still returns `200` — `order-service` doesn't care. Check the lag:

```bash
docker compose exec kafka /opt/kafka/bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group notification-service
```

`LAG` will be above zero: events waiting on disk. Now bring it back:

```bash
docker compose start notification-service
```

It processes everything it missed. **Nothing was lost, and nothing was retried** —
because nobody was ever trying to deliver anything.

**2. Add a third consumer.** Copy `notification-service`, change the `groupId`
to `analytics-service`, and subscribe to the same topic. Both services now
receive every order, and you changed **zero lines** in `order-service`.

**3. Break the ordering.** Recreate the topic with `--partitions 3` and place
several orders. They'll arrive out of order, because Kafka only guarantees
order *within* a partition. Key by customer instead of a random UUID to fix it.

---

## Project layout

```
.
├── docker-compose.yml          Kafka broker, topic creation, both services, UI
├── order-service/
│   └── src/index.js            Express + Kafka producer
└── notification-service/
    ├── src/index.js            Express + Kafka consumer
    └── src/reset.js            demo helper (clears the topic between runs)
```

Each service is its own package with its own `package.json` and `Dockerfile` —
they are genuinely separate applications that happen to live in one repo.

---

## Reset between runs

Clear the topic and the stored notifications without restarting anything:

```bash
curl -X DELETE localhost:3001/api/notifications
```

Offsets keep counting up after this. For a completely fresh start at `offset 0`:

```bash
docker compose down -v && docker compose up --build
```

---

## Running the services outside Docker

Useful if you want to edit code and see logs in your own terminals.
The services read `KAFKA_BROKER` and fall back to `localhost:9092`.

```bash
docker compose up -d kafka create-topic kafka-ui   # infra only

cd notification-service && npm install && npm start   # terminal 1
cd order-service        && npm install && npm start   # terminal 2
```

---

## Stack

Node.js 24 · Express 5 · [@confluentinc/kafka-javascript](https://github.com/confluentinc/confluent-kafka-javascript) 1.10 ·
Apache Kafka 4.3.1 (KRaft — no ZooKeeper) · [kafbat/kafka-ui](https://github.com/kafbat/kafka-ui) 1.5

> Most tutorials use `kafkajs`. It has had no release since 2023.
> `@confluentinc/kafka-javascript` is the maintained client and keeps a
> KafkaJS-compatible API, so the code looks almost identical.
