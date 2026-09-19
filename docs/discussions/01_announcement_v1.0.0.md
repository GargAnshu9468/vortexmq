# Discussion: Welcome to VortexMQ v1.0.0! 🚀

**Category**: Announcements

---

Hey everyone! 👋

I'm thrilled to introduce **VortexMQ v1.0.0**, an ultra-high performance distributed message broker and streaming task engine written in **100% pure Go** (zero CGO, zero Erlang, zero JVM).

### 🌌 Why We Built VortexMQ
In production, asynchronous messaging is a critical lifeline. However, the existing tools carry severe baggage:
* **RabbitMQ**: Requires the heavy Erlang runtime, takes 150MB+ idle RAM, and can freeze all connections under unacknowledged queue memory alarms.
* **Apache Kafka**: Requires gigabytes of JVM heap, where GC pauses stall consumer lag calculation and rebalance events interrupt processing.
* **Redis Task Queues (BullMQ / Celery)**: Lack native dead-letter queues and message replays, leading to dropped tasks on Redis restarts.

We built VortexMQ to test what happens when you combine **pure Go concurrency**, **lock-free circular ring buffers**, and an **embedded Web Studio dashboard** into a single 5.7 MB static binary.

---

### 📊 Benchmark Highlights
* **167.1 Million ops/sec** (5.98 ns/op) on in-memory ring buffers with **0 B/op heap allocation**.
* **2.33 Million msgs/sec** parallel topic publish throughput.
* **642,000 writes/sec** on segmented Write-Ahead Log (WAL) with IEEE CRC32 checksums.
* **<15 MB idle RAM** and **<5 ms cold boot time**.

---

### 🚀 Get Started in 5 Seconds:

```bash
docker run -d -p 8379:8379 -p 8380:8380 ianshugarg/vortexmq:latest
```

* **Core Broker**: `redis-cli -p 8379 RPUSH tasks "hello" && redis-cli -p 8379 RPOP tasks`
* **Quantum Web Studio**: Open [http://localhost:8380](http://localhost:8380) to see the live animated pipeline flow, interactive playground, and 1-click DLQ replay.

---

I would love to get your feedback, architectural critique, benchmark reproductions, and feature requests. What kind of workloads would you like to run on VortexMQ? Let's discuss below! 👇
