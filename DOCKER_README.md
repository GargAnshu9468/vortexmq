# VortexMQ ⚡
### Next-Generation Ultra-Fast Message Broker & Task Engine in Pure Go

VortexMQ is an ultra-high performance distributed message broker and streaming task engine written in 100% pure Go (zero CGO, zero Erlang, zero JVM).

It solves the operational bloat and GC pauses of legacy brokers (RabbitMQ, Kafka, Redis Streams) by delivering a tiny **6.9 MB container** that boots in **<5 milliseconds** and uses **<15 MB RAM**.

---

## 🚀 Quick Start (1-Command Run)

Run VortexMQ with the embedded Web Studio and Redis-compatible protocol:

```bash
docker run -d \
  --name vortexmq \
  -p 8379:8379 \
  -p 8380:8380 \
  -v vortexmq_data:/data \
  ianshugarg/vortexmq:latest
```

### 🔌 Ports Overview
* **`8379`**: **Core Broker Port** (Drop-in Redis RESP2/Stream protocol)
* **`8380`**: **Quantum Web Studio** (Live web dashboard & HTTP REST API)

---

## 🌌 Quantum Web Studio Dashboard

Open your browser to:
👉 **`http://localhost:8380`**

* **Live Pipeline Flow Visualizer**: Real-time animated stream tracking messages moving from Producers ➜ Memory Rings ➜ Workers ➜ DLQ.
* **Dead Letter Recovery Center**: Inspect poisoned tasks with failure reasons and hit **"⚡ 1-Click Replay"** to re-dispatch them instantly.
* **Interactive Message Playground**: Test publishing events directly from your browser.
* **Dark / Light Theme**: Fluid toggle with persistent settings.

---

## 💻 Connect with Any Redis Client

VortexMQ is drop-in compatible with standard Redis List and Stream commands:

```bash
# Publish an event to the "orders" topic
redis-cli -p 8379 RPUSH orders '{"item": "Laptop", "amount": 1200}'

# Consume the event
redis-cli -p 8379 RPOP orders

# Blocking wait (up to 5s)
redis-cli -p 8379 BRPOP orders 5
```

---

## 📊 Benchmark Highlights

* **Ring Buffer Throughput**: **167.1 Million ops/sec** (5.98 ns/op, 0 B/op allocations)
* **Parallel Topic Publish**: **2.33 Million msgs/sec**
* **Durable Disk Commits (WAL)**: **642,000 writes/sec** (1.55 μs/op)
* **Cold Boot Time**: **<5 ms** (vs Kafka 20s, RabbitMQ 8s)
* **Idle Memory**: **<15 MB** (vs RabbitMQ 200MB+, Kafka 1.5GB+)

---

## 🔗 Official Links
* **GitHub Repository**: [https://github.com/GargAnshu9468/vortexmq](https://github.com/GargAnshu9468/vortexmq)
* **Official Website**: [https://garganshu9468.github.io/vortexmq/](https://garganshu9468.github.io/vortexmq/)
* **License**: Apache 2.0
