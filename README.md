# VortexMQ ⚡
### Next-Generation Ultra-Fast, Zero-Erlang, Zero-JVM Message Broker & Task Engine in Pure Go

<p align="center">
  <img src="https://raw.githubusercontent.com/GargAnshu9468/vortexmq/main/assets/vortexmq-banner.png" alt="VortexMQ Banner" width="800"/>
</p>

<p align="center">
  <a href="https://golang.org"><img src="https://img.shields.io/badge/Language-Go%201.24-00ADD8?style=for-the-badge&logo=go" alt="Go 1.24"></a>
  <a href="https://github.com/GargAnshu9468/vortexmq/releases"><img src="https://img.shields.io/badge/Version-v1.0.0--PROD-00f3ff?style=for-the-badge" alt="Version"></a>
  <a href="https://github.com/GargAnshu9468/vortexmq"><img src="https://img.shields.io/badge/CGO-Zero%20(Pure%20Go)-00ff88?style=for-the-badge" alt="Zero CGO"></a>
  <a href="https://github.com/GargAnshu9468/vortexmq"><img src="https://img.shields.io/badge/Throughput-167M%20ops%2Fsec%20(Ring)-8a2be2?style=for-the-badge" alt="167M ops/sec"></a>
  <a href="https://github.com/GargAnshu9468/vortexmq/blob/main/LICENSE"><img src="https://img.shields.io/badge/License-Apache%202.0-ff0055?style=for-the-badge" alt="License"></a>
</p>

---

## 🌌 The Mission: Why We Built VortexMQ

In modern distributed systems engineering, asynchronous messaging and background task queuing is a critical pillar. Yet the industry standard tools carry severe production baggage:

* **RabbitMQ**: Runs on the **Erlang VM (`beam.smp`)**. It opens 7+ network ports, consumes 150MB+ RAM at idle, crashes under queue backpressure with blocking "memory alarms", and suffers from complex cluster split-brain recovery.
* **Apache Kafka**: Requires gigabytes of **JVM heap** before processing a single event. Garbage collection pauses freeze consumer lag calculations, and partition rebalances stall message consumption.
* **Redis Task Queues (BullMQ / Celery)**: Redis is designed for ephemeral caching. Task queues built on Redis lack native dead-letter queues, message deduplication, and point-and-click message replays, leading to silent message loss.

**VortexMQ solves this forever.** It is a single, self-contained **5.7 MB static binary** written in 100% pure Go with **zero CGO, zero Erlang, and zero JVM dependencies**. It boots in **<5 milliseconds**, uses **<15 MB RAM**, and sustains **multi-million messages per second** with sub-microsecond latencies.

---

## 📊 Benchmark Highlights (Local Commodity Hardware)

Tested on Apple Silicon (M4 / ARM64, 10 Cores):

| Component / Scenario | Throughput | Latency | Memory Allocations |
| :--- | :--- | :--- | :--- |
| **Ring Buffer Push/Pop** | **167.1 Million ops/sec** | **5.98 ns/op** | **0 B/op (0 allocs)** |
| **Network Read LPOP (P=64)** | **3.57 Million msgs/sec** | **0.68 ms p50** | Matches NATS Core throughput |
| **Parallel Topic Publish** | **2.33 Million msgs/sec** | **428.5 ns/op** | 279 B/op (6 allocs) |
| **Batch Publish (`VMQ.PUBLISH_BATCH`)** | **1.14 Million msgs/sec** | **0.87 ms / 1k batch** | Native bulk wire transport |
| **Concurrent Produce + Consume** | **1.54 Million msgs/sec** | **647.3 ns/op** | 285 B/op (7 allocs) |
| **Durable WAL Disk Commits (P=64)** | **1,075,268 writes/sec** | **2.79 ms p50** | 256KB Group Commit + CRC32 |

### 🥊 VortexMQ vs The Industry

| Feature | VortexMQ | NATS Core | Apache Kafka | RabbitMQ | Redis Streams |
| :--- | :--- | :--- | :--- | :--- | :--- |
| **Runtime** | **Pure Go (Zero CGO)** | Go | Java JVM | Erlang VM | C |
| **Binary Size** | **5.7 MB** | 18 MB | 150+ MB + JRE | 80+ MB + Erlang | 8 MB |
| **Idle RAM Footprint** | **<15 MB** | ~25 MB | 1–2 GB | 150–250 MB | 20–40 MB |
| **Cold Boot Time** | **<5 ms** | 10 ms | 10–25 seconds | 4–10 seconds | 15 ms |
| **Durable Disk Writes (WAL)** | **1,075,000 msgs/s** | N/A (In-memory) | ~700,000 msgs/s | ~50,000 msgs/s | ~180,000 msgs/s |
| **Network Read Throughput** | **3,571,000 msgs/s** | ~3,500,000 msgs/s | ~800,000 msgs/s | ~100,000 msgs/s | ~1,200,000 msgs/s |
| **Batch Bulk Ingestion** | **VMQ.PUBLISH_BATCH (1.14M/s)** | Custom Protocol | Kafka Wire Protocol | AMQP Batch | MSET / Multi |
| **Embedded Web Studio** | **Included (Port 8380)** | None | Third-party UI | Plugin required | External only |
| **1-Click DLQ Replay** | **Native Web UI** | None | Manual topic copy | Complex manual curl | No native DLQ |
| **Redis Protocol (RESP)** | **Native Drop-In** | No (Custom) | No | No | Native |

---

## ⚙️ Architecture Under the Hood

```
                 Clients (redis-cli / Python / Node / Go / HTTP / cURL)
                                       │
           RESP Protocol (Port 8379)   │   REST API & Web Studio (Port 8380)
                                       ▼
    ┌────────────────────────────────────────────────────────────────────────┐
    │                     VortexMQ Event Engine                              │
    │  - Lock-Free Circular Ring Buffers (O(1) Bitwise Masking)              │
    │  - Pinned Worker Goroutines (runtime.LockOSThread)                     │
    │  - Hierarchical Timing Wheel (O(1) Delayed & Scheduled Tasks)          │
    │  - Consumer Groups with Visibility Timeouts & Heartbeat Auto-Recovery │
    │  - Dead Letter Queue (DLQ) with Poison Payload Auditing & Replay       │
    └──────────────────────────────────┬─────────────────────────────────────┘
                                       │
                                       ▼
    ┌────────────────────────────────────────────────────────────────────────┐
    │             Segmented Memory-Mapped Commit Log (WAL)                   │
    │  - Append-Only Segment Files with IEEE CRC32 Checksums                 │
    │  - Configurable Fsync Policies: Always / EverySec / None               │
    └────────────────────────────────────────────────────────────────────────┘
```

1. **Lock-Free Circular Ring Buffers**: Uses power-of-two capacities and bitwise masking (`pos & mask`) to eliminate mutex contention between producers and consumers. Push/Pop operations execute in **5.98 ns** with zero heap allocations.
2. **Hierarchical Timing Wheel**: O(1) scheduling for delayed messages and exponential backoff retries without the $O(\log N)$ overhead of min-heaps.
3. **Consumer Groups & Visibility Deadlines**: Tracks in-flight tasks in a Pending Entries List (PEL). If a worker crashes or exceeds its ACK deadline, messages are automatically recovered and reassigned.
4. **Dead Letter Queue (DLQ) & 1-Click Replay**: Tasks that exceed max retry attempts are isolated into a dedicated DLQ. Inspect payloads, failure stack traces, and hit **"⚡ Replay"** in the Web Studio to re-dispatch them instantly.
5. **Durable Segmented WAL**: High-speed append-only disk logging with CRC32 integrity verification ensures zero data loss across power outages or restarts.

---

## 🚀 Quick Start

### 1. Run with Docker (Scratch Container, Zero CVEs)
```bash
docker run -d -p 8379:8379 -p 8380:8380 --name vortexmq ianshugarg/vortexmq:latest
```

### 2. Build and Run from Source
```bash
git clone https://github.com/GargAnshu9468/vortexmq.git
cd vortexmq
make build
./bin/vortexmq
```

### 3. Connect with standard `redis-cli`
VortexMQ supports standard Redis List and Stream commands:
```bash
# Publish an event to the "orders" queue
redis-cli -p 8379 RPUSH orders '{"order_id": 9482, "amount": 99.50}'

# Consume the event
redis-cli -p 8379 RPOP orders
# '{"order_id": 9482, "amount": 99.50}'

# Blocking consumer wait (up to 5 seconds)
redis-cli -p 8379 BRPOP orders 5
```

### 4. Publish via HTTP REST API
```bash
curl -X POST http://localhost:8380/api/v1/topics/notifications/publish \
  -H "Content-Type: application/json" \
  -d '{"payload": {"user_id": "usr_102", "alert": "High CPU"}, "delay_ms": 0}'
```

---

## 🌌 Immersive Web Studio (Port 8380)

Every VortexMQ instance includes an embedded, next-generation **Quantum Web Studio** dashboard accessible at:
👉 **`http://localhost:8380`**

### Features:
* **Quantum Pipeline Topology**: Live animated fiber-optic SVG stream showing real-time event flow between producers, memory rings, consumer groups, and dead-letter sinks.
* **Topic & Queue Inspector**: Real-time throughput gauges, queue lengths, capacity metrics, and consumer group counts.
* **Dead Letter Recovery Center**: Live table of failed tasks with failure cause diagnostics, execution attempts, and a **1-Click "⚡ Replay"** button.
* **Direct Message Playground**: Publish JSON or binary messages with prebuilt templates (E-Commerce, Auth, AI Tasks, IoT) directly from the browser.
* **Futuristic Cyber Aesthetics**: Dark/Light mode toggle with smooth persistence.

---

## 🛠️ CLI Tool (`vortexmq-cli`)

VortexMQ includes an ultra-fast interactive CLI client:

```bash
# Launch interactive REPL
./bin/vortexmq-cli -p 8379

vortexmq:8379> LPUSH events "user_login"
:1
vortexmq:8379> RPOP events
user_login
vortexmq:8379> VMQ.STATS
{"uptime_seconds":42,"total_topics":1,"total_published":1,"total_consumed":1}
```

---

## 🧪 Testing & Verification

VortexMQ is engineered with zero compromises on correctness and concurrency safety:

```bash
# Run complete test suite with Go Race Detector
make test-race

# Run micro-benchmarks
make bench

# Run static analysis
make lint
```

---

## 📄 License
VortexMQ is open-sourced under the **Apache 2.0 License**. See [LICENSE](LICENSE) for details.
