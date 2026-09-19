# Discussion: Frequently Asked Questions & Comparisons (FAQ) ❓

**Category**: Q&A

---

Here are answers to the most common questions about **VortexMQ** and how it compares to existing messaging systems:

### 1. How does VortexMQ compare to NATS?
* **Protocol**: NATS uses its own text protocol or JetStream APIs. VortexMQ supports the universal **Redis List and Stream protocol (RESP2)** out-of-the-box, allowing any Redis client to connect without installing new client SDKs.
* **Web UI**: VortexMQ includes an embedded **Quantum Web Studio** on port `8380` with an interactive animated pipeline visualizer, message playground, and 1-click Dead Letter Queue (DLQ) replay.
* **Delayed Tasks**: VortexMQ has a native **Hierarchical Timing Wheel** for $O(1)$ scheduled messages and exponential backoff retries without requiring external scheduling daemons.

---

### 2. How does VortexMQ compare to RabbitMQ?
* **Runtime**: RabbitMQ requires the **Erlang VM (`beam.smp`)**, which opens 7+ ports and consumes 150MB+ RAM at idle. VortexMQ is a **single 5.7MB pure Go binary** that uses <15MB RAM and boots in <5ms.
* **Stability Under Load**: RabbitMQ can trigger blocking "memory alarms" when queues fill up. VortexMQ uses circular bounded memory rings and segmented WAL disk flushing to handle high-pressure traffic without connection drops.

---

### 3. How does VortexMQ compare to Apache Kafka?
* **Operational Simplicity**: Kafka requires multiple gigabytes of JVM heap, dedicated partition management, and ZooKeeper or KRaft metadata management. VortexMQ runs as a standalone zero-dependency binary.
* **Garbage Collection**: Kafka can suffer from stop-the-world JVM GC pauses during traffic spikes. VortexMQ's hot paths run with **0 B/op allocations**, keeping p99 latencies predictable.

---

### 4. How does persistence work in VortexMQ?
* VortexMQ uses a **Segmented Write-Ahead Log (WAL)**. Every publish event is appended sequentially with an IEEE CRC32 checksum.
* On server restart or crash recovery, the WAL is replayed sequentially into memory in milliseconds.
* You can configure fsync policies via `-fsync=always` (strict durability), `-fsync=everysec` (recommended balance), or `-fsync=none` (maximum speed).

---

Have more questions about the architecture or how to integrate with your existing services? Ask below! 👇
