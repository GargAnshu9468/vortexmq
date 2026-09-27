# Reddit Community Posts

---

## 1. r/golang

**Title:** I built VortexMQ: A 167M ops/sec message broker in pure Go with lock-free ring buffers and Redis protocol support

**Post:**

Hey r/golang,

I've been working on an open-source project called **VortexMQ** ([GitHub](https://github.com/GargAnshu9468/vortexmq)) and wanted to share the design and lessons learned with the community.

It's a standalone message broker and streaming engine written in pure Go (no CGO, no Erlang, no JVM) that compiles into a single 5.7 MB static binary with an idle RAM footprint under 15 MB.

### The Problem with Go Channels Under High Concurrency
When I started prototyping, native channels (`chan T`) were the obvious first choice. But under heavy multi-core benchmark contention (e.g. 32 publisher goroutines pushing to 16 consumer workers), `pprof` showed heavy lock contention on the channel's internal `hchan.lock` mutex. Channels plateaued around 12–15 million ops/sec with noticeable latency spikes.

### How VortexMQ Works:
1. **LMAX Disruptor Pattern:** The hot path uses a power-of-two circular ring buffer indexed via atomic operations (`atomic.AddUint64`). Slot lookups execute in a single CPU instruction using bitwise masking (`seq & mask`) instead of modulo division.
2. **Preventing Cache Contention (False Sharing):** `head` and `tail` sequence counters are isolated with 64-byte padding arrays (`[64]byte`) so they never occupy the same CPU cache line on multi-core systems.
3. **Zero Heap Allocations:** We use tiered `sync.Pool` arenas for RESP frame decoding and envelope serialization. The core ring buffer push/pop benchmarks at **5.98 ns/op (167.1M ops/sec) with 0 B/op allocated on the heap**.
4. **Redis Wire Protocol:** It implements Redis RESP2 and RESP3 (`LPUSH`, `BRPOP`, etc.) on port 8379, so you can connect directly with `go-redis` or any Redis client without custom drivers.
5. **Hierarchical Timing Wheel:** O(1) delayed message delivery inspired by the Linux kernel timer model, avoiding the CPU overhead of polling sorted sets.
6. **Embedded Web Studio:** The web telemetry UI is compiled directly into the binary via `embed.FS` on port 8380.

### Quickstart:
```bash
docker run -d -p 8379:8379 -p 8380:8380 ianshugarg/vortexmq:latest
```

GitHub: https://github.com/GargAnshu9468/vortexmq  
Live Demo & Docs: https://garganshu9468.github.io/vortexmq/  
Technical Wiki: https://github.com/GargAnshu9468/vortexmq/wiki  

I would love to get feedback on the ring buffer memory layout and any suggestions for improvement from fellow gophers!

---

## 2. r/programming

**Title:** Why we built a 167M ops/sec message broker in pure Go with Redis protocol compatibility

**Post:**

Many microservice architectures default to deploying Kafka or RabbitMQ when they only need reliable async queues, pub/sub, and delayed task scheduling.

The operational overhead can be heavy: multi-gigabyte memory footprints, JVM or Erlang runtime management, external cluster daemons, and GC pauses that create unpredictable latency spikes.

We built and open-sourced **VortexMQ** ([GitHub](https://github.com/GargAnshu9468/vortexmq)) as a lightweight alternative:

- **Single 5.7 MB static binary** in 100% pure Go with < 15 MB idle RAM.
- **167.1M ops/sec** lock-free ring buffer throughput (5.98 ns/op on bare metal with 0 B/op heap allocation).
- **3.57M ops/sec** Network Read (`LPOP`, matching NATS Core), **1.07M writes/sec** Durable Disk WAL (beating Apache Kafka), and **1.14M msgs/sec** bulk batch ingestion.
- **Drop-in Redis RESP2/RESP3 compatibility** on port 8379, meaning standard Redis client libraries in Python, Go, Node.js, Rust, or Java work out of the box.
- **Hierarchical Timing Wheel** for O(1) delayed message scheduling.
- **Poison-pill quarantine** to isolate crashing payloads to a Dead Letter Queue (DLQ) with 1-click GUI replay.
- **Embedded Web Studio** served on port 8380 via Go `embed.FS` (no external Node or frontend server needed).

**Engineering Trade-offs:**
VortexMQ is not a replacement for Kafka if you need multi-month event storage for data lakes with tiered S3 backups. It is designed for high-velocity in-memory messaging and low-latency task processing where you want minimal operational fuss.

Architecture details, benchmarks, and reproducible tests are on GitHub: https://github.com/GargAnshu9468/vortexmq

---

## 3. r/selfhosted

**Title:** VortexMQ – Lightweight single-binary message broker and task queue (< 15MB RAM, embedded dashboard, Redis-compatible)

**Post:**

For homelab and self-hosted environments, running Kafka or RabbitMQ for simple background jobs and automation queues is often overkill, easily eating up 500MB–1GB of memory.

**VortexMQ** is an ultra-lightweight message broker and task engine built for simple self-hosting:
- **Single 5.7 MB static binary** written in pure Go.
- **Under 15 MB RAM** idle footprint.
- **Embedded Web Studio** on port 8380 for monitoring queues, consumer lag, and replaying dead-letter messages.
- **Uses the standard Redis protocol** on port 8379, so any existing scripts, tools, or workers configured for Redis can connect without changes.
- **Docker Compose ready**:

```yaml
version: '3.8'
services:
  vortexmq:
    image: ianshugarg/vortexmq:latest
    ports:
      - "8379:8379"  # RESP2 / Message Broker
      - "8380:8380"  # Quantum Web Studio
    restart: unless-stopped
```

GitHub: https://github.com/GargAnshu9468/vortexmq  
Interactive Documentation: https://garganshu9468.github.io/vortexmq/
