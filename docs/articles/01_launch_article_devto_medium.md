# Why I Built VortexMQ: A 167M ops/sec Message Broker in 100% Pure Go

Every engineering team eventually runs into the "message broker tax."

You start building a clean, modern microservice architecture in Go or Rust. The services compile to small binaries, boot in 20 milliseconds, and use under 25 MB of memory. Everything feels snappy and predictable.

Then your system grows, and you need asynchronous queueing, delayed job retries, and task streaming.

So you deploy Kafka or RabbitMQ.

Almost immediately, your infrastructure profile shifts:
- You now manage multi-gigabyte JVM heaps or an Erlang beam runtime.
- You need coordinator daemons (ZooKeeper, KRaft, or Mnesia clusters).
- Nodes consume 500 MB to 1 GB of RAM at idle before processing their first message.
- Stop-the-world garbage collection pauses occasionally turn a 150-microsecond latency into a 200-millisecond p99 spike.
- You have to wire up external Prometheus exporters, Grafana dashboards, and third-party UIs just to check queue depths and inspect dead messages.

When the queue consumes 20 times more resources than the application services producing and consuming the messages, something feels unbalanced.

I wanted a message broker that felt like Go itself: a single 5.7 MB static binary, zero runtime dependencies, instant startup, sub-microsecond latency, and enough throughput to saturate a 10GbE network link on commodity hardware.

That is why I built **[VortexMQ](https://github.com/GargAnshu9468/vortexmq)**.

---

## What is VortexMQ?

VortexMQ is an open-source, ultra-fast message broker and task engine written in pure Go (zero CGO, zero external dependencies).

You can run it locally with Docker in a couple of seconds:

```bash
docker run -d -p 8379:8379 -p 8380:8380 ianshugarg/vortexmq:latest
```

Here is the core feature set:
- **167.1 Million ops/sec** lock-free ring buffer throughput (5.98 ns/op on bare metal)
- **3.57 Million msgs/sec** network read LPOP throughput (matching NATS Core)
- **2.33 Million ops/sec** multi-worker parallel topic publish throughput
- **1.14 Million msgs/sec** native bulk batch ingestion (`VMQ.PUBLISH_BATCH`)
- **1,075,268 writes/sec** durable disk commits with 256KB WAL group commit (beating single-broker Kafka)
- **Drop-in Redis RESP2 and RESP3 compatibility**: connect using standard Redis clients in Go (`go-redis`), Python (`redis-py`), Node.js (`ioredis`), Rust, or `redis-cli`
- **Hierarchical Timing Wheel**: O(1) delayed message delivery without sorted set polling hacks
- **Poison-Pill Dead Letter Queues (DLQ)**: automatic panic recovery that isolates crashing payloads with 1-click GUI replay
- **Embedded Quantum Web Studio**: an interactive dashboard served directly from the 5.7 MB binary on port 8380 via Go's `embed.FS`
- **Under 15 MB RAM idle footprint**

---

## The Bottleneck in Go Channels (`chan T`)

When I started experimenting with the core engine, the first logical choice was native Go channels (`chan T`).

Channels are great for application concurrency, but under heavy multi-core benchmark loads (32 producer goroutines pushing to 16 consumer workers), CPU profiles in `pprof` revealed heavy lock contention. Internally, a Go channel uses an `hchan` struct protected by a mutex (`hchan.lock`). When dozens of cores hammer the same channel, CPU cores spend significant cycles waiting on cache-line invalidations and scheduler handoffs.

Native channels hit a plateau around 12 to 15 million ops/sec with noticeable latency variance under contention.

To get past this ceiling, I turned to the **LMAX Disruptor pattern**.

---

## Architectural Deep Dive: How We Reached 5.98 ns/op

```
Producer goroutine
       │
       ▼
┌────────────────────────────────────────────────────────┐
│               LMAX DISRUPTOR RING BUFFER               │
│                                                        │
│  [Slot 0] [Slot 1] [Slot 2] [Slot 3] ... [Slot N]      │
│     ▲                                       ▲          │
│     │ atomic.AddUint64                      │          │
│  Head Seq                                Tail Seq      │
│  (64-byte padded)                  (64-byte padded)    │
└────────────────────────────────────────────────────────┘
       │
       ▼
Consumer goroutine (Sub-microsecond batching)
```

### 1. Lock-Free Atomic Indexing

Instead of holding mutexes, VortexMQ stores messages in a contiguous, power-of-two circular ring buffer.

Publishers claim sequential slots using atomic instructions:

```go
nextSeq := atomic.AddUint64(&rb.head, 1) - 1
slotIndex := nextSeq & rb.mask
```

Because the buffer capacity is always a power of two (such as 65,536 or 1,048,576), calculating the slot index requires only a bitwise AND (`seq & mask`). This replaces expensive CPU integer division with a single CPU clock cycle instruction.

### 2. Eliminating False Sharing with Cache-Line Padding

Modern x86 and ARM processors do not read and write single bytes from RAM; they fetch memory in **64-byte cache lines**.

If Core 0 (running a publisher) writes to the `head` sequence counter, and Core 1 (running a consumer) reads the `tail` sequence counter, but both variables share the same 64-byte memory segment:
- Core 0's write invalidates Core 1's L1/L2 cache line.
- Core 1 is forced to reload the entire line from L3 cache or main RAM, even though it never accessed `head`.

This hardware contention (false sharing) can silently cut multi-core throughput by over 70%.

In VortexMQ, every hot sequence counter is explicitly padded with 64-byte boundary arrays:

```go
type RingBuffer struct {
    _pad0 [64]byte
    head  uint64
    _pad1 [64]byte
    tail  uint64
    _pad2 [64]byte
    mask  uint64
    slots []MessageSlot
}
```

This guarantees `head` and `tail` never share a CPU cache line, allowing independent cores to run at full hardware memory bus speed.

### 3. Zero Heap Allocations with `sync.Pool`

In high-throughput Go services, garbage collector (GC) pauses are rarely caused by the number of objects; they are caused by the *rate of heap allocations*.

If every incoming TCP frame allocates a new 4KB byte slice, processing 1 million messages per second allocates 4 GB of heap memory per second. The Go runtime will trigger continuous GC sweep phases, generating unpredictable latency spikes.

VortexMQ uses tiered `sync.Pool` arenas for RESP frame decoding and payload envelopment. Memory buffers are acquired from the pool on frame arrival and recycled immediately after the message is enqueued:

```go
var framePool = sync.Pool{
    New: func() any {
        b := make([]byte, 4096)
        return &b
    },
}
```

On hot paths, heap allocations measure **0 bytes per operation**.

---

## Drop-In Redis Compatibility: No Custom SDKs Required

One of the biggest hurdles when adopting a new broker is having to install proprietary client SDKs and rewrite application code.

VortexMQ implements the **Redis Serialization Protocol (RESP2 and RESP3)** on port `8379`. If your application already uses Redis for job queues or pub/sub, you can point your existing client directly to VortexMQ.

### Example: Python (`redis-py`)

```python
import redis

# Connect directly to VortexMQ on port 8379
client = redis.Redis(host="localhost", port=8379, db=0)

# Publish a job
client.lpush("tasks:orders", '{"order_id": 9482, "amount": 149.99}')

# Consume with blocking pop
queue, payload = client.brpop("tasks:orders", timeout=5)
print(f"Processed: {payload.decode('utf-8')}")
```

### Example: Go (`go-redis`)

```go
package main

import (
    "context"
    "fmt"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:8379",
    })

    // Publish to the lock-free ring
    rdb.LPush(ctx, "billing:invoices", `{"invoice_id": "INV-2026-001"}`)

    // Blocking consumer worker
    res, err := rdb.BRPop(ctx, 0, "billing:invoices").Result()
    if err != nil {
        panic(err)
    }
    fmt.Printf("Received payload: %s\n", res[1])
}
```

---

## O(1) Delayed Scheduling: Hierarchical Timing Wheel

Scheduling messages for future execution (such as retry delays, billing reminders, or notification timers) is usually awkward:
- In RabbitMQ, teams often combine dead-letter exchanges with TTLs or install plugins.
- In Redis, workers poll sorted sets (`ZADD` and `ZRANGEBYSCORE`), which creates CPU burn and race conditions across multiple consumers.

VortexMQ embeds a **Hierarchical Timing Wheel** (inspired by the Linux kernel timer model):

```
[Wheel 0: 10ms per tick]  ──► Range: 0 to 1,000ms
       │
[Wheel 1: 1s per tick]   ──► Range: 1s to 60s
       │
[Wheel 2: 1m per tick]   ──► Range: 1m to 60m
```

Inserting or canceling a timer is strictly an **O(1)** pointer operation. Messages sleep efficiently without thread-blocking until their exact deadline, at which point the timer drops the message directly into the consumer's active ring buffer.

---

## Production Guardrails: Poison-Pill Quarantine & Web UI

A common failure mode in background worker pools is the "poison pill" payload: a malformed JSON string or unexpected schema that triggers an unhandled panic in the consumer. Standard queues will often re-queue the payload indefinitely, trapping workers in a crash loop.

VortexMQ includes built-in panic guards:
1. When a worker crashes or exceeds its retry budget (`MaxRetries = 3`), the broker isolates the message.
2. The payload is moved to the topic's **Dead Letter Queue (DLQ)** along with the failure timestamp, error reason, and stack trace.
3. The rest of the queue continues processing uninterrupted.

From the embedded **Quantum Web Studio** (`http://localhost:8380`), you can inspect the quarantined payloads, check consumer lag, and click **Replay** to re-inject fixed messages into active processing.

---

## Benchmark Numbers (Reproducible)

All benchmarks can be verified locally on bare metal:

```bash
git clone https://github.com/GargAnshu9468/vortexmq.git
cd vortexmq
go test -benchmem -bench=. ./benchmarks/...
```

### Benchmark Results

| Operation | Throughput | Latency | Heap Allocation |
| :--- | :--- | :--- | :--- |
| **Ring Buffer Push/Pop** | **167.1M ops/sec** | **5.98 ns/op** | **0 B/op (0 allocs)** |
| **TCP Client (Pipelined)** | **2.33M ops/sec** | **428 ns/op** | **0 B/op pooled** |
| **Hierarchical Timer Wheel** | **14.2M ops/sec** | **70.1 ns/op** | **0 B/op** |
| **DLQ Panic Quarantine** | **1.85M ops/sec** | **540 ns/op** | **1 alloc/op** |

---

## What VortexMQ is NOT (Engineering Trade-offs)

No tool is right for every problem. Being clear about trade-offs is essential:

- **Not an analytical data lake**: If you need multi-month event retention across hundreds of gigabytes for Hadoop or Snowflake queries with tiered S3 storage, Kafka is the right tool. VortexMQ is designed for high-velocity operational messaging and task dispatching.
- **Not a complex AMQP topology**: If you require intricate topic exchanges, header routing rules, and dynamic queue federations with dozens of AMQP plugins, RabbitMQ's feature set is broader. VortexMQ prioritizes raw speed, simplicity, and standard Redis protocol semantics.

---

## Getting Started

You can test VortexMQ in less than a minute:

```bash
# 1. Start with Docker
docker run -d --name vortexmq -p 8379:8379 -p 8380:8380 ianshugarg/vortexmq:latest

# 2. Test with redis-cli
redis-cli -p 8379 LPUSH my-queue "Hello VortexMQ"
redis-cli -p 8379 BRPOP my-queue 0

# 3. Open Web Studio in your browser
open http://localhost:8380
```

- **GitHub Repository**: [github.com/GargAnshu9468/vortexmq](https://github.com/GargAnshu9468/vortexmq)
- **Interactive Documentation & Live Simulator**: [garganshu9468.github.io/vortexmq](https://garganshu9468.github.io/vortexmq/)
- **Technical Wiki**: [github.com/GargAnshu9468/vortexmq/wiki](https://github.com/GargAnshu9468/vortexmq/wiki)
- **Community Discussions**: [github.com/GargAnshu9468/vortexmq/discussions](https://github.com/GargAnshu9468/vortexmq/discussions)

Feedback, PRs, and benchmark reports on different hardware architectures are very welcome!
