# Show HN: VortexMQ

### Recommended Titles (< 80 characters):

**Option 1 (73 chars - Recommended):**
```
Show HN: VortexMQ – Fast message broker in pure Go with Redis protocol
```

**Option 2 (72 chars):**
```
Show HN: VortexMQ – Message broker in pure Go with Redis protocol and UI
```

**Option 3 (66 chars):**
```
Show HN: VortexMQ – Pure Go message broker with Redis protocol
```

### Suggested URL (Link submission):
`https://github.com/GargAnshu9468/vortexmq`

---

### First Comment / Text Submission:

Hi HN,

I built VortexMQ because I wanted a lightweight message broker that felt like a native Go binary: single 5.7 MB static file, zero runtime dependencies (no JVM, no Erlang), under 15 MB of idle RAM, and fast enough for high-throughput streaming workloads.

Often when building microservices, we only need fast queueing, pub/sub, and delayed retries. Deploying full-blown Kafka or RabbitMQ brings hundreds of megabytes of idle memory overhead, external cluster daemons, and GC pauses that cause latency spikes under load.

A few technical details on how it is implemented:

- **Lock-Free Ring Buffer:** Rather than using mutex-protected Go channels (`chan T`), the core hot path uses a bounded circular ring buffer with atomic sequence increments (`atomic.AddUint64`). Slot lookups use bitwise masking (`seq & mask`) instead of modulo arithmetic.
- **Cache-Line Padding:** Atomic sequence counters (`head` and `tail`) are isolated with 64-byte padding arrays (`[64]byte`) to eliminate hardware false sharing across CPU cores. On bare metal, ring buffer push/pop benchmarks at 5.98 ns/op with zero heap allocations, network read LPOP reaches 3.57M msgs/sec (matching NATS Core), and 256KB buffered WAL group commits sustain 1.07M durable writes/sec (beating single-broker Kafka).
- **Redis Wire Protocol:** Instead of requiring proprietary client libraries, VortexMQ implements the Redis RESP2/RESP3 protocol on port 8379. Existing clients like `go-redis`, `redis-py`, `ioredis`, or `redis-cli` work out of the box.
- **Hierarchical Timing Wheel:** O(1) delayed task delivery inspired by the Linux kernel timer design, avoiding the sorted set polling overhead typical of Redis-based queues.
- **Poison-Pill Quarantine:** If a message triggers an unhandled panic in a consumer or exceeds retry limits, the broker catches the panic and moves the payload to a Dead Letter Queue (DLQ) with stack traces rather than crash-looping workers.
- **Embedded Web Studio:** An administrative dashboard is embedded directly into the 5.7 MB binary via Go `embed.FS` on port 8380 for monitoring queue throughput and replaying DLQ messages with one click.

**Limitations and Trade-offs:**
VortexMQ is not intended to replace Kafka for multi-terabyte analytical event lakes with months of tiered S3 storage, nor does it attempt to mirror the full complex topic exchange routing of AMQP/RabbitMQ. It is focused on high-throughput in-memory messaging, low-latency task queues, and simple operations.

Source code and reproducible benchmarks:
https://github.com/GargAnshu9468/vortexmq

Interactive live sandbox & documentation:
https://garganshu9468.github.io/vortexmq/

I would love to get your thoughts on the ring buffer implementation, the memory layout, and any edge cases in our RESP parser.
