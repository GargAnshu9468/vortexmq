# Welcome to the VortexMQ Wiki ⚡

**VortexMQ** is an ultra-high performance, single-binary, distributed message broker and streaming task engine written in 100% pure Go (zero CGO). 

It is designed as a modern, lightweight, crash-resilient alternative to legacy message brokers (**RabbitMQ**, **Apache Kafka**) and fragile cache-backed task queues (**Celery**, **BullMQ**).

---

## 🧭 Wiki Table of Contents

| Guide | Description |
| :--- | :--- |
| **[Architecture Deep Dive](Architecture-Deep-Dive)** | Internal mechanics of lock-free circular ring buffers, segmented WAL, and timing wheels. |
| **[Redis Protocol Compatibility](Redis-Protocol-Compatibility)** | Comprehensive reference of all supported RESP2 list, stream, and native `VMQ.*` commands. |
| **[Quantum Web Studio Guide](Quantum-Web-Studio)** | Visualizing pipeline topologies, real-time message streams, and 1-click DLQ replay. |
| **[Benchmarks & Performance](Benchmarks-and-Performance)** | Verified benchmark statistics, microsecond latency profiles, and reproduction scripts. |
| **[Production Deployment & Docker](Production-Deployment-and-Docker)** | Production deployment patterns, Docker scratch containers, Kubernetes, and tuning. |

---

## ⚡ Core Technical Specifications

* **Language & Runtime**: 100% Pure Go 1.23+ (Zero CGO, Zero Erlang, Zero JVM).
* **Binary Size**: 5.7 MB static binary (includes embedded Web Studio).
* **Docker Image Size**: 3.72 MB compressed (`scratch` base container, 0 CVEs).
* **Idle Memory**: <15 MB RSS RAM.
* **Cold Boot Time**: <5 milliseconds.
* **Hot-Path Ring Latency**: 5.98 nanoseconds per operation (0 B/op heap allocation).
* **Maximum Ring Throughput**: 167.1 Million operations/sec.
* **Parallel Topic Ingestion**: 2.33 Million messages/sec.
* **Durability**: Segmented memory-mapped (`mmap`) append-only commit log (WAL) with IEEE CRC32 checksums.
* **Protocols**:
  * **Redis RESP2 / Stream Protocol**: Port `8379`
  * **HTTP REST & Embedded Web Studio**: Port `8380`

---

## 🚀 Quick Navigation
* **Official Website**: [https://garganshu9468.github.io/vortexmq/](https://garganshu9468.github.io/vortexmq/)
* **GitHub Repository**: [https://github.com/GargAnshu9468/vortexmq](https://github.com/GargAnshu9468/vortexmq)
* **Docker Hub**: [hub.docker.com/r/ianshugarg/vortexmq](https://hub.docker.com/r/ianshugarg/vortexmq)
