# Benchmarks & Performance Guide 📊

This document provides complete benchmark numbers, methodology, and reproduction instructions for **VortexMQ v1.0.0**.

---

## 🏁 Summary Benchmark Results

Tested on Apple Silicon (M4 / ARM64, 10 Cores, macOS Sequoia):

| Benchmark Function | Operations/sec | Latency | Memory per Op | Allocations |
| :--- | :--- | :--- | :--- | :--- |
| **`BenchmarkRingBuffer_PushPop`** | **167.1 Million ops/sec** | **5.98 ns/op** | **0 B/op** | **0 allocs/op** |
| **`BenchmarkBroker_Publish`** | **2.33 Million msgs/sec** | **428.5 ns/op** | 279 B/op | 6 allocs/op |
| **`BenchmarkBroker_ProduceConsume`**| **1.54 Million msgs/sec** | **647.3 ns/op** | 285 B/op | 7 allocs/op |
| **`BenchmarkWAL_Write`** | **642,000 writes/sec** | **1.55 μs/op** | 689 B/op | 4 allocs/op |

---

## 🔬 Benchmark Analysis & Deep-Dive

### 1. In-Memory Ring Buffer (`5.98 ns/op`, `0 B/op`)
* **What it measures**: Direct memory queue ingestion and extraction using our bounded circular ring buffer.
* **Why it is so fast**:
  * Power-of-two capacity bitwise mask: eliminates arithmetic division instructions.
  * Zero heap allocations: reusable slice pointers ensure the Go garbage collector is never triggered.

### 2. Multi-Goroutine Parallel Publish (`2.33 Million msgs/sec`)
* **What it measures**: Concurrent publisher goroutines hammering the Broker's topic management, payload parsing, and queue ingestion simultaneously.
* **Why it scales**:
  * Sharded lock striping across topics ensures goroutines publishing to different topics experience zero lock contention.

### 3. Full Producer-Consumer Pipeline (`1.54 Million msgs/sec`)
* **What it measures**: Continuous, simultaneous publish and consume pipelines under heavy multi-client load.

### 4. Segmented Write-Ahead Log (`642,000 commits/sec`)
* **What it measures**: Raw disk serialization with 4-byte length prefix, 4-byte IEEE CRC32 checksum calculation, opcode tagging, and sequential append writes.

---

## 🛠️ How to Reproduce Benchmarks Locally

You can run the full benchmark suite on your own machine using the Go test runner:

```bash
# 1. Clone the repository
git clone https://github.com/GargAnshu9468/vortexmq.git
cd vortexmq

# 2. Run all benchmarks with memory allocation reporting
go test -benchmem -bench=. ./benchmarks/...
```

### Expected Output Format:
```text
goos: darwin
goarch: arm64
pkg: github.com/GargAnshu9468/vortexmq/benchmarks
cpu: Apple M4
BenchmarkRingBuffer_PushPop-10          196265694         5.984 ns/op          0 B/op          0 allocs/op
BenchmarkBroker_Publish-10                2816172       428.5 ns/op          279 B/op          6 allocs/op
BenchmarkBroker_ProduceConsume-10         1909074       647.3 ns/op          285 B/op          7 allocs/op
BenchmarkWAL_Write-10                      765975      1557 ns/op            689 B/op          4 allocs/op
PASS
```

---

## 🎛️ Profiling CPU and Memory with pprof

To generate flame graphs and memory allocation profiles:

```bash
# Generate CPU profile
go test -bench=BenchmarkBroker_Publish -cpuprofile=cpu.pprof ./benchmarks/...

# Inspect CPU bottlenecks with interactive pprof
go tool pprof -http=:8080 cpu.pprof

# Generate Heap Memory profile
go test -bench=BenchmarkRingBuffer_PushPop -memprofile=mem.pprof ./benchmarks/...
go tool pprof -http=:8080 mem.pprof
```
