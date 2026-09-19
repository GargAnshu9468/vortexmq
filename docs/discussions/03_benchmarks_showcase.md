# Discussion: Benchmark Reproduction & Hardware Showcase ⚡

**Category**: Show and Tell

---

Hey systems enthusiasts! 🏎️

We measured **167.1 Million operations/sec** on our in-memory lock-free ring buffers and **2.33 Million msgs/sec** parallel topic publishes on an Apple Silicon M4 (10 cores).

We would love to see how VortexMQ performs on your hardware!

### 🛠️ How to Run the Benchmarks:

```bash
git clone https://github.com/GargAnshu9468/vortexmq.git
cd vortexmq
go test -benchmem -bench=. ./benchmarks/...
```

---

### 📝 Share Your Results:
Reply with:
1. **CPU / Architecture** (e.g. AMD Ryzen 9 7950X, Intel Core i9-14900K, AWS Graviton 3/4, Raspberry Pi 5)
2. **OS & Go Version**
3. **Your `go test -benchmem` output**

Let's see who can push the ring buffer and publish benchmarks to the highest throughput! 🚀
