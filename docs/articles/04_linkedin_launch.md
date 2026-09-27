# LinkedIn Launch Post

### Post Copy:

I’m excited to share an open-source project I’ve been building: **VortexMQ** 🚀

If you’ve ever deployed Kafka or RabbitMQ for microservices or background job queues, you’re familiar with the infrastructure overhead: heavy JVM or Erlang runtimes, multi-hundred megabyte memory baselines, external cluster coordinators, and GC pauses that cause unpredictable p99 latency spikes.

I wanted a message broker engineered with the simplicity and performance of Go: a single 5.7 MB static binary, sub-microsecond latency, zero runtime bloat, and zero configuration.

Here is what **VortexMQ** delivers:

• **167.1 Million ops/sec** lock-free ring buffer throughput (5.98 ns/op on bare metal with 0 B/op heap allocation)  
• **3.57 Million ops/sec** Network Read (`LPOP`, matching NATS Core) & **1.07 Million writes/sec** Durable Disk WAL (surpassing single-node Apache Kafka)  
• **Drop-in Redis RESP2/RESP3 Compatibility**: Connect instantly using standard Redis SDKs in Go, Python, Node.js, Rust, or Java without learning a new API  
• **Hierarchical Timing Wheel**: O(1) delayed message scheduling without the CPU overhead of polling sorted sets  
• **Poison-Pill Quarantine**: Built-in panic recovery catches crashing consumer payloads and isolates them to a Dead Letter Queue (DLQ) with 1-click GUI replay  
• **Embedded Quantum Web Studio**: A real-time dashboard served directly from the 5.7 MB binary on port 8380 via Go `embed.FS`  
• **Under 15 MB RAM** idle footprint

You can test it locally with Docker in seconds:
`docker run -d -p 8379:8379 -p 8380:8380 ianshugarg/vortexmq:latest`

Explore the project:
⭐ GitHub: https://github.com/GargAnshu9468/vortexmq  
🖥️ Documentation & Live Sandbox: https://garganshu9468.github.io/vortexmq/  
📖 Technical Wiki: https://github.com/GargAnshu9468/vortexmq/wiki  

Feedback, discussions, and contributions are very welcome!

#golang #distributedsystems #softwareengineering #opensource #backend #systemsdesign #performance #docker
