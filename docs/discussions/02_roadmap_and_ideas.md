# Discussion: Community Roadmap & Protocol Priorities 💡

**Category**: Ideas / RFC

---

Hi Gophers and systems engineers! ⚡

Now that **VortexMQ v1.0.0** is live with drop-in Redis RESP2 list/stream compatibility and the embedded Quantum Web Studio, we are planning the roadmap for v1.1.0 and beyond.

Here are the top protocol bridges and architectural capabilities we are evaluating:

### 1. 🔌 Native AMQP 0-9-1 Adapter
* Direct drop-in RabbitMQ protocol bridge on port `5672`.
* Allows existing Celery / Spring Boot / Pika Python services to switch from RabbitMQ without changing their AMQP connection strings.

### 2. 📡 Embedded MQTT 5.0 Broker
* Ultra-lightweight IoT telemetry ingestion directly into VortexMQ topics.
* Enables edge devices (sensors, robotics, connected vehicles) to publish QoS 0/1/2 messages with sub-millisecond queuing.

### 3. 🌐 Web-Native Server-Sent Events (SSE) & WebSockets
* Allow frontend web apps, React/Vue clients, or mobile devices to subscribe to topic streams directly over HTTP/3 or WebSockets without an intermediary gateway.

### 4. 👑 Distributed Raft Consensus Clustering
* Multi-node replication and leader election across 3 or 5 nodes for zero-downtime high-availability (HA).

---

### 🗳️ We want your vote!
Which of these would be most impactful for your tech stack? 
Are there any other protocols, metrics, or client libraries you would love to see? Let us know in the replies!
