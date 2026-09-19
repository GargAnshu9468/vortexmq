# Quantum Web Studio Guide 🌌

Every VortexMQ server instance comes with an embedded, next-generation **Quantum Web Studio** dashboard compiled directly into the binary.

Access it in any web browser at:
👉 **`http://localhost:8380`**

---

## 🎨 Design Philosophy: Cyber-Quantum Aesthetic

Unlike traditional infrastructure dashboards that look like generic bootstrap tables or require complex Grafana / Prometheus deployments, Quantum Studio is engineered with a **Gen-Beta aesthetic**:
* **Dark Obsidian Void Theme**: Deep `#07090e` base with ambient glowing cyan (`#00f3ff`) and hyper-violet (`#8a2be2`) light orbs.
* **Crisp Frosted Light Theme**: High-contrast, clean typography with subtle teal neon accents and smooth theme persistence in `localStorage`.
* **Live Reactive Telemetry**: Stream polling every 1000ms providing real-time metrics with zero UI stutter.
* **Fully Responsive**: Adapts seamlessly to 4K monitors, laptops, tablets, and smartphones.

---

## 🧭 Dashboard Tabs

### 1. 📊 Pipeline Topology
* **Live Fiber-Optic SVG Stream**: Displays animated data particles flowing in real time from **Producers** ➜ **Vortex Core (256-Shard Memory Rings)** ➜ **Workers / Consumers** ➜ **DLQ (if rejected)**.
* **High-Impact Metric Cards**:
  * **TOTAL PUBLISHED**: Cumulative ingestion count across all topics.
  * **TOTAL CONSUMED**: Total messages delivered to workers.
  * **TOTAL ACKNOWLEDGED**: Successful task completions.
  * **DEAD LETTERED (DLQ)**: Failed tasks currently isolated.

---

### 2. 📑 Topics & Queues
* Lists all active topics in a responsive grid.
* Each topic card displays:
  * **Queued Count**: Current unconsumed items in memory.
  * **Total Published & Consumed**: Throughput counters.
  * **Consumer Groups**: Active worker groups attached to this topic.
  * **DLQ Poisoned**: Failed tasks belonging to this topic.

---

### 3. 🛡️ Dead Letter Recovery Center (DLQ)
The killer operational feature of VortexMQ:
* **Interactive Table**: Displays all poisoned tasks with:
  * Unique Message ID
  * Source Topic
  * Exact Failure Cause (e.g. *"Ack deadline expired"*, *"JSON parse failed"*, *"Database lock timeout"*)
  * Attempt Counts
  * Timestamp of failure
  * Raw Payload Preview
* **⚡ 1-Click Instant Replay**:
  * Clicking **"⚡ Replay"** removes the message from DLQ, resets its retry count to 0, and re-injects it into the active queue with an immediate green toast confirmation.
* **🗑️ Purge All DLQ**: Instantly empties the dead letter storage.

---

### 4. 🚀 Direct Message Playground
* Allows developers to test topics and task routing directly in the browser without opening a terminal.
* **Quick Templates**:
  * **Order Created**: E-commerce transactional payload.
  * **User Signup**: Authentication & user registration event.
  * **AI LLM Task**: Prompt payload with model parameters and priority.
  * **IoT Telemetry**: Sensor reading with temperature and battery levels.
* **Delivery Delay Slider**: Configure delayed messages from `0ms` (immediate) to `60000ms` (1 minute).

---

## 🌐 Embedded REST API Endpoints

The Web Studio also exposes a full REST API on port `8380`:

| Method | Endpoint | Description |
| :--- | :--- | :--- |
| `GET` | `/api/v1/stats` | Returns full cluster operational metrics JSON. |
| `GET` | `/api/v1/topics` | Lists all active topic names. |
| `POST` | `/api/v1/topics/:topic/publish` | Publishes payload JSON (supports `delay_ms`). |
| `GET` | `/api/v1/topics/:topic/consume` | Consumes next task (supports `timeout_ms`). |
| `GET` | `/api/v1/dlq` | Lists all dead letter entries with error causes. |
| `POST` | `/api/v1/dlq/replay` | Replays a dead letter message back to active queue. |
| `DELETE` | `/api/v1/dlq/purge` | Purges all dead letter queues. |
