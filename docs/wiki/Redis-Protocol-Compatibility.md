# Redis Protocol Compatibility Reference 🔌

VortexMQ provides native drop-in compatibility with the **Redis RESP2 / RESP3 wire protocol** on port **`8379`**.

This allows teams to migrate existing applications written in **Python, Go, Node.js, Java, Rust, or C#** to VortexMQ with **zero code changes**—simply point your Redis client host and port to `localhost:8379`.

---

## 📋 Supported Commands Matrix

### 1. List / Simple Queue Commands
| Command | Syntax | Description |
| :--- | :--- | :--- |
| **`LPUSH`** | `LPUSH <topic> <msg1> [msg2 ...]` | Prepends one or multiple messages to the topic ring. Returns queue length. |
| **`RPUSH`** | `RPUSH <topic> <msg1> [msg2 ...]` | Appends one or multiple messages to the topic ring. Returns queue length. |
| **`LPOP`** | `LPOP <topic>` | Extracts and removes the first message from the topic. |
| **`RPOP`** | `RPOP <topic>` | Extracts and removes the latest message from the topic. |
| **`BLPOP`** | `BLPOP <topic> <timeout_sec>` | Blocking pop from the head of the topic with timeout in seconds. |
| **`BRPOP`** | `BRPOP <topic> <timeout_sec>` | Blocking pop from the tail of the topic with timeout in seconds. |
| **`LLEN`** | `LLEN <topic>` | Returns the current unconsumed message count of the topic. |

---

### 2. Stream & Consumer Group Commands
| Command | Syntax | Description |
| :--- | :--- | :--- |
| **`XADD`** | `XADD <topic> * <field> <value> [...]` | Appends a structured message stream entry. Returns unique Message ID. |
| **`XGROUP`** | `XGROUP CREATE <topic> <group> $ [MKSTREAM]` | Initializes a consumer group for cooperative worker scaling. |
| **`XREADGROUP`** | `XREADGROUP GROUP <group> <consumer> [BLOCK ms] STREAMS <topic> >` | Checks out in-flight tasks with visibility deadlines. |
| **`XACK`** | `XACK <topic> <group> <id1> [id2 ...]` | Confirms successful task completion and purges message from PEL. |

---

### 3. Server Administration Commands
| Command | Syntax | Description |
| :--- | :--- | :--- |
| **`PING`** | `PING [message]` | Tests server connectivity; returns `+PONG` or echoed message. |
| **`ECHO`** | `ECHO <message>` | Echoes back the provided message string. |
| **`AUTH`** | `AUTH <password>` | Authenticates the connection if a password is configured. |
| **`INFO`** | `INFO [section]` | Returns cluster uptime, topic counts, throughput, and memory stats. |
| **`COMMAND`** | `COMMAND` | Returns empty array for client initialization compatibility. |
| **`QUIT`** | `QUIT` | Closes the client connection gracefully. |

---

### 4. Native High-Speed VortexMQ Commands (`VMQ.*`)
For maximum throughput and native features like delayed tasks and DLQ replay:

| Command | Syntax | Description |
| :--- | :--- | :--- |
| **`VMQ.PUBLISH`** | `VMQ.PUBLISH <topic> <payload> [delay_ms]` | High-speed publish with optional millisecond delay. |
| **`VMQ.CONSUME`** | `VMQ.CONSUME <topic> [timeout_ms]` | High-speed consume with optional millisecond timeout. |
| **`VMQ.ACK`** | `VMQ.ACK <topic> <group> <msg_id>` | Confirms task completion under consumer group. |
| **`VMQ.NACK`** | `VMQ.NACK <topic> <group> <msg_id> [reason]` | Rejects task, triggering exponential backoff retry or DLQ routing. |
| **`VMQ.REPLAY`** | `VMQ.REPLAY <topic> <msg_id>` | Restores a failed message from DLQ back into the active queue. |
| **`VMQ.STATS`** | `VMQ.STATS` | Returns full JSON telemetry across all active topics and memory rings. |

---

## 💻 Client Code Examples

### Python (`redis-py`)
```python
import redis

# Connect directly to VortexMQ
r = redis.Redis(host='localhost', port=8379, db=0)

# Publish an event
r.rpush('orders', '{"order_id": 9482, "amount": 149.50}')

# Blocking consumer worker
topic, payload = r.brpop('orders', timeout=5)
print(f"Processed order: {payload.decode('utf-8')}")
```

### Go (`go-redis`)
```go
package main

import (
	"context"
	"fmt"
	"github.com/redis/go-redis/v9"
)

func main() {
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:8379",
	})
	ctx := context.Background()

	// Push task
	rdb.RPush(ctx, "tasks", "process_report")

	// Pop task
	val, _ := rdb.RPop(ctx, "tasks").Result()
	fmt.Printf("Received task: %s\n", val)
}
```

### Node.js (`ioredis`)
```javascript
const Redis = require('ioredis');
const vmq = new Redis({ host: 'localhost', port: 8379 });

async function run() {
  await vmq.rpush('notifications', JSON.stringify({ user: 'usr_1', msg: 'Welcome!' }));
  const task = await vmq.rpop('notifications');
  console.log('Received notification:', JSON.parse(task));
}
run();
```
