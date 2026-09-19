# Production Deployment & Docker Guide 🚀

This guide covers running **VortexMQ** in mission-critical production environments using Docker, Docker Compose, Kubernetes, or native Linux systemd services.

---

## 🐳 Option 1: Docker (Single Command Run)

Run the official, multi-arch scratch container:

```bash
docker run -d \
  --name vortexmq \
  --restart unless-stopped \
  -p 8379:8379 \
  -p 8380:8380 \
  -v vortexmq_data:/data \
  ianshugarg/vortexmq:latest
```

* **`-p 8379:8379`**: Exposes the Redis RESP2/Stream client broker.
* **`-p 8380:8380`**: Exposes the Quantum Web Studio and REST API.
* **`-v vortexmq_data:/data`**: Mounts a persistent volume for the Write-Ahead Log (WAL).

---

## 📦 Option 2: Docker Compose

Create a `docker-compose.yml` file:

```yaml
version: '3.8'

services:
  vortexmq:
    image: ianshugarg/vortexmq:latest
    container_name: vortexmq
    restart: unless-stopped
    ports:
      - "8379:8379"
      - "8380:8380"
    volumes:
      - vmq_data:/data
    command:
      - "-dir=/data"
      - "-broker-port=8379"
      - "-studio-port=8380"
      - "-fsync=everysec"
      - "-password=${VORTEXMQ_PASSWORD:-}"
    deploy:
      resources:
        limits:
          memory: 512M
        reservations:
          memory: 32M

volumes:
  vmq_data:
    driver: local
```

Start the stack:
```bash
docker compose up -d
```

---

## ☸️ Option 3: Kubernetes Deployment

Create a `vortexmq-deployment.yaml` manifest:

```yaml
apiVersion: apps/v1
kind: Deployment
metadata:
  name: vortexmq
  labels:
    app: vortexmq
spec:
  replicas: 1
  selector:
    matchLabels:
      app: vortexmq
  template:
    metadata:
      labels:
        app: vortexmq
    spec:
      containers:
      - name: vortexmq
        image: ianshugarg/vortexmq:latest
        imagePullPolicy: IfNotPresent
        ports:
        - name: broker
          containerPort: 8379
        - name: studio
          containerPort: 8380
        volumeMounts:
        - name: data-volume
          mountPath: /data
        resources:
          requests:
            memory: "32Mi"
            cpu: "100m"
          limits:
            memory: "512Mi"
            cpu: "2000m"
      volumes:
      - name: data-volume
        persistentVolumeClaim:
          claimName: vortexmq-pvc
---
apiVersion: v1
kind: Service
metadata:
  name: vortexmq
spec:
  selector:
    app: vortexmq
  ports:
  - name: broker
    port: 8379
    targetPort: 8379
  - name: studio
    port: 8380
    targetPort: 8380
  type: ClusterIP
```

---

## 🐧 Option 4: Linux Native `systemd` Service

If deploying directly to Ubuntu/Debian/RHEL bare metal or VMs:

1. Download the statically linked binary:
   ```bash
   curl -LO https://github.com/GargAnshu9468/vortexmq/releases/download/v1.0.0/vortexmq-v1.0.0-linux-amd64.tar.gz
   tar -xzf vortexmq-v1.0.0-linux-amd64.tar.gz
   sudo mv vortexmq-v1.0.0-linux-amd64/vortexmq /usr/local/bin/
   sudo chmod +x /usr/local/bin/vortexmq
   ```

2. Create a data directory:
   ```bash
   sudo mkdir -p /var/lib/vortexmq
   sudo useradd -r -s /bin/false vortexmq
   sudo chown -R vortexmq:vortexmq /var/lib/vortexmq
   ```

3. Create the systemd service file:
   `/etc/systemd/system/vortexmq.service`:
   ```ini
   [Unit]
   Description=VortexMQ High-Throughput Message Broker
   After=network.target

   [Service]
   Type=simple
   User=vortexmq
   Group=vortexmq
   ExecStart=/usr/local/bin/vortexmq -dir /var/lib/vortexmq -broker-port 8379 -studio-port 8380 -fsync everysec
   Restart=always
   RestartSec=3
   LimitNOFILE=65536

   [Install]
   WantedBy=multi-user.target
   ```

4. Enable and start:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable --now vortexmq
   sudo systemctl status vortexmq
   ```

---

## ⚙️ Server Configuration CLI Flags

| Flag | Default | Description |
| :--- | :--- | :--- |
| **`-broker-port`** | `8379` | TCP port for Redis RESP2 and Stream client connections. |
| **`-studio-port`** | `8380` | HTTP port for the embedded Quantum Web Studio & REST API. |
| **`-dir`** | `./data` | Directory where Write-Ahead Log (WAL) segments are stored. |
| **`-wal`** | `true` | Enables/disables disk persistence. Set to `false` for pure in-memory mode. |
| **`-fsync`** | `everysec` | Fsync policy: `always` (maximum durability), `everysec` (balanced), or `none` (max speed). |
| **`-password`** | `""` | Optional authentication password (enforces `AUTH <password>` requirement). |
