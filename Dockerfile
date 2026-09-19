# Build stage
FROM golang:1.24-alpine AS builder

WORKDIR /app

# Install git and ca-certificates
RUN apk update && apk add --no-cache git ca-certificates tzdata

COPY go.mod ./
# COPY go.sum ./
RUN go mod download || true

COPY . .

# Build statically linked binaries
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags="-w -s -extldflags '-static'" -o /vortexmq cmd/vortexmq/main.go
RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build -a -ldflags="-w -s -extldflags '-static'" -o /vortexmq-cli cmd/vortexmq-cli/main.go

# Scratch runtime stage
FROM scratch

COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder /usr/share/zoneinfo /usr/share/zoneinfo
COPY --from=builder /vortexmq /vortexmq
COPY --from=builder /vortexmq-cli /vortexmq-cli

# Default data directory
VOLUME ["/data"]

# Expose ports: 8379 (Core Broker/RESP), 8380 (Web Studio / REST / WebSockets)
EXPOSE 8379 8380

ENTRYPOINT ["/vortexmq"]
CMD ["-dir", "/data", "-broker-port", "8379", "-studio-port", "8380"]
