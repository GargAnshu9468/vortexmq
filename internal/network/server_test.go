package network_test

import (
	"bufio"
	"net"
	"strings"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexmq/internal/network"
	"github.com/GargAnshu9468/vortexmq/internal/queue"
)

func TestTCPServer_EndToEnd(t *testing.T) {
	broker := queue.NewBroker(nil)
	defer broker.Close()

	cfg := network.ServerConfig{
		Addr: "127.0.0.1:18379", // Use isolated test port
	}
	server := network.NewServer(cfg, broker)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start server: %v", err)
	}
	defer server.Stop()

	// Wait for server to bind
	time.Sleep(50 * time.Millisecond)

	conn, err := net.Dial("tcp", "127.0.0.1:18379")
	if err != nil {
		t.Fatalf("failed to connect: %v", err)
	}
	defer conn.Close()

	rd := bufio.NewReader(conn)

	// 1. Test PING
	_, _ = conn.Write([]byte("PING\r\n"))
	resp, _ := rd.ReadString('\n')
	if !strings.HasPrefix(resp, "+PONG") {
		t.Fatalf("expected +PONG, got %s", resp)
	}

	// 2. Test RPUSH and RPOP
	_, _ = conn.Write([]byte("*3\r\n$5\r\nRPUSH\r\n$5\r\ntasks\r\n$11\r\nhello-world\r\n"))
	resp, _ = rd.ReadString('\n')
	if !strings.HasPrefix(resp, ":1") {
		t.Fatalf("expected :1, got %s", resp)
	}

	_, _ = conn.Write([]byte("*2\r\n$4\r\nRPOP\r\n$5\r\ntasks\r\n"))
	resp, _ = rd.ReadString('\n') // $11\r\n
	payload, _ := rd.ReadString('\n')
	if strings.TrimSpace(payload) != "hello-world" {
		t.Fatalf("expected 'hello-world', got %s", payload)
	}

	// 3. Test VMQ.PUBLISH and VMQ.CONSUME
	_, _ = conn.Write([]byte("*3\r\n$11\r\nVMQ.PUBLISH\r\n$6\r\nevents\r\n$10\r\nsome-event\r\n"))
	resp, _ = rd.ReadString('\n')
	if !strings.HasPrefix(resp, "$") {
		t.Fatalf("expected bulk string message ID, got %s", resp)
	}
	_, _ = rd.ReadString('\n') // read trailing ID line

	_, _ = conn.Write([]byte("*2\r\n$11\r\nVMQ.CONSUME\r\n$6\r\nevents\r\n"))
	resp, _ = rd.ReadString('\n')
	payload, _ = rd.ReadString('\n')
	if strings.TrimSpace(payload) != "some-event" {
		t.Fatalf("expected 'some-event', got %s", payload)
	}
}
