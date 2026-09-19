package api_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexmq/internal/api"
	"github.com/GargAnshu9468/vortexmq/internal/queue"
)

func TestAPIServer_EndToEnd(t *testing.T) {
	broker := queue.NewBroker(nil)
	defer broker.Close()

	cfg := api.ServerConfig{
		Addr: "127.0.0.1:18380",
	}
	server := api.NewServer(cfg, broker)
	if err := server.Start(); err != nil {
		t.Fatalf("failed to start api server: %v", err)
	}
	time.Sleep(50 * time.Millisecond)

	baseURL := "http://127.0.0.1:18380"

	// 1. Test Static Web Studio UI Root
	resp, err := http.Get(baseURL + "/")
	if err != nil {
		t.Fatalf("failed to get studio UI: %v", err)
	}
	body, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if !strings.Contains(string(body), "VortexMQ") {
		t.Fatalf("expected studio html with 'VortexMQ', got: %s", string(body))
	}

	// 2. Test Stats API
	resp, err = http.Get(baseURL + "/api/v1/stats")
	if err != nil {
		t.Fatalf("failed to get stats: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	// 3. Test HTTP Publish API
	pubPayload := map[string]any{
		"payload": map[string]string{"event": "order_paid"},
	}
	pubJSON, _ := json.Marshal(pubPayload)
	resp, err = http.Post(baseURL+"/api/v1/topics/payments/publish", "application/json", bytes.NewReader(pubJSON))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from publish, got %d", resp.StatusCode)
	}
	var pubResp map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&pubResp)
	_ = resp.Body.Close()
	msgID, ok := pubResp["id"].(string)
	if !ok || msgID == "" {
		t.Fatalf("expected message id, got %v", pubResp)
	}

	// 4. Test HTTP Consume API
	resp, err = http.Get(baseURL + "/api/v1/topics/payments/consume?timeout_ms=100")
	if err != nil {
		t.Fatalf("failed to consume: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from consume, got %d", resp.StatusCode)
	}
	var consumedMsg queue.Message
	_ = json.NewDecoder(resp.Body).Decode(&consumedMsg)
	_ = resp.Body.Close()

	if consumedMsg.ID != msgID {
		t.Fatalf("expected msg id %s, got %s", msgID, consumedMsg.ID)
	}

	// 5. Test DLQ Replay API
	topic, _ := broker.GetTopic("payments")
	topic.DLQ().Add(&consumedMsg, "simulated error")

	replayPayload := map[string]string{
		"topic": "payments",
		"id":    msgID,
	}
	replayJSON, _ := json.Marshal(replayPayload)
	resp, err = http.Post(baseURL+"/api/v1/dlq/replay", "application/json", bytes.NewReader(replayJSON))
	if err != nil {
		t.Fatalf("failed to replay dlq: %v", err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200 OK from replay, got %d", resp.StatusCode)
	}
	_ = resp.Body.Close()

	if topic.Stats().DeadLetterCount != 0 {
		t.Fatalf("expected DLQ to be empty after replay, got %d", topic.Stats().DeadLetterCount)
	}
	fmt.Println("All API & Studio endpoints verified successfully!")
}
