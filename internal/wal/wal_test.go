package wal

import (
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexmq/internal/queue"
)

func TestWALWriteAndReplay(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "vortexmq_wal_test_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := OpenWAL(tempDir, FsyncAlways)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	for i := 0; i < 50; i++ {
		msg := &queue.Message{
			ID:        "msg-" + string(rune('A'+(i%26))),
			Topic:     "orders",
			Payload:   []byte("test order payload"),
			Timestamp: time.Now().UnixNano(),
		}
		if err := w.WritePublish(msg); err != nil {
			t.Fatalf("failed to write publish event %d: %v", i, err)
		}
	}

	if err := w.Close(); err != nil {
		t.Fatalf("failed to close WAL: %v", err)
	}

	// Replay
	replayedCount := 0
	replayed, err := Replay(tempDir, func(msg *queue.Message) {
		replayedCount++
		if msg.Topic != "orders" {
			t.Errorf("unexpected topic: %s", msg.Topic)
		}
	})

	if err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if replayed != 50 || replayedCount != 50 {
		t.Fatalf("expected 50 replayed messages, got %d", replayed)
	}
}

func TestWALCorruptedRecordReplay(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "vortexmq_wal_corrupt_*")
	if err != nil {
		t.Fatalf("failed to create temp dir: %v", err)
	}
	defer os.RemoveAll(tempDir)

	w, err := OpenWAL(tempDir, FsyncAlways)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	msg := &queue.Message{
		ID:        "msg-1",
		Topic:     "events",
		Payload:   []byte("valid event"),
		Timestamp: time.Now().UnixNano(),
	}
	_ = w.WritePublish(msg)
	_ = w.Close()

	// Append corrupt bytes to the segment file
	segPath := filepath.Join(tempDir, "vortexmq_000001.wal")
	f, err := os.OpenFile(segPath, os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		t.Fatalf("failed to open wal file: %v", err)
	}
	_, _ = f.Write([]byte{0, 0, 0, 15, 99, 88, 77, 66, 1, 2, 3}) // invalid CRC and truncated payload
	_ = f.Close()

	// Replay must gracefully recover valid messages and stop at corrupted boundary without panic
	count, err := Replay(tempDir, nil)
	if err != nil {
		t.Fatalf("expected no fatal error on corrupted frame, got: %v", err)
	}
	if count != 1 {
		t.Fatalf("expected 1 valid recovered record, got %d", count)
	}
}
