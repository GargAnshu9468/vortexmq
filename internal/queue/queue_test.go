package queue_test

import (
	"fmt"
	"os"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexmq/internal/queue"
	"github.com/GargAnshu9468/vortexmq/internal/wal"
)

func TestRingBuffer_Basic(t *testing.T) {
	rb := queue.NewRingBuffer(1024)

	if rb.Len() != 0 {
		t.Fatalf("expected initial len 0, got %d", rb.Len())
	}

	msg1 := queue.NewMessage("test", []byte("hello"))
	msg2 := queue.NewMessage("test", []byte("world"))

	rb.Push(msg1)
	rb.Push(msg2)

	if rb.Len() != 2 {
		t.Fatalf("expected len 2, got %d", rb.Len())
	}

	out1, ok := rb.Pop()
	if !ok || string(out1.Payload) != "hello" {
		t.Fatalf("unexpected pop 1: %v", out1)
	}

	out2, ok := rb.Pop()
	if !ok || string(out2.Payload) != "world" {
		t.Fatalf("unexpected pop 2: %v", out2)
	}

	if rb.Len() != 0 {
		t.Fatalf("expected empty buffer, got %d", rb.Len())
	}
}

func TestRingBuffer_Concurrent(t *testing.T) {
	rb := queue.NewRingBuffer(4096)
	numProducers := 20
	msgsPerProducer := 1000
	totalMsgs := numProducers * msgsPerProducer

	var wg sync.WaitGroup
	var receivedCount int64

	// Consumer goroutines
	numConsumers := 10
	stopConsumer := make(chan struct{})

	for c := 0; c < numConsumers; c++ {
		go func() {
			for {
				select {
				case <-stopConsumer:
					return
				default:
					msg, ok := rb.PopWait(10 * time.Millisecond)
					if ok && msg != nil {
						atomic.AddInt64(&receivedCount, 1)
					}
				}
			}
		}()
	}

	// Producer goroutines
	for p := 0; p < numProducers; p++ {
		wg.Add(1)
		go func(pID int) {
			defer wg.Done()
			for i := 0; i < msgsPerProducer; i++ {
				m := queue.NewMessage("concurrent", []byte(fmt.Sprintf("msg-%d-%d", pID, i)))
				rb.Push(m)
			}
		}(p)
	}

	wg.Wait()

	// Wait for consumers to drain
	deadline := time.Now().Add(5 * time.Second)
	for atomic.LoadInt64(&receivedCount) < int64(totalMsgs) && time.Now().Before(deadline) {
		time.Sleep(10 * time.Millisecond)
	}

	close(stopConsumer)

	if atomic.LoadInt64(&receivedCount) != int64(totalMsgs) {
		t.Fatalf("expected %d messages, received %d", totalMsgs, receivedCount)
	}
}

func TestTimingWheel_Delayed(t *testing.T) {
	var delivered atomic.Bool
	tw := queue.NewTimingWheel(20*time.Millisecond, 100, func(msg *queue.Message) {
		delivered.Store(true)
	})
	defer tw.Stop()

	msg := queue.NewMessage("delayed", []byte("delayed payload"))
	tw.Add(msg, 60*time.Millisecond)

	if delivered.Load() {
		t.Fatal("message was delivered immediately without delay")
	}

	time.Sleep(150 * time.Millisecond)

	if !delivered.Load() {
		t.Fatal("delayed message was not delivered after timeout")
	}
}

func TestConsumerGroup_AckNackAndDLQ(t *testing.T) {
	broker := queue.NewBroker(nil)
	defer broker.Close()

	topicName := "orders"
	groupName := "order-workers"
	consumerID := "worker-1"

	// Publish 1 message
	msg, err := broker.Publish(topicName, []byte(`{"order_id":1234}`))
	if err != nil {
		t.Fatalf("failed to publish: %v", err)
	}

	// Consume under group
	consumedMsg, ok := broker.ConsumeGroup(topicName, groupName, consumerID, 100*time.Millisecond)
	if !ok || consumedMsg.ID != msg.ID {
		t.Fatalf("failed to consume group message: %v", consumedMsg)
	}

	// Fail the message (NACK 1)
	broker.Nack(topicName, groupName, consumedMsg.ID, "Payment gateway unreachable")

	// Verify retry count
	topic, _ := broker.GetTopic(topicName)
	if topic.Stats().DeadLetterCount != 0 {
		t.Fatal("expected message to retry, not in DLQ yet")
	}

	// Wait for backoff retry delivery
	time.Sleep(250 * time.Millisecond)

	// Consume retry
	consumedRetry, ok := broker.ConsumeGroup(topicName, groupName, consumerID, 500*time.Millisecond)
	if !ok {
		t.Fatal("failed to consume retried message")
	}

	// Fail again until MaxRetries (default 3)
	broker.Nack(topicName, groupName, consumedRetry.ID, "Payment gateway timeout")
	time.Sleep(300 * time.Millisecond)

	consumedFinal, _ := broker.ConsumeGroup(topicName, groupName, consumerID, 500*time.Millisecond)
	if consumedFinal != nil {
		broker.Nack(topicName, groupName, consumedFinal.ID, "Permanent card failure")
	}

	// Check DLQ
	if topic.Stats().DeadLetterCount != 1 {
		t.Fatalf("expected 1 DLQ message, got %d", topic.Stats().DeadLetterCount)
	}

	// Replay DLQ
	replayed, ok := broker.ReplayDLQ(topicName, msg.ID)
	if !ok || replayed == nil {
		t.Fatal("failed to replay dead letter message")
	}

	if topic.Stats().DeadLetterCount != 0 {
		t.Fatal("expected DLQ to be empty after replay")
	}
}

func TestWAL_CrashRecovery(t *testing.T) {
	tempDir, err := os.MkdirTemp("", "vortexmq_wal_test_*")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.OpenWAL(tempDir, wal.FsyncAlways)
	if err != nil {
		t.Fatalf("failed to open WAL: %v", err)
	}

	totalWrites := 100
	for i := 0; i < totalWrites; i++ {
		m := queue.NewMessage("wal_topic", []byte(fmt.Sprintf("event-%d", i)))
		if err := w.WritePublish(m); err != nil {
			t.Fatalf("wal write failed: %v", err)
		}
	}

	_ = w.Close()

	// Replay from WAL
	replayedCount := 0
	recovered, err := wal.Replay(tempDir, func(msg *queue.Message) {
		replayedCount++
	})
	if err != nil {
		t.Fatalf("wal replay failed: %v", err)
	}

	if recovered != totalWrites || replayedCount != totalWrites {
		t.Fatalf("expected %d recovered messages, got %d (callback %d)", totalWrites, recovered, replayedCount)
	}
}
