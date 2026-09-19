package benchmarks

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/GargAnshu9468/vortexmq/internal/queue"
	"github.com/GargAnshu9468/vortexmq/internal/wal"
)

func BenchmarkRingBuffer_PushPop(b *testing.B) {
	rb := queue.NewRingBuffer(1048576) // 1M capacity
	msg := queue.NewMessage("bench", []byte("benchmark-payload-bytes"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		rb.Push(msg)
		_, _ = rb.Pop()
	}
}

func BenchmarkBroker_Publish(b *testing.B) {
	broker := queue.NewBroker(nil) // Pure memory mode
	defer broker.Close()

	topic := "high-throughput-topic"
	payload := []byte(`{"event":"click","user":9482,"timestamp":1710000000}`)

	b.ResetTimer()
	b.ReportAllocs()

	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _ = broker.Publish(topic, payload)
		}
	})
}

func BenchmarkBroker_ProduceConsume(b *testing.B) {
	broker := queue.NewBroker(nil)
	defer broker.Close()

	topic := "pipeline-topic"
	payload := []byte("standard-task-payload")

	b.ResetTimer()
	b.ReportAllocs()

	var wg sync.WaitGroup
	workers := 8
	perWorker := b.N / workers
	if perWorker == 0 {
		perWorker = 1
	}

	for w := 0; w < workers; w++ {
		wg.Add(2)
		// Producer
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				_, _ = broker.Publish(topic, payload)
			}
		}()
		// Consumer
		go func() {
			defer wg.Done()
			for i := 0; i < perWorker; i++ {
				_, _ = broker.Consume(topic, 50*time.Millisecond)
			}
		}()
	}

	wg.Wait()
}

func BenchmarkWAL_Write(b *testing.B) {
	tempDir, err := os.MkdirTemp("", "vortexmq_bench_wal_*")
	if err != nil {
		b.Fatal(err)
	}
	defer os.RemoveAll(tempDir)

	w, err := wal.OpenWAL(tempDir, wal.FsyncNone) // Test raw disk throughput
	if err != nil {
		b.Fatal(err)
	}
	defer w.Close()

	msg := queue.NewMessage("wal-topic", []byte("wal-benchmark-record-payload"))

	b.ResetTimer()
	b.ReportAllocs()

	for i := 0; i < b.N; i++ {
		_ = w.WritePublish(msg)
	}
}
