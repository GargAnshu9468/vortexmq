package queue

import (
	"errors"
	"sync"
	"sync/atomic"
	"time"
)

// BrokerWAL defines the persistence contract between broker and WAL engine.
type BrokerWAL interface {
	WritePublish(msg *Message) error
	WriteAck(topic, group, msgID string) error
	Close() error
}

// BrokerStats aggregates cluster-wide operational metrics.
type BrokerStats struct {
	UptimeSeconds   int64        `json:"uptime_seconds"`
	TotalTopics     int          `json:"total_topics"`
	TotalPublished  uint64       `json:"total_published"`
	TotalConsumed   uint64       `json:"total_consumed"`
	TotalAcked      uint64       `json:"total_acked"`
	TotalNacked     uint64       `json:"total_nacked"`
	TotalDLQ        uint64       `json:"total_dlq"`
	Topics          []TopicStats `json:"topics"`
	AllocatedMemory uint64       `json:"allocated_memory_bytes"`
}

// Broker coordinates topics, persistence, consumer timeouts, and cluster metrics.
type Broker struct {
	mu          sync.RWMutex
	topics      map[string]*Topic
	wal         BrokerWAL
	startTime   time.Time
	closed      bool
	sweepTicker *time.Ticker
	quitSweep   chan struct{}

	totalPublished uint64
	totalConsumed  uint64
	totalAcked     uint64
	totalNacked    uint64
	totalDLQ       uint64
}

// NewBroker constructs a Broker instance with an optional persistence WAL.
func NewBroker(wal BrokerWAL) *Broker {
	b := &Broker{
		topics:      make(map[string]*Topic),
		wal:         wal,
		startTime:   time.Now(),
		sweepTicker: time.NewTicker(500 * time.Millisecond),
		quitSweep:   make(chan struct{}),
	}

	go b.runSweepLoop()
	return b
}

func (b *Broker) runSweepLoop() {
	for {
		select {
		case <-b.sweepTicker.C:
			b.sweepAllTimeouts()
		case <-b.quitSweep:
			return
		}
	}
}

func (b *Broker) sweepAllTimeouts() {
	b.mu.RLock()
	topics := make([]*Topic, 0, len(b.topics))
	for _, t := range b.topics {
		topics = append(topics, t)
	}
	b.mu.RUnlock()

	for _, t := range topics {
		_ = t.SweepConsumerTimeouts()
	}
}

// GetOrCreateTopic safely accesses or initializes a topic.
func (b *Broker) GetOrCreateTopic(name string) *Topic {
	b.mu.Lock()
	defer b.mu.Unlock()

	if t, exists := b.topics[name]; exists {
		return t
	}

	t := NewTopic(name, 131072) // 128k slot default capacity
	b.topics[name] = t
	return t
}

// GetTopic returns an existing topic or false if not found.
func (b *Broker) GetTopic(name string) (*Topic, bool) {
	b.mu.RLock()
	defer b.mu.RUnlock()
	t, ok := b.topics[name]
	return t, ok
}

// Publish enqueues a new payload to the topic and syncs to WAL if enabled.
func (b *Broker) Publish(topicName string, payload []byte) (*Message, error) {
	t := b.GetOrCreateTopic(topicName)
	msg := NewMessage(topicName, payload)

	if b.wal != nil {
		if err := b.wal.WritePublish(msg); err != nil {
			return nil, err
		}
	}

	ok := t.Publish(msg)
	if !ok {
		return nil, errors.New("topic queue rejected message (closed)")
	}

	atomic.AddUint64(&b.totalPublished, 1)
	return msg, nil
}

// PublishFront prepends a payload (LPUSH behavior).
func (b *Broker) PublishFront(topicName string, payload []byte) (*Message, error) {
	t := b.GetOrCreateTopic(topicName)
	msg := NewMessage(topicName, payload)

	if b.wal != nil {
		if err := b.wal.WritePublish(msg); err != nil {
			return nil, err
		}
	}

	ok := t.PublishFront(msg)
	if !ok {
		return nil, errors.New("topic queue rejected message (closed)")
	}

	atomic.AddUint64(&b.totalPublished, 1)
	return msg, nil
}

// PublishDelayed schedules a message for future execution.
func (b *Broker) PublishDelayed(topicName string, payload []byte, delay time.Duration) (*Message, error) {
	t := b.GetOrCreateTopic(topicName)
	msg := NewMessage(topicName, payload)

	if b.wal != nil {
		if err := b.wal.WritePublish(msg); err != nil {
			return nil, err
		}
	}

	t.PublishDelayed(msg, delay)
	atomic.AddUint64(&b.totalPublished, 1)
	return msg, nil
}

// Consume fetches the next message from the specified topic.
func (b *Broker) Consume(topicName string, timeout time.Duration) (*Message, bool) {
	t, ok := b.GetTopic(topicName)
	if !ok {
		return nil, false
	}

	msg, ok := t.ConsumeWait(timeout)
	if ok {
		atomic.AddUint64(&b.totalConsumed, 1)
	}
	return msg, ok
}

// ConsumeGroup checks out a message under a consumer group with visibility timeout.
func (b *Broker) ConsumeGroup(topicName, groupName, consumerID string, timeout time.Duration) (*Message, bool) {
	t := b.GetOrCreateTopic(topicName)
	msg, ok := t.ConsumeGroup(groupName, consumerID, timeout)
	if ok {
		atomic.AddUint64(&b.totalConsumed, 1)
	}
	return msg, ok
}

// Ack confirms processing of a message under a consumer group.
func (b *Broker) Ack(topicName, groupName, msgID string) bool {
	t, ok := b.GetTopic(topicName)
	if !ok {
		return false
	}

	acked := t.Ack(groupName, msgID)
	if acked {
		atomic.AddUint64(&b.totalAcked, 1)
		if b.wal != nil {
			_ = b.wal.WriteAck(topicName, groupName, msgID)
		}
	}
	return acked
}

// Nack marks a task as failed, triggering exponential backoff retry or DLQ routing.
func (b *Broker) Nack(topicName, groupName, msgID, reason string) bool {
	t, ok := b.GetTopic(topicName)
	if !ok {
		return false
	}

	nacked := t.Nack(groupName, msgID, reason)
	if nacked {
		atomic.AddUint64(&b.totalNacked, 1)
	}
	return nacked
}

// ReplayDLQ restores a dead-letter message back into the active queue.
func (b *Broker) ReplayDLQ(topicName, msgID string) (*Message, bool) {
	t, ok := b.GetTopic(topicName)
	if !ok {
		return nil, false
	}
	return t.ReplayDeadLetter(msgID)
}

// TopicsList returns names of all active topics.
func (b *Broker) TopicsList() []string {
	b.mu.RLock()
	defer b.mu.RUnlock()

	list := make([]string, 0, len(b.topics))
	for name := range b.topics {
		list = append(list, name)
	}
	return list
}

// Stats returns comprehensive telemetry across all topics.
func (b *Broker) Stats() BrokerStats {
	b.mu.RLock()
	topicStats := make([]TopicStats, 0, len(b.topics))
	var dlqTotal uint64
	for _, t := range b.topics {
		s := t.Stats()
		topicStats = append(topicStats, s)
		dlqTotal += s.DeadLettered
	}
	topicCount := len(b.topics)
	b.mu.RUnlock()

	return BrokerStats{
		UptimeSeconds:  int64(time.Since(b.startTime).Seconds()),
		TotalTopics:    topicCount,
		TotalPublished: atomic.LoadUint64(&b.totalPublished),
		TotalConsumed:  atomic.LoadUint64(&b.totalConsumed),
		TotalAcked:     atomic.LoadUint64(&b.totalAcked),
		TotalNacked:    atomic.LoadUint64(&b.totalNacked),
		TotalDLQ:       dlqTotal,
		Topics:         topicStats,
	}
}

// Close terminates all topics and flushes the persistence layer.
func (b *Broker) Close() error {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		return nil
	}
	b.closed = true

	b.sweepTicker.Stop()
	close(b.quitSweep)

	for _, t := range b.topics {
		t.Close()
	}

	if b.wal != nil {
		return b.wal.Close()
	}
	return nil
}
