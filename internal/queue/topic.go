package queue

import (
	"sync"
	"sync/atomic"
	"time"
)

// TopicStats aggregates real-time metrics for a topic.
type TopicStats struct {
	Name            string `json:"name"`
	Length          int    `json:"length"`
	Capacity        int    `json:"capacity"`
	Published       uint64 `json:"published"`
	Consumed        uint64 `json:"consumed"`
	Acked           uint64 `json:"acked"`
	Nacked          uint64 `json:"nacked"`
	DeadLettered    uint64 `json:"dead_lettered"`
	BytesPublished  uint64 `json:"bytes_published"`
	BytesConsumed   uint64 `json:"bytes_consumed"`
	ConsumerGroups  int    `json:"consumer_groups"`
	DeadLetterCount int    `json:"dead_letter_count"`
}

// Topic represents an independent message stream with in-memory buffering,
// durable recovery hooks, consumer groups, and dead-letter queue.
type Topic struct {
	mu     sync.RWMutex
	name   string
	ring   *RingBuffer
	groups map[string]*ConsumerGroup
	dlq    *DeadLetterQueue
	tw     *TimingWheel
	closed bool

	// Atomic counters
	published    uint64
	consumed     uint64
	acked        uint64
	nacked       uint64
	deadLettered uint64
	bytesPub     uint64
	bytesSub     uint64
}

// NewTopic instantiates a topic with ring buffer, DLQ, and timing wheel.
func NewTopic(name string, capacity uint64) *Topic {
	t := &Topic{
		name:   name,
		ring:   NewRingBuffer(capacity),
		groups: make(map[string]*ConsumerGroup),
		dlq:    NewDeadLetterQueue(50000),
	}

	// Initialize timing wheel for delayed delivery and retries
	t.tw = NewTimingWheel(50*time.Millisecond, 3600, func(msg *Message) {
		t.ring.Push(msg)
	})

	return t
}

// Name returns the topic identifier.
func (t *Topic) Name() string {
	return t.name
}

// Publish enqueues a message immediately into the active ring buffer.
func (t *Topic) Publish(msg *Message) bool {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return false
	}
	t.mu.RUnlock()

	ok := t.ring.Push(msg)
	if ok {
		atomic.AddUint64(&t.published, 1)
		atomic.AddUint64(&t.bytesPub, uint64(len(msg.Payload)))
	}
	return ok
}

// PublishFront prepends a message (LPUSH behavior).
func (t *Topic) PublishFront(msg *Message) bool {
	t.mu.RLock()
	if t.closed {
		t.mu.RUnlock()
		return false
	}
	t.mu.RUnlock()

	ok := t.ring.PushFront(msg)
	if ok {
		atomic.AddUint64(&t.published, 1)
		atomic.AddUint64(&t.bytesPub, uint64(len(msg.Payload)))
	}
	return ok
}

// PublishDelayed schedules a message for future delivery.
func (t *Topic) PublishDelayed(msg *Message, delay time.Duration) {
	msg.DeliverAt = time.Now().Add(delay).UnixNano()
	atomic.AddUint64(&t.published, 1)
	atomic.AddUint64(&t.bytesPub, uint64(len(msg.Payload)))
	t.tw.Add(msg, delay)
}

// Consume extracts the next message without consumer group tracking (simple queue semantics).
func (t *Topic) Consume() (*Message, bool) {
	msg, ok := t.ring.Pop()
	if ok {
		atomic.AddUint64(&t.consumed, 1)
		atomic.AddUint64(&t.bytesSub, uint64(len(msg.Payload)))
	}
	return msg, ok
}

// ConsumeWait extracts the next message, blocking up to timeout if empty.
func (t *Topic) ConsumeWait(timeout time.Duration) (*Message, bool) {
	msg, ok := t.ring.PopWait(timeout)
	if ok {
		atomic.AddUint64(&t.consumed, 1)
		atomic.AddUint64(&t.bytesSub, uint64(len(msg.Payload)))
	}
	return msg, ok
}

// GetOrCreateGroup fetches or initializes a consumer group for cooperative consumption.
func (t *Topic) GetOrCreateGroup(groupName string, defaultTimeout time.Duration) *ConsumerGroup {
	t.mu.Lock()
	defer t.mu.Unlock()

	if group, exists := t.groups[groupName]; exists {
		return group
	}

	group := NewConsumerGroup(
		groupName,
		t.name,
		defaultTimeout,
		func(msg *Message, delay time.Duration) {
			// On retry with backoff
			t.tw.Add(msg, delay)
		},
		func(msg *Message, cause string) {
			// On dead letter
			t.dlq.Add(msg, cause)
			atomic.AddUint64(&t.deadLettered, 1)
		},
	)
	t.groups[groupName] = group
	return group
}

// ConsumeGroup checks out a message under a consumer group with visibility timeout tracking.
func (t *Topic) ConsumeGroup(groupName, consumerID string, timeout time.Duration) (*Message, bool) {
	group := t.GetOrCreateGroup(groupName, 30*time.Second)

	msg, ok := t.ring.PopWait(timeout)
	if !ok {
		return nil, false
	}

	group.Checkout(msg, consumerID, 30*time.Second)
	atomic.AddUint64(&t.consumed, 1)
	atomic.AddUint64(&t.bytesSub, uint64(len(msg.Payload)))
	return msg, true
}

// Ack confirms processing under a consumer group.
func (t *Topic) Ack(groupName, msgID string) bool {
	t.mu.RLock()
	group, exists := t.groups[groupName]
	t.mu.RUnlock()

	if !exists {
		return false
	}

	ok := group.Ack(msgID)
	if ok {
		atomic.AddUint64(&t.acked, 1)
	}
	return ok
}

// Nack rejects a message under a consumer group, initiating retry or DLQ routing.
func (t *Topic) Nack(groupName, msgID, reason string) bool {
	t.mu.RLock()
	group, exists := t.groups[groupName]
	t.mu.RUnlock()

	if !exists {
		return false
	}

	ok := group.Nack(msgID, reason)
	if ok {
		atomic.AddUint64(&t.nacked, 1)
	}
	return ok
}

// DLQ returns the DeadLetterQueue instance for inspection and management.
func (t *Topic) DLQ() *DeadLetterQueue {
	return t.dlq
}

// ReplayDeadLetter restores a failed message from DLQ back to the active queue.
func (t *Topic) ReplayDeadLetter(msgID string) (*Message, bool) {
	msg, ok := t.dlq.Remove(msgID)
	if !ok {
		return nil, false
	}

	msg.RetryCount = 0
	msg.FailureCause = ""
	msg.DeliverAt = time.Now().UnixNano()
	t.ring.Push(msg)
	return msg, true
}

// Stats returns a snapshot of topic operational metrics.
func (t *Topic) Stats() TopicStats {
	t.mu.RLock()
	groupCount := len(t.groups)
	t.mu.RUnlock()

	return TopicStats{
		Name:            t.name,
		Length:          t.ring.Len(),
		Capacity:        t.ring.Cap(),
		Published:       atomic.LoadUint64(&t.published),
		Consumed:        atomic.LoadUint64(&t.consumed),
		Acked:           atomic.LoadUint64(&t.acked),
		Nacked:          atomic.LoadUint64(&t.nacked),
		DeadLettered:    atomic.LoadUint64(&t.deadLettered),
		BytesPublished:  atomic.LoadUint64(&t.bytesPub),
		BytesConsumed:   atomic.LoadUint64(&t.bytesSub),
		ConsumerGroups:  groupCount,
		DeadLetterCount: t.dlq.Len(),
	}
}

// SweepConsumerTimeouts inspects all groups for expired pending messages.
func (t *Topic) SweepConsumerTimeouts() int {
	t.mu.RLock()
	groups := make([]*ConsumerGroup, 0, len(t.groups))
	for _, g := range t.groups {
		groups = append(groups, g)
	}
	t.mu.RUnlock()

	total := 0
	for _, g := range groups {
		total += g.SweepTimeouts()
	}
	return total
}

// Close gracefully closes the topic and its worker timers.
func (t *Topic) Close() {
	t.mu.Lock()
	defer t.mu.Unlock()

	if !t.closed {
		t.closed = true
		t.ring.Close()
		t.tw.Stop()
	}
}
