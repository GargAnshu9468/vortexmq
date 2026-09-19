package queue

import (
	"crypto/rand"
	"encoding/hex"
	"fmt"
	"sync/atomic"
	"time"
)

var globalSeq uint64

// Message represents a discrete task or event payload passing through VortexMQ.
type Message struct {
	ID           string            `json:"id"`
	Topic        string            `json:"topic"`
	Payload      []byte            `json:"payload"`
	Headers      map[string]string `json:"headers,omitempty"`
	Priority     uint8             `json:"priority"` // 0 = default, up to 255
	Timestamp    int64             `json:"timestamp"` // Unix Nano
	DeliverAt    int64             `json:"deliver_at,omitempty"` // Unix Nano for delayed messages
	RetryCount   int               `json:"retry_count"`
	MaxRetries   int               `json:"max_retries"`
	ConsumerID   string            `json:"consumer_id,omitempty"`
	AckDeadline  int64             `json:"ack_deadline,omitempty"` // Unix Nano
	FailureCause string            `json:"failure_cause,omitempty"`
}

// NewMessage creates a new Message with an optimal timestamp-ordered sequential ID.
func NewMessage(topic string, payload []byte) *Message {
	now := time.Now().UnixNano()
	seq := atomic.AddUint64(&globalSeq, 1)

	// Format: <millis>-<seq>-<rand4>
	var randBuf [2]byte
	_, _ = rand.Read(randBuf[:])
	randSuffix := hex.EncodeToString(randBuf[:])

	id := fmt.Sprintf("%d-%d-%s", now/int64(time.Millisecond), seq%100000, randSuffix)

	return &Message{
		ID:         id,
		Topic:      topic,
		Payload:    payload,
		Headers:    make(map[string]string),
		Timestamp:  now,
		DeliverAt:  now,
		MaxRetries: 3, // Default 3 retries before DLQ
	}
}

// Clone returns a shallow copy of the message with fresh headers map.
func (m *Message) Clone() *Message {
	headersCopy := make(map[string]string, len(m.Headers))
	for k, v := range m.Headers {
		headersCopy[k] = v
	}
	return &Message{
		ID:           m.ID,
		Topic:        m.Topic,
		Payload:      m.Payload,
		Headers:      headersCopy,
		Priority:     m.Priority,
		Timestamp:    m.Timestamp,
		DeliverAt:    m.DeliverAt,
		RetryCount:   m.RetryCount,
		MaxRetries:   m.MaxRetries,
		ConsumerID:   m.ConsumerID,
		AckDeadline:  m.AckDeadline,
		FailureCause: m.FailureCause,
	}
}
