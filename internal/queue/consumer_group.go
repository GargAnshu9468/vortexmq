package queue

import (
	"sync"
	"time"
)

// PendingMessage tracks an in-flight message checked out by a consumer.
type PendingMessage struct {
	Message      *Message
	ConsumerID   string
	DeliveryTime int64 // Unix Nano
	AckDeadline  int64 // Unix Nano
}

// ConsumerGroup manages cooperative message consumption with at-least-once delivery.
type ConsumerGroup struct {
	mu             sync.RWMutex
	name           string
	topicName      string
	pel            map[string]*PendingMessage // Pending Entries List: msgID -> PendingMessage
	consumers      map[string]int64           // consumerID -> lastHeartbeat (Unix Nano)
	defaultTimeout time.Duration
	onRetry        func(msg *Message, delay time.Duration)
	onDeadLetter   func(msg *Message, cause string)
}

// NewConsumerGroup initializes a consumer group with deadline checking and recovery hooks.
func NewConsumerGroup(
	name string,
	topicName string,
	defaultTimeout time.Duration,
	onRetry func(msg *Message, delay time.Duration),
	onDeadLetter func(msg *Message, cause string),
) *ConsumerGroup {
	if defaultTimeout <= 0 {
		defaultTimeout = 30 * time.Second
	}
	return &ConsumerGroup{
		name:           name,
		topicName:      topicName,
		pel:            make(map[string]*PendingMessage),
		consumers:      make(map[string]int64),
		defaultTimeout: defaultTimeout,
		onRetry:        onRetry,
		onDeadLetter:   onDeadLetter,
	}
}

// Checkout assigns a message to a consumer and places it in the Pending Entries List (PEL).
func (cg *ConsumerGroup) Checkout(msg *Message, consumerID string, customTimeout time.Duration) {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	timeout := cg.defaultTimeout
	if customTimeout > 0 {
		timeout = customTimeout
	}

	now := time.Now().UnixNano()
	msg.ConsumerID = consumerID
	msg.AckDeadline = now + timeout.Nanoseconds()

	cg.consumers[consumerID] = now
	cg.pel[msg.ID] = &PendingMessage{
		Message:      msg,
		ConsumerID:   consumerID,
		DeliveryTime: now,
		AckDeadline:  msg.AckDeadline,
	}
}

// Ack confirms successful processing of a message and clears it from the PEL.
func (cg *ConsumerGroup) Ack(msgID string) bool {
	cg.mu.Lock()
	defer cg.mu.Unlock()

	if _, exists := cg.pel[msgID]; exists {
		delete(cg.pel, msgID)
		return true
	}
	return false
}

// Nack indicates processing failure. If retries remain, it re-queues; otherwise routes to DLQ.
func (cg *ConsumerGroup) Nack(msgID string, reason string) bool {
	cg.mu.Lock()
	pending, exists := cg.pel[msgID]
	if !exists {
		cg.mu.Unlock()
		return false
	}
	delete(cg.pel, msgID)
	cg.mu.Unlock()

	msg := pending.Message
	msg.RetryCount++
	msg.FailureCause = reason

	if msg.RetryCount >= msg.MaxRetries {
		if cg.onDeadLetter != nil {
			cg.onDeadLetter(msg, reason)
		}
	} else {
		// Exponential backoff: 100ms * 2^(retries-1)
		backoff := time.Duration(100*(1<<(msg.RetryCount-1))) * time.Millisecond
		if backoff > 10*time.Second {
			backoff = 10 * time.Second
		}
		if cg.onRetry != nil {
			cg.onRetry(msg, backoff)
		}
	}
	return true
}

// SweepTimeouts finds expired in-flight messages and handles redelivery or DLQ escalation.
func (cg *ConsumerGroup) SweepTimeouts() int {
	cg.mu.Lock()
	now := time.Now().UnixNano()
	var timedOut []*Message

	for id, pending := range cg.pel {
		if now >= pending.AckDeadline {
			timedOut = append(timedOut, pending.Message)
			delete(cg.pel, id)
		}
	}
	cg.mu.Unlock()

	for _, msg := range timedOut {
		msg.RetryCount++
		msg.FailureCause = "Ack deadline expired (worker timeout/crash)"
		if msg.RetryCount >= msg.MaxRetries {
			if cg.onDeadLetter != nil {
				cg.onDeadLetter(msg, msg.FailureCause)
			}
		} else {
			if cg.onRetry != nil {
				cg.onRetry(msg, 500*time.Millisecond)
			}
		}
	}
	return len(timedOut)
}

// PendingCount returns the number of currently checked-out messages awaiting ACK.
func (cg *ConsumerGroup) PendingCount() int {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return len(cg.pel)
}

// ActiveConsumers returns the count of known consumers in this group.
func (cg *ConsumerGroup) ActiveConsumers() int {
	cg.mu.RLock()
	defer cg.mu.RUnlock()
	return len(cg.consumers)
}

// ListPending returns a snapshot of in-flight messages for debugging and monitoring.
func (cg *ConsumerGroup) ListPending(limit int) []*PendingMessage {
	cg.mu.RLock()
	defer cg.mu.RUnlock()

	result := make([]*PendingMessage, 0, len(cg.pel))
	for _, p := range cg.pel {
		result = append(result, &PendingMessage{
			Message:      p.Message.Clone(),
			ConsumerID:   p.ConsumerID,
			DeliveryTime: p.DeliveryTime,
			AckDeadline:  p.AckDeadline,
		})
		if limit > 0 && len(result) >= limit {
			break
		}
	}
	return result
}
