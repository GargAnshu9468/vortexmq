package queue

import (
	"sync"
	"time"
)

// DeadLetterEntry represents a failed task retained for investigation and replay.
type DeadLetterEntry struct {
	Message      *Message `json:"message"`
	FailedAt     int64    `json:"failed_at"` // Unix Nano
	FailureCause string   `json:"failure_cause"`
	Attempts     int      `json:"attempts"`
}

// DeadLetterQueue stores failed tasks and facilitates one-click replay.
type DeadLetterQueue struct {
	mu      sync.RWMutex
	entries map[string]*DeadLetterEntry
	order   []string // FIFO order of dead letter IDs
	maxSize int
}

// NewDeadLetterQueue creates a DLQ with a bounded capacity.
func NewDeadLetterQueue(maxSize int) *DeadLetterQueue {
	if maxSize <= 0 {
		maxSize = 50000
	}
	return &DeadLetterQueue{
		entries: make(map[string]*DeadLetterEntry),
		order:   make([]string, 0, 1024),
		maxSize: maxSize,
	}
}

// Add stores a failed message in the DLQ.
func (dlq *DeadLetterQueue) Add(msg *Message, cause string) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	// If at capacity, evict oldest
	if len(dlq.entries) >= dlq.maxSize && len(dlq.order) > 0 {
		oldestID := dlq.order[0]
		dlq.order = dlq.order[1:]
		delete(dlq.entries, oldestID)
	}

	entry := &DeadLetterEntry{
		Message:      msg.Clone(),
		FailedAt:     time.Now().UnixNano(),
		FailureCause: cause,
		Attempts:     msg.RetryCount,
	}

	if _, exists := dlq.entries[msg.ID]; !exists {
		dlq.order = append(dlq.order, msg.ID)
	}
	dlq.entries[msg.ID] = entry
}

// Get returns a specific dead letter entry by message ID.
func (dlq *DeadLetterQueue) Get(id string) (*DeadLetterEntry, bool) {
	dlq.mu.RUnlock()
	defer dlq.mu.RUnlock()
	entry, ok := dlq.entries[id]
	return entry, ok
}

// List returns a slice of dead letter entries with pagination.
func (dlq *DeadLetterQueue) List(offset, limit int) []*DeadLetterEntry {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()

	total := len(dlq.order)
	if offset >= total {
		return nil
	}

	end := offset + limit
	if end > total {
		end = total
	}

	result := make([]*DeadLetterEntry, 0, end-offset)
	for i := offset; i < end; i++ {
		id := dlq.order[i]
		if entry, ok := dlq.entries[id]; ok {
			result = append(result, entry)
		}
	}
	return result
}

// Remove removes an entry from DLQ (e.g. after successful replay or dismissal).
func (dlq *DeadLetterQueue) Remove(id string) (*Message, bool) {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	entry, exists := dlq.entries[id]
	if !exists {
		return nil, false
	}

	delete(dlq.entries, id)
	for i, oid := range dlq.order {
		if oid == id {
			dlq.order = append(dlq.order[:i], dlq.order[i+1:]...)
			break
		}
	}
	return entry.Message, true
}

// Len returns the current count of dead letter messages.
func (dlq *DeadLetterQueue) Len() int {
	dlq.mu.RLock()
	defer dlq.mu.RUnlock()
	return len(dlq.entries)
}

// Purge empties the entire DLQ.
func (dlq *DeadLetterQueue) Purge() int {
	dlq.mu.Lock()
	defer dlq.mu.Unlock()

	count := len(dlq.entries)
	dlq.entries = make(map[string]*DeadLetterEntry)
	dlq.order = dlq.order[:0]
	return count
}
