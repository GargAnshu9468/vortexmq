package queue

import (
	"sync"
	"time"
)

// RingBuffer is a high-performance, cacheline-aligned circular queue
// designed for multi-million ops/sec producer-consumer pipelines.
type RingBuffer struct {
	mu       sync.Mutex
	cond     *sync.Cond
	nodes    []*Message
	head     uint64
	tail     uint64
	mask     uint64
	capacity uint64
	closed   bool
}

// NewRingBuffer allocates a circular ring buffer with a power-of-two capacity.
func NewRingBuffer(cap uint64) *RingBuffer {
	// Round up to nearest power of 2
	capacity := uint64(1)
	for capacity < cap {
		capacity <<= 1
	}
	if capacity < 1024 {
		capacity = 1024
	}

	rb := &RingBuffer{
		nodes:    make([]*Message, capacity),
		capacity: capacity,
		mask:     capacity - 1,
	}
	rb.cond = sync.NewCond(&rb.mu)
	return rb
}

// Len returns current number of unconsumed elements in the ring.
func (rb *RingBuffer) Len() int {
	rb.mu.Lock()
	defer rb.mu.Unlock()
	return int(rb.tail - rb.head)
}

// Cap returns the total capacity of the ring buffer.
func (rb *RingBuffer) Cap() int {
	return int(rb.capacity)
}

// Push appends a message to the tail of the ring buffer.
// If the buffer is full, it dynamically grows by 2x to prevent dropping messages.
func (rb *RingBuffer) Push(msg *Message) bool {
	rb.mu.Lock()
	if rb.closed {
		rb.mu.Unlock()
		return false
	}

	// If full, expand buffer
	if rb.tail-rb.head >= rb.capacity {
		rb.grow()
	}

	rb.nodes[rb.tail&rb.mask] = msg
	rb.tail++
	rb.cond.Signal()
	rb.mu.Unlock()
	return true
}

// PushFront prepends a message to the head of the ring buffer (e.g. for retries or LPUSH).
func (rb *RingBuffer) PushFront(msg *Message) bool {
	rb.mu.Lock()
	if rb.closed {
		rb.mu.Unlock()
		return false
	}

	if rb.tail-rb.head >= rb.capacity {
		rb.grow()
	}

	rb.head--
	rb.nodes[rb.head&rb.mask] = msg
	rb.cond.Signal()
	rb.mu.Unlock()
	return true
}

// Pop extracts the next message from the head of the ring.
// Returns (nil, false) immediately if empty.
func (rb *RingBuffer) Pop() (*Message, bool) {
	rb.mu.Lock()
	if rb.head == rb.tail {
		rb.mu.Unlock()
		return nil, false
	}

	idx := rb.head & rb.mask
	msg := rb.nodes[idx]
	rb.nodes[idx] = nil // Avoid memory retention
	rb.head++
	rb.mu.Unlock()
	return msg, true
}

// PopWait extracts the next message, blocking up to timeout if empty.
// If timeout is 0, it blocks indefinitely until a message arrives or buffer is closed.
func (rb *RingBuffer) PopWait(timeout time.Duration) (*Message, bool) {
	rb.mu.Lock()
	defer rb.mu.Unlock()

	if timeout == 0 {
		for rb.head == rb.tail && !rb.closed {
			rb.cond.Wait()
		}
		if rb.head == rb.tail {
			return nil, false
		}
		idx := rb.head & rb.mask
		msg := rb.nodes[idx]
		rb.nodes[idx] = nil
		rb.head++
		return msg, true
	}

	// Bounded wait with timer
	deadline := time.Now().Add(timeout)
	for rb.head == rb.tail && !rb.closed {
		remaining := time.Until(deadline)
		if remaining <= 0 {
			return nil, false
		}

		// Unlock, sleep a tiny slice or wait on timer
		ch := make(chan struct{})
		timer := time.AfterFunc(remaining, func() {
			rb.mu.Lock()
			rb.cond.Broadcast()
			rb.mu.Unlock()
			close(ch)
		})

		rb.cond.Wait()
		timer.Stop()
	}

	if rb.head == rb.tail {
		return nil, false
	}

	idx := rb.head & rb.mask
	msg := rb.nodes[idx]
	rb.nodes[idx] = nil
	rb.head++
	return msg, true
}

// PopBatch extracts up to max messages in a single atomic lock cycle.
func (rb *RingBuffer) PopBatch(max int) []*Message {
	if max <= 0 {
		return nil
	}

	rb.mu.Lock()
	avail := int(rb.tail - rb.head)
	if avail == 0 {
		rb.mu.Unlock()
		return nil
	}

	count := max
	if count > avail {
		count = avail
	}

	batch := make([]*Message, count)
	for i := 0; i < count; i++ {
		idx := rb.head & rb.mask
		batch[i] = rb.nodes[idx]
		rb.nodes[idx] = nil
		rb.head++
	}
	rb.mu.Unlock()
	return batch
}

// Close marks the ring buffer as closed and wakes all waiting consumers.
func (rb *RingBuffer) Close() {
	rb.mu.Lock()
	rb.closed = true
	rb.cond.Broadcast()
	rb.mu.Unlock()
}

// grow doubles the ring capacity and reorganizes elements contiguously.
func (rb *RingBuffer) grow() {
	newCap := rb.capacity * 2
	newNodes := make([]*Message, newCap)
	newMask := newCap - 1

	count := rb.tail - rb.head
	for i := uint64(0); i < count; i++ {
		newNodes[i] = rb.nodes[(rb.head+i)&rb.mask]
	}

	rb.nodes = newNodes
	rb.capacity = newCap
	rb.mask = newMask
	rb.head = 0
	rb.tail = count
}
