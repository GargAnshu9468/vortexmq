package queue

import (
	"sync"
	"time"
)

type timerEntry struct {
	msg     *Message
	rounds  int
	next    *timerEntry
	prev    *timerEntry
}

// TimingWheel manages O(1) scheduling of delayed and retry messages.
type TimingWheel struct {
	mu          sync.Mutex
	interval    time.Duration
	slots       int
	buckets     []*timerEntry
	currentSlot int
	ticker      *time.Ticker
	quit        chan struct{}
	onExpire    func(msg *Message)
	closed      bool
}

// NewTimingWheel constructs a timing wheel with a given tick interval and slot count.
func NewTimingWheel(interval time.Duration, slots int, onExpire func(msg *Message)) *TimingWheel {
	if slots <= 0 {
		slots = 3600 // 3600 slots @ 1s = 1 hour wheel
	}
	if interval <= 0 {
		interval = 50 * time.Millisecond
	}

	tw := &TimingWheel{
		interval: interval,
		slots:    slots,
		buckets:  make([]*timerEntry, slots),
		ticker:   time.NewTicker(interval),
		quit:     make(chan struct{}),
		onExpire: onExpire,
	}

	go tw.run()
	return tw
}

func (tw *TimingWheel) run() {
	for {
		select {
		case <-tw.ticker.C:
			tw.advance()
		case <-tw.quit:
			return
		}
	}
}

// Add schedules a message to be executed after the specified delay.
func (tw *TimingWheel) Add(msg *Message, delay time.Duration) {
	if delay <= 0 {
		if tw.onExpire != nil {
			tw.onExpire(msg)
		}
		return
	}

	tw.mu.Lock()
	defer tw.mu.Unlock()

	if tw.closed {
		return
	}

	ticks := int(delay / tw.interval)
	if ticks == 0 {
		ticks = 1
	}

	rounds := ticks / tw.slots
	targetSlot := (tw.currentSlot + (ticks % tw.slots)) % tw.slots

	entry := &timerEntry{
		msg:    msg,
		rounds: rounds,
	}

	// Insert into bucket linked list
	entry.next = tw.buckets[targetSlot]
	if tw.buckets[targetSlot] != nil {
		tw.buckets[targetSlot].prev = entry
	}
	tw.buckets[targetSlot] = entry
}

func (tw *TimingWheel) advance() {
	tw.mu.Lock()
	slot := tw.currentSlot
	tw.currentSlot = (tw.currentSlot + 1) % tw.slots
	entry := tw.buckets[slot]

	var expired []*Message
	var remainingHead *timerEntry

	for entry != nil {
		next := entry.next
		if entry.rounds <= 0 {
			expired = append(expired, entry.msg)
		} else {
			entry.rounds--
			// Retain in bucket
			entry.next = remainingHead
			if remainingHead != nil {
				remainingHead.prev = entry
			}
			entry.prev = nil
			remainingHead = entry
		}
		entry = next
	}
	tw.buckets[slot] = remainingHead
	tw.mu.Unlock()

	// Dispatch outside lock to prevent blocking the wheel
	for _, m := range expired {
		if tw.onExpire != nil {
			tw.onExpire(m)
		}
	}
}

// Stop terminates the timing wheel loop.
func (tw *TimingWheel) Stop() {
	tw.mu.Lock()
	defer tw.mu.Unlock()

	if !tw.closed {
		tw.closed = true
		tw.ticker.Stop()
		close(tw.quit)
	}
}
