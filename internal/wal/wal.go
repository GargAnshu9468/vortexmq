package wal

import (
	"encoding/binary"
	"encoding/json"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/GargAnshu9468/vortexmq/internal/queue"
)

const (
	OpPublish byte = 1
	OpAck     byte = 2
	OpNack    byte = 3
	OpDLQ     byte = 4
)

// FsyncPolicy defines how often the write-ahead log calls fsync to disk.
type FsyncPolicy int

const (
	FsyncEverySec FsyncPolicy = iota
	FsyncAlways
	FsyncNone
)

// WAL manages segmented, CRC32-checksummed append-only logs for durability.
type WAL struct {
	mu          sync.Mutex
	dir         string
	policy      FsyncPolicy
	activeFile  *os.File
	segmentID   int
	maxSegSize  int64
	currentSize int64
	closed      bool
	syncTicker  *time.Ticker
	quitSync    chan struct{}
}

// OpenWAL initializes or recovers a WAL instance in the specified directory.
func OpenWAL(dir string, policy FsyncPolicy) (*WAL, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("failed to create wal directory: %w", err)
	}

	w := &WAL{
		dir:        dir,
		policy:     policy,
		maxSegSize: 64 * 1024 * 1024, // 64 MB per segment
		quitSync:   make(chan struct{}),
	}

	// Open or create current segment
	segPath := filepath.Join(dir, "vortexmq_000001.wal")
	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err != nil {
		return nil, fmt.Errorf("failed to open wal segment: %w", err)
	}
	stat, _ := f.Stat()
	w.activeFile = f
	w.segmentID = 1
	if stat != nil {
		w.currentSize = stat.Size()
	}

	if policy == FsyncEverySec {
		w.syncTicker = time.NewTicker(1 * time.Second)
		go w.periodicFsync()
	}

	return w, nil
}

func (w *WAL) periodicFsync() {
	for {
		select {
		case <-w.syncTicker.C:
			w.Sync()
		case <-w.quitSync:
			return
		}
	}
}

// WritePublish records a message publication event to the commit log.
func (w *WAL) WritePublish(msg *queue.Message) error {
	data, err := json.Marshal(msg)
	if err != nil {
		return err
	}
	return w.writeRecord(OpPublish, data)
}

// WriteAck records an acknowledgment event.
func (w *WAL) WriteAck(topic, group, msgID string) error {
	payload := fmt.Sprintf("%s:%s:%s", topic, group, msgID)
	return w.writeRecord(OpAck, []byte(payload))
}

func (w *WAL) writeRecord(op byte, payload []byte) error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return errors.New("wal is closed")
	}

	// Record wire format:
	// [4 bytes TotalPayloadLen][4 bytes CRC32][1 byte OpCode][N bytes Payload]
	payloadLen := uint32(len(payload))
	totalLen := 4 + 4 + 1 + payloadLen

	buf := make([]byte, totalLen)
	binary.BigEndian.PutUint32(buf[0:4], totalLen)

	// Checksum over OpCode + Payload
	checksum := crc32.ChecksumIEEE(append([]byte{op}, payload...))
	binary.BigEndian.PutUint32(buf[4:8], checksum)
	buf[8] = op
	copy(buf[9:], payload)

	n, err := w.activeFile.Write(buf)
	if err != nil {
		return err
	}
	w.currentSize += int64(n)

	if w.policy == FsyncAlways {
		_ = w.activeFile.Sync()
	}

	// Segment rotation if exceeds max segment size
	if w.currentSize >= w.maxSegSize {
		w.rotateSegment()
	}

	return nil
}

func (w *WAL) rotateSegment() {
	_ = w.activeFile.Sync()
	_ = w.activeFile.Close()

	w.segmentID++
	segPath := filepath.Join(w.dir, fmt.Sprintf("vortexmq_%06d.wal", w.segmentID))
	f, err := os.OpenFile(segPath, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0644)
	if err == nil {
		w.activeFile = f
		w.currentSize = 0
	}
}

// Sync flushes kernel buffers to persistent disk storage.
func (w *WAL) Sync() {
	w.mu.Lock()
	defer w.mu.Unlock()
	if w.activeFile != nil && !w.closed {
		_ = w.activeFile.Sync()
	}
}

// Replay reads all WAL files in sequence and restores messages into the broker.
func Replay(dir string, onPublish func(msg *queue.Message)) (int, error) {
	files, err := filepath.Glob(filepath.Join(dir, "vortexmq_*.wal"))
	if err != nil || len(files) == 0 {
		return 0, nil
	}

	count := 0
	header := make([]byte, 8)

	for _, file := range files {
		f, err := os.Open(file)
		if err != nil {
			continue
		}

		for {
			_, err := io.ReadFull(f, header)
			if err != nil {
				break
			}

			totalLen := binary.BigEndian.Uint32(header[0:4])
			expectedCRC := binary.BigEndian.Uint32(header[4:8])

			if totalLen < 9 {
				break // Invalid record
			}

			payloadLen := totalLen - 8
			body := make([]byte, payloadLen)
			if _, err := io.ReadFull(f, body); err != nil {
				break
			}

			actualCRC := crc32.ChecksumIEEE(body)
			if actualCRC != expectedCRC {
				// Corrupted record encountered; stop replay at boundary
				break
			}

			op := body[0]
			recordData := body[1:]

			if op == OpPublish {
				var msg queue.Message
				if err := json.Unmarshal(recordData, &msg); err == nil {
					if onPublish != nil {
						onPublish(&msg)
						count++
					}
				}
			}
		}
		_ = f.Close()
	}

	return count, nil
}

// Close flushes and terminates the WAL engine.
func (w *WAL) Close() error {
	w.mu.Lock()
	defer w.mu.Unlock()

	if w.closed {
		return nil
	}
	w.closed = true

	if w.syncTicker != nil {
		w.syncTicker.Stop()
		close(w.quitSync)
	}

	if w.activeFile != nil {
		_ = w.activeFile.Sync()
		return w.activeFile.Close()
	}
	return nil
}
