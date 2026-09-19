package network

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/GargAnshu9468/vortexmq/internal/protocol"
	"github.com/GargAnshu9468/vortexmq/internal/queue"
)

// ServerConfig holds configuration parameters for the VortexMQ TCP server.
type ServerConfig struct {
	Addr       string
	Password   string
	MaxClients int
}

// Server coordinates incoming TCP client connections and RESP command dispatch.
type Server struct {
	cfg      ServerConfig
	broker   *queue.Broker
	listener net.Listener
	mu       sync.Mutex
	conns    map[net.Conn]struct{}
	closed   bool
	wg       sync.WaitGroup
}

// NewServer constructs a new TCP broker server.
func NewServer(cfg ServerConfig, broker *queue.Broker) *Server {
	if cfg.Addr == "" {
		cfg.Addr = ":8379"
	}
	return &Server{
		cfg:    cfg,
		broker: broker,
		conns:  make(map[net.Conn]struct{}),
	}
}

// Start launches the TCP listener and client connection handler loop.
func (s *Server) Start() error {
	l, err := net.Listen("tcp", s.cfg.Addr)
	if err != nil {
		return fmt.Errorf("failed to bind tcp listener on %s: %w", s.cfg.Addr, err)
	}
	s.listener = l

	log.Printf("[VortexMQ] ⚡ TCP Broker listening on %s (RESP2/Stream Protocol)", s.cfg.Addr)

	go s.acceptLoop()
	return nil
}

func (s *Server) acceptLoop() {
	for {
		conn, err := s.listener.Accept()
		if err != nil {
			s.mu.Lock()
			isClosed := s.closed
			s.mu.Unlock()
			if isClosed {
				return
			}
			continue
		}

		s.mu.Lock()
		if s.cfg.MaxClients > 0 && len(s.conns) >= s.cfg.MaxClients {
			s.mu.Unlock()
			_ = conn.Close()
			continue
		}
		s.conns[conn] = struct{}{}
		s.mu.Unlock()

		s.wg.Add(1)
		go s.handleConnection(conn)
	}
}

func (s *Server) handleConnection(conn net.Conn) {
	defer func() {
		_ = conn.Close()
		s.mu.Lock()
		delete(s.conns, conn)
		s.mu.Unlock()
		s.wg.Done()
	}()

	// TCP KeepAlive & NoDelay for ultra-low latency
	if tc, ok := conn.(*net.TCPConn); ok {
		_ = tc.SetNoDelay(true)
		_ = tc.SetKeepAlive(true)
		_ = tc.SetKeepAlivePeriod(30 * time.Second)
	}

	reader := protocol.NewReader(conn)
	writer := protocol.NewWriter(conn)
	authenticated := s.cfg.Password == ""

	for {
		cmd, err := reader.ReadCommand()
		if err != nil {
			return
		}

		if !authenticated && cmd.Name != "AUTH" && cmd.Name != "QUIT" {
			_ = writer.WriteError("NOAUTH Authentication required.")
			continue
		}

		switch cmd.Name {
		case "PING":
			if len(cmd.Args) > 0 {
				_ = writer.WriteBulkString(cmd.Args[0])
			} else {
				_ = writer.WriteRaw(protocol.PONG)
			}

		case "ECHO":
			if len(cmd.Args) > 0 {
				_ = writer.WriteBulkString(cmd.Args[0])
			} else {
				_ = writer.WriteError("wrong number of arguments for 'echo'")
			}

		case "AUTH":
			if len(cmd.Args) != 1 {
				_ = writer.WriteError("wrong number of arguments for 'auth'")
				continue
			}
			if s.cfg.Password != "" && string(cmd.Args[0]) == s.cfg.Password {
				authenticated = true
				_ = writer.WriteRaw(protocol.OK)
			} else if s.cfg.Password == "" {
				authenticated = true
				_ = writer.WriteRaw(protocol.OK)
			} else {
				_ = writer.WriteError("ERR invalid password")
			}

		case "COMMAND":
			_ = writer.WriteRaw(protocol.EmptyArray)

		case "INFO":
			stats := s.broker.Stats()
			infoStr := fmt.Sprintf(
				"# VortexMQ Telemetry\r\n"+
					"version:1.0.0\r\n"+
					"uptime_seconds:%d\r\n"+
					"total_topics:%d\r\n"+
					"total_published:%d\r\n"+
					"total_consumed:%d\r\n"+
					"total_acked:%d\r\n"+
					"total_nacked:%d\r\n"+
					"total_dlq:%d\r\n",
				stats.UptimeSeconds, stats.TotalTopics,
				stats.TotalPublished, stats.TotalConsumed,
				stats.TotalAcked, stats.TotalNacked, stats.TotalDLQ,
			)
			_ = writer.WriteBulkString([]byte(infoStr))

		// List Queue Commands: LPUSH, RPUSH, LPOP, RPOP, BRPOP, LLEN
		case "LPUSH":
			if len(cmd.Args) < 2 {
				_ = writer.WriteError("wrong number of arguments for 'lpush'")
				continue
			}
			topicName := string(cmd.Args[0])
			count := 0
			for _, val := range cmd.Args[1:] {
				if _, err := s.broker.PublishFront(topicName, val); err == nil {
					count++
				}
			}
			t, _ := s.broker.GetTopic(topicName)
			_ = writer.WriteInteger(int64(t.Stats().Length))

		case "RPUSH":
			if len(cmd.Args) < 2 {
				_ = writer.WriteError("wrong number of arguments for 'rpush'")
				continue
			}
			topicName := string(cmd.Args[0])
			count := 0
			for _, val := range cmd.Args[1:] {
				if _, err := s.broker.Publish(topicName, val); err == nil {
					count++
				}
			}
			t, _ := s.broker.GetTopic(topicName)
			_ = writer.WriteInteger(int64(t.Stats().Length))

		case "LPOP":
			if len(cmd.Args) < 1 {
				_ = writer.WriteError("wrong number of arguments for 'lpop'")
				continue
			}
			topicName := string(cmd.Args[0])
			msg, ok := s.broker.Consume(topicName, 0)
			if !ok || msg == nil {
				_ = writer.WriteBulkString(nil)
			} else {
				_ = writer.WriteBulkString(msg.Payload)
			}

		case "RPOP":
			if len(cmd.Args) < 1 {
				_ = writer.WriteError("wrong number of arguments for 'rpop'")
				continue
			}
			topicName := string(cmd.Args[0])
			msg, ok := s.broker.Consume(topicName, 0)
			if !ok || msg == nil {
				_ = writer.WriteBulkString(nil)
			} else {
				_ = writer.WriteBulkString(msg.Payload)
			}

		case "BRPOP", "BLPOP":
			if len(cmd.Args) < 2 {
				_ = writer.WriteError("wrong number of arguments for blocking pop")
				continue
			}
			topicName := string(cmd.Args[0])
			timeoutSec, _ := strconv.Atoi(string(cmd.Args[len(cmd.Args)-1]))
			timeout := time.Duration(timeoutSec) * time.Second

			msg, ok := s.broker.Consume(topicName, timeout)
			if !ok || msg == nil {
				_ = writer.WriteRaw(protocol.NullBulk)
			} else {
				// RESP2 array of [key, value]
				elements := [][]byte{[]byte(topicName), msg.Payload}
				_ = writer.WriteArray(elements)
			}

		case "LLEN":
			if len(cmd.Args) != 1 {
				_ = writer.WriteError("wrong number of arguments for 'llen'")
				continue
			}
			topicName := string(cmd.Args[0])
			if t, ok := s.broker.GetTopic(topicName); ok {
				_ = writer.WriteInteger(int64(t.Stats().Length))
			} else {
				_ = writer.WriteInteger(0)
			}

		// Stream / Consumer Group Commands
		case "XADD":
			if len(cmd.Args) < 3 {
				_ = writer.WriteError("wrong number of arguments for 'xadd'")
				continue
			}
			topicName := string(cmd.Args[0])
			// Parse field values as JSON map payload
			payloadMap := make(map[string]string)
			args := cmd.Args[2:] // skip topic and stream id (e.g. "*")
			for i := 0; i+1 < len(args); i += 2 {
				payloadMap[string(args[i])] = string(args[i+1])
			}
			payloadBytes, _ := json.Marshal(payloadMap)
			msg, err := s.broker.Publish(topicName, payloadBytes)
			if err != nil {
				_ = writer.WriteError(err.Error())
			} else {
				_ = writer.WriteBulkString([]byte(msg.ID))
			}

		case "XGROUP":
			// Subcommand: XGROUP CREATE <key> <group> <id> [MKSTREAM]
			if len(cmd.Args) >= 4 && strings.ToUpper(string(cmd.Args[0])) == "CREATE" {
				topicName := string(cmd.Args[1])
				groupName := string(cmd.Args[2])
				t := s.broker.GetOrCreateTopic(topicName)
				t.GetOrCreateGroup(groupName, 30*time.Second)
				_ = writer.WriteRaw(protocol.OK)
			} else {
				_ = writer.WriteError("unsupported xgroup subcommand")
			}

		case "XREADGROUP":
			// Format: XREADGROUP GROUP <group> <consumer> [COUNT <c>] [BLOCK <ms>] STREAMS <key> <id>
			var groupName, consumerID, topicName string
			var blockMs int

			for i := 0; i < len(cmd.Args); i++ {
				arg := strings.ToUpper(string(cmd.Args[i]))
				if arg == "GROUP" && i+2 < len(cmd.Args) {
					groupName = string(cmd.Args[i+1])
					consumerID = string(cmd.Args[i+2])
					i += 2
				} else if arg == "BLOCK" && i+1 < len(cmd.Args) {
					blockMs, _ = strconv.Atoi(string(cmd.Args[i+1]))
					i++
				} else if arg == "STREAMS" && i+1 < len(cmd.Args) {
					topicName = string(cmd.Args[i+1])
					break
				}
			}

			if topicName == "" || groupName == "" {
				_ = writer.WriteError("invalid xreadgroup arguments")
				continue
			}

			timeout := time.Duration(blockMs) * time.Millisecond
			msg, ok := s.broker.ConsumeGroup(topicName, groupName, consumerID, timeout)
			if !ok || msg == nil {
				_ = writer.WriteRaw(protocol.NullBulk)
			} else {
				elements := [][]byte{
					[]byte(msg.ID),
					msg.Payload,
				}
				_ = writer.WriteArray(elements)
			}

		case "XACK":
			if len(cmd.Args) < 3 {
				_ = writer.WriteError("wrong number of arguments for 'xack'")
				continue
			}
			topicName := string(cmd.Args[0])
			groupName := string(cmd.Args[1])
			ackedCount := int64(0)
			for _, idBytes := range cmd.Args[2:] {
				if s.broker.Ack(topicName, groupName, string(idBytes)) {
					ackedCount++
				}
			}
			_ = writer.WriteInteger(ackedCount)

		// Native High-Speed VortexMQ Commands
		case "VMQ.PUBLISH":
			if len(cmd.Args) < 2 {
				_ = writer.WriteError("wrong number of arguments for 'vmq.publish'")
				continue
			}
			topicName := string(cmd.Args[0])
			payload := cmd.Args[1]
			var delay time.Duration
			if len(cmd.Args) >= 3 {
				if ms, err := strconv.Atoi(string(cmd.Args[2])); err == nil && ms > 0 {
					delay = time.Duration(ms) * time.Millisecond
				}
			}

			var msg *queue.Message
			if delay > 0 {
				msg, err = s.broker.PublishDelayed(topicName, payload, delay)
			} else {
				msg, err = s.broker.Publish(topicName, payload)
			}

			if err != nil {
				_ = writer.WriteError(err.Error())
			} else {
				_ = writer.WriteBulkString([]byte(msg.ID))
			}

		case "VMQ.CONSUME":
			if len(cmd.Args) < 1 {
				_ = writer.WriteError("wrong number of arguments for 'vmq.consume'")
				continue
			}
			topicName := string(cmd.Args[0])
			timeout := 0 * time.Millisecond
			if len(cmd.Args) >= 2 {
				if ms, err := strconv.Atoi(string(cmd.Args[1])); err == nil {
					timeout = time.Duration(ms) * time.Millisecond
				}
			}

			msg, ok := s.broker.Consume(topicName, timeout)
			if !ok || msg == nil {
				_ = writer.WriteRaw(protocol.NullBulk)
			} else {
				_ = writer.WriteBulkString(msg.Payload)
			}

		case "VMQ.ACK":
			if len(cmd.Args) != 3 {
				_ = writer.WriteError("usage: VMQ.ACK <topic> <group> <msg_id>")
				continue
			}
			if s.broker.Ack(string(cmd.Args[0]), string(cmd.Args[1]), string(cmd.Args[2])) {
				_ = writer.WriteRaw(protocol.OK)
			} else {
				_ = writer.WriteError("ACK rejected: message not found in pending list")
			}

		case "VMQ.NACK":
			if len(cmd.Args) < 3 {
				_ = writer.WriteError("usage: VMQ.NACK <topic> <group> <msg_id> [reason]")
				continue
			}
			reason := "processing failed"
			if len(cmd.Args) >= 4 {
				reason = string(cmd.Args[3])
			}
			if s.broker.Nack(string(cmd.Args[0]), string(cmd.Args[1]), string(cmd.Args[2]), reason) {
				_ = writer.WriteRaw(protocol.OK)
			} else {
				_ = writer.WriteError("NACK rejected: message not found in pending list")
			}

		case "VMQ.REPLAY":
			if len(cmd.Args) != 2 {
				_ = writer.WriteError("usage: VMQ.REPLAY <topic> <msg_id>")
				continue
			}
			if _, ok := s.broker.ReplayDLQ(string(cmd.Args[0]), string(cmd.Args[1])); ok {
				_ = writer.WriteRaw(protocol.OK)
			} else {
				_ = writer.WriteError("message not found in dead letter queue")
			}

		case "VMQ.STATS":
			statsBytes, _ := json.Marshal(s.broker.Stats())
			_ = writer.WriteBulkString(statsBytes)

		case "QUIT":
			_ = writer.WriteRaw(protocol.OK)
			return

		default:
			_ = writer.WriteError(fmt.Sprintf("unknown command '%s'", cmd.Name))
		}
	}
}

// Stop closes the listener and gracefully disconnects active client sessions.
func (s *Server) Stop() error {
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil
	}
	s.closed = true
	if s.listener != nil {
		_ = s.listener.Close()
	}
	for conn := range s.conns {
		_ = conn.Close()
	}
	s.mu.Unlock()

	s.wg.Wait()
	return nil
}
