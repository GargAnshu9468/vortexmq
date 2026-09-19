package api

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/GargAnshu9468/vortexmq/internal/queue"
	"github.com/GargAnshu9468/vortexmq/web"
)

// ServerConfig configures the HTTP API & Web Studio server.
type ServerConfig struct {
	Addr string
}

// Server hosts the REST endpoints and embedded Web Studio.
type Server struct {
	cfg        ServerConfig
	broker     *queue.Broker
	httpServer *http.Server
}

// NewServer creates a new API and Web Studio server.
func NewServer(cfg ServerConfig, broker *queue.Broker) *Server {
	if cfg.Addr == "" {
		cfg.Addr = ":8380"
	}
	return &Server{
		cfg:    cfg,
		broker: broker,
	}
}

// Start boots the HTTP server in a background goroutine.
func (s *Server) Start() error {
	mux := http.NewServeMux()

	// REST API Routes
	mux.HandleFunc("/api/v1/stats", s.handleStats)
	mux.HandleFunc("/api/v1/topics", s.handleTopics)
	mux.HandleFunc("/api/v1/topics/", s.handleTopicAction)
	mux.HandleFunc("/api/v1/dlq", s.handleDLQ)
	mux.HandleFunc("/api/v1/dlq/replay", s.handleDLQReplay)
	mux.HandleFunc("/api/v1/dlq/purge", s.handleDLQPurge)

	// Embedded Web Studio UI
	sub, err := fs.Sub(web.StudioFS, "studio")
	if err == nil {
		mux.Handle("/", http.FileServer(http.FS(sub)))
	} else {
		mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
			_, _ = w.Write([]byte("VortexMQ API & Studio Active on port 8380"))
		})
	}

	s.httpServer = &http.Server{
		Addr:         s.cfg.Addr,
		Handler:      s.corsMiddleware(mux),
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 15 * time.Second,
	}

	log.Printf("[VortexMQ] 🌌 Immersive Web Studio running on http://0.0.0.0%s", s.cfg.Addr)

	go func() {
		if err := s.httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[VortexMQ] HTTP Studio server error: %v", err)
		}
	}()

	return nil
}

func (s *Server) corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, POST, PUT, DELETE, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type, Authorization")
		if r.Method == http.MethodOptions {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func (s *Server) handleStats(w http.ResponseWriter, r *http.Request) {
	stats := s.broker.Stats()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(stats)
}

func (s *Server) handleTopics(w http.ResponseWriter, r *http.Request) {
	topics := s.broker.TopicsList()
	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"topics": topics,
		"count":  len(topics),
	})
}

func (s *Server) handleTopicAction(w http.ResponseWriter, r *http.Request) {
	// Path: /api/v1/topics/{topic}/{action}
	parts := strings.Split(strings.TrimPrefix(r.URL.Path, "/api/v1/topics/"), "/")
	if len(parts) == 0 || parts[0] == "" {
		http.Error(w, `{"error":"missing topic name"}`, http.StatusBadRequest)
		return
	}

	topicName := parts[0]
	action := ""
	if len(parts) >= 2 {
		action = parts[1]
	}

	switch {
	case action == "publish" && r.Method == http.MethodPost:
		body, err := io.ReadAll(r.Body)
		if err != nil {
			http.Error(w, `{"error":"failed to read request body"}`, http.StatusBadRequest)
			return
		}

		var req struct {
			Payload any `json:"payload"`
			DelayMs int `json:"delay_ms"`
		}

		var payloadBytes []byte
		if err := json.Unmarshal(body, &req); err == nil && req.Payload != nil {
			switch p := req.Payload.(type) {
			case string:
				payloadBytes = []byte(p)
			default:
				payloadBytes, _ = json.Marshal(p)
			}
		} else {
			payloadBytes = body
		}

		var msg *queue.Message
		if req.DelayMs > 0 {
			msg, err = s.broker.PublishDelayed(topicName, payloadBytes, time.Duration(req.DelayMs)*time.Millisecond)
		} else {
			msg, err = s.broker.Publish(topicName, payloadBytes)
		}

		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%s"}`, err.Error()), http.StatusInternalServerError)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(map[string]any{
			"status": "published",
			"id":     msg.ID,
			"topic":  topicName,
		})

	case action == "consume" && r.Method == http.MethodGet:
		timeoutMs, _ := strconv.Atoi(r.URL.Query().Get("timeout_ms"))
		msg, ok := s.broker.Consume(topicName, time.Duration(timeoutMs)*time.Millisecond)
		if !ok || msg == nil {
			w.WriteHeader(http.StatusNoContent)
			return
		}

		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(msg)

	default:
		http.Error(w, `{"error":"unsupported topic action"}`, http.StatusNotFound)
	}
}

func (s *Server) handleDLQ(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("topic")
	var entries []*queue.DeadLetterEntry

	if topicName != "" {
		if t, ok := s.broker.GetTopic(topicName); ok {
			entries = t.DLQ().List(0, 100)
		}
	} else {
		// Collect from all topics
		for _, name := range s.broker.TopicsList() {
			if t, ok := s.broker.GetTopic(name); ok {
				entries = append(entries, t.DLQ().List(0, 50)...)
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(entries)
}

func (s *Server) handleDLQReplay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		Topic string `json:"topic"`
		ID    string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Topic == "" || req.ID == "" {
		http.Error(w, `{"error":"topic and id required"}`, http.StatusBadRequest)
		return
	}

	msg, ok := s.broker.ReplayDLQ(req.Topic, req.ID)
	if !ok || msg == nil {
		http.Error(w, `{"error":"failed to replay message"}`, http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "replayed",
		"id":     msg.ID,
		"topic":  req.Topic,
	})
}

func (s *Server) handleDLQPurge(w http.ResponseWriter, r *http.Request) {
	topicName := r.URL.Query().Get("topic")
	count := 0

	if topicName != "" {
		if t, ok := s.broker.GetTopic(topicName); ok {
			count = t.DLQ().Purge()
		}
	} else {
		for _, name := range s.broker.TopicsList() {
			if t, ok := s.broker.GetTopic(name); ok {
				count += t.DLQ().Purge()
			}
		}
	}

	w.Header().Set("Content-Type", "application/json")
	_ = json.NewEncoder(w).Encode(map[string]any{
		"status": "purged",
		"count":  count,
	})
}

// Stop gracefully shuts down the HTTP server.
func (s *Server) Stop(ctx context.Context) error {
	if s.httpServer != nil {
		return s.httpServer.Shutdown(ctx)
	}
	return nil
}
