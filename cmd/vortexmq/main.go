package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/GargAnshu9468/vortexmq/internal/api"
	"github.com/GargAnshu9468/vortexmq/internal/network"
	"github.com/GargAnshu9468/vortexmq/internal/queue"
	"github.com/GargAnshu9468/vortexmq/internal/wal"
)

const (
	ColorCyan   = "\033[38;2;0;243;255m"
	ColorPurple = "\033[38;2;138;43;226m"
	ColorGreen  = "\033[38;2;0;255;136m"
	ColorReset  = "\033[0m"
)

func printBanner() {
	banner := ColorCyan + `
  ██╗   ██╗ ██████╗ ██████╗ ████████╗███████╗██╗  ██╗    ███╗   ███╗ ██████╗ 
  ██║   ██║██╔═══██╗██╔══██╗╚══██╔══╝██╔════╝╚██╗██╔╝    ████╗ ████║██╔═══██╗
  ██║   ██║██║   ██║██████╔╝   ██║   █████╗   ╚███╔╝     ██╔████╔██║██║   ██║
  ╚██╗ ██╔╝██║   ██║██╔══██╗   ██║   ██╔══╝   ██╔██╗     ██║╚██╔╝██║██║▄▄ ██║
   ╚████╔╝ ╚██████╔╝██║  ██║   ██║   ███████╗██╔╝ ██╗    ██║ ╚═╝ ██║╚██████╔╝
    ╚═══╝   ╚═════╝ ╚═╝  ╚═╝   ╚═╝   ╚══════╝╚═╝  ╚═╝    ╚═╝     ╚═╝ ╚══▀▀═╝ 
` + ColorReset +
		ColorPurple + "  » Ultra-Fast Zero-Erlang, Zero-JVM Message Broker & Task Engine «\n" + ColorReset +
		ColorGreen + "  Version: 1.0.0-PROD  |  Protocol: RESP2/Stream  |  Engine: Lock-Free Rings & WAL\n" + ColorReset +
		"----------------------------------------------------------------------"
	fmt.Println(banner)
}

func main() {
	brokerPort := flag.Int("broker-port", 8379, "Core TCP/RESP broker port")
	studioPort := flag.Int("studio-port", 8380, "Web Studio & REST API port")
	dataDir := flag.String("dir", "./data", "WAL persistence directory")
	enableWAL := flag.Bool("wal", true, "Enable write-ahead logging persistence")
	password := flag.String("password", "", "Authentication password (optional)")
	fsyncPolicyStr := flag.String("fsync", "everysec", "Fsync policy: always, everysec, none")
	flag.Parse()

	printBanner()

	// 1. Initialize WAL Persistence if enabled
	var walEngine *wal.WAL
	if *enableWAL {
		policy := wal.FsyncEverySec
		switch *fsyncPolicyStr {
		case "always":
			policy = wal.FsyncAlways
		case "none":
			policy = wal.FsyncNone
		}

		w, err := wal.OpenWAL(*dataDir, policy)
		if err != nil {
			log.Fatalf("[VortexMQ] Fatal: failed to initialize WAL: %v", err)
		}
		walEngine = w
		log.Printf("[VortexMQ] 💾 Write-Ahead Log active in %s (Fsync: %s)", *dataDir, *fsyncPolicyStr)
	} else {
		log.Println("[VortexMQ] 🚀 Running in Pure In-Memory Ultra-Speed Mode (WAL Disabled)")
	}

	// 2. Initialize Central Broker
	broker := queue.NewBroker(walEngine)

	// 3. Replay WAL if existing logs found
	if *enableWAL {
		recovered, err := wal.Replay(*dataDir, func(msg *queue.Message) {
			t := broker.GetOrCreateTopic(msg.Topic)
			t.Publish(msg)
		})
		if err != nil {
			log.Printf("[VortexMQ] ⚠️ Warning during WAL replay: %v", err)
		} else if recovered > 0 {
			log.Printf("[VortexMQ] 🔄 Successfully recovered %d messages from WAL into memory", recovered)
		}
	}

	// 4. Start TCP Broker Server
	tcpCfg := network.ServerConfig{
		Addr:     fmt.Sprintf(":%d", *brokerPort),
		Password: *password,
	}
	tcpServer := network.NewServer(tcpCfg, broker)
	if err := tcpServer.Start(); err != nil {
		log.Fatalf("[VortexMQ] Fatal: failed to start TCP Broker: %v", err)
	}

	// 5. Start HTTP API & Embedded Web Studio Server
	apiCfg := api.ServerConfig{
		Addr: fmt.Sprintf(":%d", *studioPort),
	}
	apiServer := api.NewServer(apiCfg, broker)
	if err := apiServer.Start(); err != nil {
		log.Fatalf("[VortexMQ] Fatal: failed to start Web Studio: %v", err)
	}

	log.Printf("[VortexMQ] ⚡ Redis/RESP Client Port: 0.0.0.0:%d (redis-cli -p %d)", *brokerPort, *brokerPort)
	log.Printf("[VortexMQ] 🌌 Immersive Visual Studio: http://0.0.0.0:%d", *studioPort)

	// 6. Graceful Shutdown Handler
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
	<-sigCh

	log.Println("\n[VortexMQ] Shutting down gracefully...")

	// Stop servers
	_ = tcpServer.Stop()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = apiServer.Stop(ctx)

	// Flush and close broker
	_ = broker.Close()

	log.Println("[VortexMQ] Server terminated cleanly. Goodbye!")
}
