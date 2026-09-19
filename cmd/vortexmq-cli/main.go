package main

import (
	"bufio"
	"flag"
	"fmt"
	"net"
	"os"
	"strconv"
	"strings"

	"github.com/GargAnshu9468/vortexmq/internal/protocol"
)

func main() {
	host := flag.String("h", "127.0.0.1", "VortexMQ broker host")
	port := flag.Int("p", 8379, "VortexMQ broker port")
	password := flag.String("a", "", "VortexMQ auth password")
	flag.Parse()

	addr := net.JoinHostPort(*host, strconv.Itoa(*port))
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		fmt.Printf("Error: Could not connect to VortexMQ on %s: %v\n", addr, err)
		os.Exit(1)
	}
	defer conn.Close()

	reader := protocol.NewReader(conn)
	writer := protocol.NewWriter(conn)

	// Authenticate if password provided
	if *password != "" {
		_ = writer.WriteArray([][]byte{[]byte("AUTH"), []byte(*password)})
		cmd, _ := reader.ReadCommand()
		_ = cmd
	}

	args := flag.Args()
	if len(args) > 0 {
		// Run single command
		cmdBytes := make([][]byte, len(args))
		for i, a := range args {
			cmdBytes[i] = []byte(a)
		}
		_ = writer.WriteArray(cmdBytes)

		// Read and display response
		lineReader := bufio.NewReader(conn)
		respLine, _ := lineReader.ReadString('\n')
		fmt.Print(respLine)
		return
	}

	// Interactive REPL
	fmt.Printf("Connected to VortexMQ at %s\n", addr)
	fmt.Println("Type commands (e.g. PING, LPUSH <topic> <msg>, RPOP <topic>, VMQ.STATS, QUIT):")

	scanner := bufio.NewScanner(os.Stdin)
	for {
		fmt.Printf("vortexmq:%d> ", *port)
		if !scanner.Scan() {
			break
		}

		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		if strings.EqualFold(line, "exit") || strings.EqualFold(line, "quit") {
			break
		}

		parts := strings.Fields(line)
		cmdBytes := make([][]byte, len(parts))
		for i, p := range parts {
			cmdBytes[i] = []byte(p)
		}

		if err := writer.WriteArray(cmdBytes); err != nil {
			fmt.Printf("Error sending command: %v\n", err)
			break
		}

		// Read simple line response
		lineReader := bufio.NewReader(conn)
		respLine, err := lineReader.ReadString('\n')
		if err != nil {
			fmt.Printf("Connection closed by server: %v\n", err)
			break
		}

		if strings.HasPrefix(respLine, "$") {
			// Bulk string: read payload
			var length int
			_, _ = fmt.Sscanf(respLine, "$%d", &length)
			if length >= 0 {
				payload := make([]byte, length+2)
				_, _ = lineReader.Read(payload)
				fmt.Print(string(payload[:length]) + "\n")
			} else {
				fmt.Println("(nil)")
			}
		} else {
			fmt.Print(respLine)
		}
	}
}
