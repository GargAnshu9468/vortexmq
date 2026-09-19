package protocol

import (
	"bytes"
	"strings"
	"testing"
)

func TestRESPReaderInlineCommand(t *testing.T) {
	input := "PING\r\n"
	r := NewReader(strings.NewReader(input))
	cmd, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cmd.Name != "PING" {
		t.Fatalf("expected PING, got %s", cmd.Name)
	}
}

func TestRESPReaderArrayCommand(t *testing.T) {
	input := "*3\r\n$3\r\nSET\r\n$3\r\nfoo\r\n$3\r\nbar\r\n"
	r := NewReader(strings.NewReader(input))
	cmd, err := r.ReadCommand()
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if cmd.Name != "SET" {
		t.Fatalf("expected SET, got %s", cmd.Name)
	}
	if len(cmd.Args) != 2 {
		t.Fatalf("expected 2 args, got %d", len(cmd.Args))
	}
	if string(cmd.Args[0]) != "foo" || string(cmd.Args[1]) != "bar" {
		t.Fatalf("unexpected args: %v", cmd.Args)
	}
}

func TestRESPReaderExceedsMaxLimits(t *testing.T) {
	// Exceeds max count
	inputHugeArray := "*999999\r\n"
	r := NewReader(strings.NewReader(inputHugeArray))
	_, err := r.ReadCommand()
	if err == nil {
		t.Fatalf("expected error for excessive array count, got nil")
	}

	// Exceeds max bulk string length
	inputHugeBulk := "*1\r\n$999999999\r\n"
	r2 := NewReader(strings.NewReader(inputHugeBulk))
	_, err = r2.ReadCommand()
	if err == nil {
		t.Fatalf("expected error for excessive bulk length, got nil")
	}
}

func TestRESPWriter(t *testing.T) {
	var buf bytes.Buffer
	w := NewWriter(&buf)

	_ = w.WriteSimpleString("PONG")
	_ = w.WriteInteger(42)
	_ = w.WriteBulkString([]byte("hello"))
	_ = w.WriteError("unknown command")

	expected := "+PONG\r\n:42\r\n$5\r\nhello\r\n-ERR unknown command\r\n"
	if buf.String() != expected {
		t.Fatalf("unexpected RESP output: got %q, expected %q", buf.String(), expected)
	}
}
