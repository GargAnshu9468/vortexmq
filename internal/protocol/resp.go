package protocol

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"strconv"
)

// RESP data type prefixes
const (
	SimpleString = '+'
	Error        = '-'
	Integer      = ':'
	BulkString   = '$'
	Array        = '*'
)

var (
	CRLF       = []byte("\r\n")
	NullBulk   = []byte("$-1\r\n")
	EmptyArray = []byte("*0\r\n")
	PONG       = []byte("+PONG\r\n")
	OK         = []byte("+OK\r\n")
)

// Command represents a parsed RESP command with arguments.
type Command struct {
	Name string
	Args [][]byte
}

// Reader decodes RESP2 streams with minimal heap allocation.
type Reader struct {
	rd *bufio.Reader
}

// NewReader wraps an io.Reader in a buffered RESP decoder.
func NewReader(r io.Reader) *Reader {
	return &Reader{rd: bufio.NewReaderSize(r, 64*1024)}
}

// ReadCommand parses the next incoming RESP array command from the stream.
func (r *Reader) ReadCommand() (*Command, error) {
	line, err := r.readLine()
	if err != nil {
		return nil, err
	}

	if len(line) == 0 {
		return nil, errors.New("empty command line")
	}

	// Inline command (e.g. "PING\r\n" or "SET a b\r\n")
	if line[0] != Array {
		parts := bytes.Fields(line)
		if len(parts) == 0 {
			return nil, errors.New("empty inline command")
		}
		cmdName := string(bytes.ToUpper(parts[0]))
		return &Command{
			Name: cmdName,
			Args: parts[1:],
		}, nil
	}

	// Array command (*<count>\r\n)
	count, err := strconv.Atoi(string(line[1:]))
	if err != nil || count <= 0 {
		return nil, fmt.Errorf("invalid array length: %w", err)
	}

	args := make([][]byte, 0, count)
	for i := 0; i < count; i++ {
		argLine, err := r.readLine()
		if err != nil {
			return nil, err
		}

		if len(argLine) == 0 || argLine[0] != BulkString {
			return nil, errors.New("expected bulk string in command array")
		}

		bulkLen, err := strconv.Atoi(string(argLine[1:]))
		if err != nil {
			return nil, fmt.Errorf("invalid bulk string length: %w", err)
		}

		if bulkLen < 0 {
			args = append(args, nil)
			continue
		}

		buf := make([]byte, bulkLen+2) // +2 for trailing CRLF
		if _, err := io.ReadFull(r.rd, buf); err != nil {
			return nil, err
		}

		args = append(args, buf[:bulkLen])
	}

	if len(args) == 0 {
		return nil, errors.New("empty command arguments")
	}

	cmdName := string(bytes.ToUpper(args[0]))
	return &Command{
		Name: cmdName,
		Args: args[1:],
	}, nil
}

func (r *Reader) readLine() ([]byte, error) {
	line, isPrefix, err := r.rd.ReadLine()
	if err != nil {
		return nil, err
	}
	if isPrefix {
		// Read remaining chunks if line exceeds initial buffer
		fullLine := append([]byte(nil), line...)
		for isPrefix {
			var chunk []byte
			chunk, isPrefix, err = r.rd.ReadLine()
			if err != nil {
				return nil, err
			}
			fullLine = append(fullLine, chunk...)
		}
		return fullLine, nil
	}
	return line, nil
}

// Writer formats RESP wire responses.
type Writer struct {
	w io.Writer
}

// NewWriter creates a RESP response serializer.
func NewWriter(w io.Writer) *Writer {
	return &Writer{w: w}
}

func (w *Writer) WriteSimpleString(s string) error {
	_, err := fmt.Fprintf(w.w, "+%s\r\n", s)
	return err
}

func (w *Writer) WriteError(msg string) error {
	_, err := fmt.Fprintf(w.w, "-ERR %s\r\n", msg)
	return err
}

func (w *Writer) WriteInteger(n int64) error {
	_, err := fmt.Fprintf(w.w, ":%d\r\n", n)
	return err
}

func (w *Writer) WriteBulkString(b []byte) error {
	if b == nil {
		_, err := w.w.Write(NullBulk)
		return err
	}
	_, err := fmt.Fprintf(w.w, "$%d\r\n%s\r\n", len(b), b)
	return err
}

func (w *Writer) WriteArray(elements [][]byte) error {
	if elements == nil {
		_, err := w.w.Write(EmptyArray)
		return err
	}
	if _, err := fmt.Fprintf(w.w, "*%d\r\n", len(elements)); err != nil {
		return err
	}
	for _, el := range elements {
		if err := w.WriteBulkString(el); err != nil {
			return err
		}
	}
	return nil
}

func (w *Writer) WriteRaw(data []byte) error {
	_, err := w.w.Write(data)
	return err
}
