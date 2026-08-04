package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	port         = 8080
	bufferSize   = 4096
	writeBufSize = 65536 // backpressure threshold
)

// --- JSON telemetry events (same format as epoll_visual_server.c) ---

type TelemetryEvent struct {
	Type         string `json:"type"`
	FD           int    `json:"fd,omitempty"`
	Message      string `json:"message,omitempty"`
	Port         int    `json:"port,omitempty"`
	ListenFD     int    `json:"listen_fd,omitempty"`
	EpollFD      int    `json:"epoll_fd,omitempty"`
	Nfds         int    `json:"nfds,omitempty"`
	BytesRead    int    `json:"bytes_read,omitempty"`
	BytesWritten int    `json:"bytes_written,omitempty"`
	WriteLen     int    `json:"write_len,omitempty"`
	WritePos     int    `json:"write_pos,omitempty"`
	Capacity     int    `json:"capacity,omitempty"`
	Active       *bool  `json:"active,omitempty"`
}

func emit(event TelemetryEvent) {
	data, _ := json.Marshal(event)
	fmt.Println(string(data))
	os.Stdout.Sync()
}

// --- Client state ---

type Client struct {
	Conn         net.Conn
	WriteBuf     []byte
	WriteLen     int
	Backpressure bool
	mu           sync.Mutex
}

func newClient(conn net.Conn) *Client {
	return &Client{
		Conn:     conn,
		WriteBuf: make([]byte, 0, writeBufSize),
	}
}

// --- Server ---

type Server struct {
	clients   map[int]*Client
	clientsMu sync.Mutex
	nextFD    int
	listenFD  int
}

func NewServer() *Server {
	return &Server{
		clients: make(map[int]*Client),
	}
}

func (s *Server) nextClientFD() int {
	s.nextFD++
	return s.nextFD
}

func (s *Server) addClient(fd int, c *Client) {
	s.clientsMu.Lock()
	s.clients[fd] = c
	s.clientsMu.Unlock()
}

func (s *Server) removeClient(fd int) {
	s.clientsMu.Lock()
	delete(s.clients, fd)
	s.clientsMu.Unlock()
}

func (s *Server) handleClient(conn net.Conn) {
	fd := s.nextClientFD()
	client := newClient(conn)
	s.addClient(fd, client)

	emit(TelemetryEvent{
		Type: "accept",
		FD:   fd,
		Port: port,
	})
	log.Printf("[Server] New client connected on fd %d", fd)

	reader := bufio.NewReader(conn)

	for {
		// Read available data (non-blocking via bufio)
		data, err := reader.Peek(reader.Buffered())
		if err != nil {
			s.handleReadError(fd, client, err)
			return
		}

		if len(data) == 0 {
			// No data available, try blocking read with timeout
			conn.SetReadDeadline(time.Now().Add(100 * time.Millisecond))
			n, err := reader.Read(make([]byte, 1))
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue // no data yet, loop again
				}
				s.handleReadError(fd, client, err)
				return
			}
			_ = n
			data, _ = reader.Peek(reader.Buffered())
		}

		if len(data) > 0 {
			s.handleRead(fd, client, data)
		}
	}
}

func (s *Server) handleRead(fd int, client *Client, data []byte) {
	client.mu.Lock()
	defer client.mu.Unlock()

	n := len(data)
	emit(TelemetryEvent{
		Type:      "read",
		FD:        fd,
		BytesRead: n,
		WriteLen:  client.WriteLen,
		WritePos:  0,
		Capacity:  writeBufSize,
	})

	// Echo back immediately
	client.WriteBuf = append(client.WriteBuf, data...)
	client.WriteLen += n

	s.flushWrite(fd, client)
}

func (s *Server) handleReadError(fd int, client *Client, err error) {
	if err == io.EOF {
		log.Printf("[Server] Client fd %d disconnected", fd)
		emit(TelemetryEvent{
			Type: "close",
			FD:   fd,
		})
	} else {
		log.Printf("[Server] Client fd %d error: %v", fd, err)
		emit(TelemetryEvent{
			Type:    "close",
			FD:      fd,
			Message: err.Error(),
		})
	}

	client.Conn.Close()
	s.removeClient(fd)
}

func (s *Server) flushWrite(fd int, client *Client) {
	if client.WriteLen == 0 {
		return
	}

	// Write all buffered data
	n, err := client.Conn.Write(client.WriteBuf[:client.WriteLen])
	if err != nil {
		log.Printf("[Server] Write error on fd %d: %v", fd, err)
		client.WriteBuf = client.WriteBuf[:0]
		client.WriteLen = 0
		client.Backpressure = false
		return
	}

	emit(TelemetryEvent{
		Type:         "write",
		FD:           fd,
		BytesWritten: n,
		WriteLen:     client.WriteLen,
		WritePos:     n,
		Capacity:     writeBufSize,
	})

	client.WriteBuf = client.WriteBuf[n:]
	client.WriteLen -= n

	if client.WriteLen == 0 {
		client.Backpressure = false
		emit(TelemetryEvent{
			Type:     "buffer_cleared",
			FD:       fd,
			Capacity: writeBufSize,
		})
	} else if client.WriteLen >= writeBufSize/2 {
		// Trigger backpressure signal
		active := true
		emit(TelemetryEvent{
			Type:     "backpressure",
			FD:       fd,
			Active:   &active,
			WriteLen: client.WriteLen,
			Capacity: writeBufSize,
		})
		client.Backpressure = true
	}
}

func main() {
	// Enable line-buffered stdout for JSON telemetry
	scanner := bufio.NewScanner(os.Stdin)
	_ = scanner

	ln, err := net.Listen("tcp", fmt.Sprintf(":%d", port))
	if err != nil {
		emit(TelemetryEvent{
			Type:    "error",
			Message: fmt.Sprintf("Failed to bind port %d: %v", port, err),
		})
		log.Fatalf("Failed to listen on port %d: %v", port, err)
	}
	defer ln.Close()

	// Use a fake FD number for the listener (matches C version's style)
	server := NewServer()
	server.listenFD = 1 // arbitrary FD for listener

	emit(TelemetryEvent{
		Type:     "init",
		Port:     port,
		ListenFD: server.listenFD,
		EpollFD:  1, // epoll_fd equivalent
	})

	log.Printf("[Server] Go echo server listening on :%d", port)
	log.Printf("[Server] JSON telemetry events emitted to stdout")
	log.Printf("[Server] Open http://localhost:8000 to view the dashboard")

	for {
		conn, err := ln.Accept()
		if err != nil {
			log.Printf("[Server] Accept error: %v", err)
			continue
		}
		go server.handleClient(conn)
	}
}
