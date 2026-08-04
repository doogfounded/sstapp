package main

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"
)

const (
	visualizerPort = 8000
)

// Resolve paths relative to this executable's location
func resolveServerBinary() string {
	exe, err := os.Executable()
	if err == nil {
		relPath := exe + "/../server/server.exe"
		if _, err := os.Stat(relPath); err == nil {
			return relPath
		}
	}
	absPath := "D:\\Desktop\\cool\\sstapp\\server\\server.exe"
	if _, err := os.Stat(absPath); err == nil {
		return absPath
	}
	return ""
}

// SSE client connection
type sseClient struct {
	writer http.ResponseWriter
	flush  http.Flusher
	done   chan struct{}
}

var (
	sseClients      = make(map[*sseClient]bool)
	sseClientsMu     sync.RWMutex
	lastInitEvent   string
	lastInitEventMu sync.RWMutex

	managedConns   = make(map[int]net.Conn)
	managedConnsMu sync.Mutex
	connCounter    int
)

func broadcastEvent(data string) {
	sseClientsMu.RLock()
	defer sseClientsMu.RUnlock()

	for c := range sseClients {
		_, err := c.writer.Write([]byte("data: " + data + "\n\n"))
		if err == nil {
			c.flush.Flush()
		}
	}
}

func handleSSE(w http.ResponseWriter, r *http.Request) {
	flusher, ok := w.(http.Flusher)
	if !ok {
		http.Error(w, "Streaming not supported", http.StatusInternalServerError)
		return
	}

	w.Header().Set("Content-Type", "text/event-stream")
	w.Header().Set("Cache-Control", "no-cache")
	w.Header().Set("Connection", "keep-alive")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	client := &sseClient{
		writer: w,
		flush:  flusher,
		done:   make(chan struct{}),
	}

	sseClientsMu.Lock()
	sseClients[client] = true
	sseClientsMu.Unlock()

	log.Printf("[Visualizer] SSE client connected")

	// Send initial connection event
	fmt.Fprintf(w, "data: {\"type\":\"sse_connected\"}\n\n")
	flusher.Flush()

	// Send last init event if available
	lastInitEventMu.RLock()
	initEv := lastInitEvent
	lastInitEventMu.RUnlock()

	if initEv != "" {
		fmt.Fprintf(w, "data: %s\n\n", initEv)
		flusher.Flush()
	}

	// Wait for client disconnect
	<-r.Context().Done()

	sseClientsMu.Lock()
	delete(sseClients, client)
	sseClientsMu.Unlock()

	log.Printf("[Visualizer] SSE client disconnected")
}

func handleAPIStatus(w http.ResponseWriter, r *http.Request) {
	sseClientsMu.RLock()
	count := len(sseClients)
	sseClientsMu.RUnlock()

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(map[string]interface{}{
		"sse_clients": count,
		"status":      "running",
	})
}

func handleSpawnClient(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	conn, err := net.Dial("tcp", "127.0.0.1:8080")
	if err != nil {
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	managedConnsMu.Lock()
	connCounter++
	id := connCounter
	managedConns[id] = conn
	managedConnsMu.Unlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":    "connected",
		"client_id": id,
	})
}

func handleSendData(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	sizeStr := r.URL.Query().Get("size")
	size := 1024
	if sizeStr != "" {
		if s, err := strconv.Atoi(sizeStr); err == nil {
			size = s
		}
	}

	managedConnsMu.Lock()
	var conn net.Conn
	for _, c := range managedConns {
		conn = c
		break
	}
	managedConnsMu.Unlock()

	closeAfter := false
	if conn == nil {
		var err error
		conn, err = net.Dial("tcp", "127.0.0.1:8080")
		if err != nil {
			http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
			return
		}
		closeAfter = true
	}

	payload := make([]byte, size)
	for i := range payload {
		payload[i] = 'A'
	}

	_, err := conn.Write(payload)
	if err != nil {
		if closeAfter {
			conn.Close()
		}
		http.Error(w, fmt.Sprintf(`{"error":"%v"}`, err), http.StatusInternalServerError)
		return
	}

	buf := make([]byte, size)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)

	if closeAfter {
		conn.Close()
	}

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status":         "sent",
		"bytes_sent":     size,
		"bytes_received": n,
	})
}

func handleChokeClient(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")
	chokeStr := r.URL.Query().Get("choke")
	choke := chokeStr == "true"

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "ok",
		"choked": choke,
	})
}

func handleCloseClients(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Access-Control-Allow-Origin", "*")

	managedConnsMu.Lock()
	for id, conn := range managedConns {
		conn.Close()
		delete(managedConns, id)
	}
	managedConnsMu.Unlock()

	json.NewEncoder(w).Encode(map[string]interface{}{
		"status": "closed_all",
	})
}

func handleRoot(w http.ResponseWriter, r *http.Request) {
	indexPath := "D:\\Desktop\\cool\\sstapp\\index.html"
	data, err := os.ReadFile(indexPath)
	if err != nil {
		http.Error(w, "index.html not found: "+err.Error(), http.StatusNotFound)
		return
	}
	w.Header().Set("Content-Type", "text/html")
	w.Write(data)
}

func main() {
	serverBin := resolveServerBinary()
	var cmd *exec.Cmd
	if serverBin != "" {
		log.Printf("[Visualizer] Launching server executable: %s", serverBin)
		cmd = exec.Command(serverBin)
	} else {
		log.Printf("[Visualizer] Launching server via go run...")
		cmd = exec.Command("go", "run", "D:\\Desktop\\cool\\sstapp\\server\\main.go")
	}

	stdoutPipe, err := cmd.StdoutPipe()
	if err != nil {
		log.Fatalf("[Visualizer] Failed to create stdout pipe: %v", err)
	}
	cmd.Stderr = os.Stderr

	if err := cmd.Start(); err != nil {
		log.Fatalf("[Visualizer] Failed to start server process: %v", err)
	}
	log.Printf("[Visualizer] Started server process (PID %d)", cmd.Process.Pid)

	defer func() {
		if cmd.Process != nil {
			log.Printf("[Visualizer] Terminating server process (PID %d)", cmd.Process.Pid)
			cmd.Process.Kill()
			cmd.Wait()
		}
	}()

	go func() {
		scanner := bufio.NewScanner(stdoutPipe)
		for scanner.Scan() {
			line := strings.TrimSpace(scanner.Text())
			if line == "" {
				continue
			}
			log.Printf("[Server Event] %s", line)
			if strings.HasPrefix(line, "{") && strings.HasSuffix(line, "}") {
				if strings.Contains(line, `"type":"init"`) || strings.Contains(line, `"type": "init"`) {
					lastInitEventMu.Lock()
					lastInitEvent = line
					lastInitEventMu.Unlock()
				}
				broadcastEvent(line)
			}
		}
		if err := scanner.Err(); err != nil {
			log.Printf("[Visualizer] Stdout scanner error: %v", err)
		}
	}()

	http.HandleFunc("/events", handleSSE)
	http.HandleFunc("/api/status", handleAPIStatus)
	http.HandleFunc("/api/spawn_client", handleSpawnClient)
	http.HandleFunc("/api/send_data", handleSendData)
	http.HandleFunc("/api/choke_client", handleChokeClient)
	http.HandleFunc("/api/close_clients", handleCloseClients)
	http.HandleFunc("/", handleRoot)

	addr := fmt.Sprintf(":%d", visualizerPort)
	log.Printf("[Visualizer] Dashboard available at http://localhost%s", addr)
	log.Printf("[Visualizer] SSE endpoint at http://localhost%s/events", addr)

	srv := &http.Server{Addr: addr}

	stopChan := make(chan os.Signal, 1)
	signal.Notify(stopChan, os.Interrupt, syscall.SIGTERM)

	go func() {
		buf := make([]byte, 1)
		for {
			_, err := os.Stdin.Read(buf)
			if err != nil {
				select {
				case stopChan <- os.Interrupt:
				default:
				}
				return
			}
		}
	}()

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Printf("[Visualizer] HTTP server error: %v", err)
		}
	}()

	<-stopChan
	log.Printf("[Visualizer] Shutting down visualizer and backend server...")
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	srv.Shutdown(ctx)
}

