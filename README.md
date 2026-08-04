# SSTApp - Go Rewrite

Epoll-based TCP echo server rewritten in Go.

## Project Structure

```
sstapp/
├── server/           # Go echo server (replaces epoll_visual_server.c)
├── visualizer/       # Go HTTP/SSE dashboard server
├── traffic/          # Go traffic generator
├── index.html        # Web dashboard (unchanged)
├── visualizer.py     # Original Python visualizer (kept for reference)
├── traffic_generator.py  # Original Python traffic generator (kept for reference)
└── epoll_*.c         # Original C servers (kept for reference)
```

## Prerequisites

- Go 1.21+ installed

## Quick Start

### 1. Start the server

```bash
cd server
go run main.go
```

The server listens on `:8080` and emits JSON telemetry to stdout.

### 2. Start the visualizer (optional)

In a **separate terminal**:

```bash
cd visualizer
go run main.go
```

Then open http://localhost:8000 in your browser.

### 3. Run traffic generator

In a **separate terminal**:

```bash
cd traffic
go run main.go normal 3 10    # 3 clients for 10 seconds
go run main.go churn 20        # 20 rapid connections
```

## Original C Servers (for reference)

The original C implementations are kept alongside:

- `epoll_server.c` — Basic epoll echo server
- `epoll_buffered_server.c` — Buffered write variant
- `epoll_loop.c` — Simple epoll demo with stdin
- `epoll_visual_server.c` — Full server with JSON telemetry

## JSON Telemetry Events

The server emits these JSON events to stdout:

| Event | Description |
|-------|-------------|
| `init` | Server started |
| `accept` | New client connected |
| `read` | Data received from client |
| `write` | Data sent to client |
| `backpressure` | Write buffer full |
| `buffer_cleared` | Write buffer drained |
| `close` | Client disconnected |
| `error` | Server error |
