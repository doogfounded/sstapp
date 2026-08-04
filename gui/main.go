package main

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os/exec"
	"strconv"
	"strings"
	"time"

	"fyne.io/fyne/v2"
	"fyne.io/fyne/v2/app"
	"fyne.io/fyne/v2/container"
	"fyne.io/fyne/v2/dialog"
	"fyne.io/fyne/v2/layout"
	"fyne.io/fyne/v2/widget"
)

const (
	serverPort    = 8080
	visualizerURL = "http://localhost:8000"
)

// --- Data models ---

type ClientInfo struct {
	FD           int
	Backpressure bool
	WriteLen     int
	Capacity     int
	ConnectedAt  time.Time
}

type LogEntry struct {
	Timestamp time.Time
	Type      string
	Details   string
}

// --- Main app ---

type Dashboard struct {
	wn          fyne.Window
	state       *DashboardState
	server      *exec.Cmd
	logEntries  []LogEntry
	clientMap   map[int]*ClientInfo
	spawnBtn    *widget.Button
	sendBtn     *widget.Button
	floodBtn    *widget.Button
	chokeBtn    *widget.Button
	closeBtn    *widget.Button
	clientList  *widget.List
	eventList   *widget.List
	statusLabel *widget.Label
	listenFDLbl *widget.Label
	epollFDLbl  *widget.Label
}

type DashboardState struct {
	ListenFD  int
	EpollFD   int
	Port      int
	StartedAt time.Time
}

func NewDashboard() *Dashboard {
	return &Dashboard{
		state:      &DashboardState{},
		logEntries: make([]LogEntry, 0),
		clientMap:  make(map[int]*ClientInfo),
	}
}

func (d *Dashboard) Run() {
	a := app.New()
	d.wn = a.NewWindow("Go Epoll Dashboard")

	// Start the visualizer (which starts the server)
	d.startServer()
	defer d.stopServer()

	// Connect to SSE for real-time events
	go d.connectSSE()

	// Build UI
	d.buildUI()

	d.wn.SetOnClosed(func() {
		d.stopServer()
	})

	d.wn.ShowAndRun()
}

func (d *Dashboard) startServer() {
	cmd := exec.Command("go", "run", "D:\\Desktop\\cool\\sstapp\\visualizer\\main.go")
	r, _ := io.Pipe()
	cmd.Stdin = r
	cmd.Stdout = nil
	cmd.Stderr = nil
	if err := cmd.Start(); err != nil {
		log.Printf("Failed to start visualizer: %v", err)
	}
	d.server = cmd
	log.Printf("Started visualizer (PID %d)", cmd.Process.Pid)
}

func (d *Dashboard) stopServer() {
	if d.server != nil && d.server.Process != nil {
		log.Printf("Stopping visualizer/server process (PID %d)...", d.server.Process.Pid)
		_ = d.server.Process.Kill()
		_ = d.server.Wait()
		d.server = nil
	}
}

func (d *Dashboard) connectSSE() {
	for {
		resp, err := http.Get(visualizerURL + "/events")
		if err != nil {
			time.Sleep(500 * time.Millisecond)
			continue
		}

		reader := bufio.NewReader(resp.Body)
		for {
			line, err := reader.ReadString('\n')
			if err != nil {
				resp.Body.Close()
				time.Sleep(500 * time.Millisecond)
				break
			}
			if !strings.HasPrefix(line, "data: ") {
				continue
			}
			data := strings.TrimSpace(strings.TrimPrefix(line, "data: "))
			d.handleEvent(data)
		}
	}
}

func (d *Dashboard) handleEvent(data string) {
	var event map[string]interface{}
	if err := json.Unmarshal([]byte(data), &event); err != nil {
		return
	}

	eventType, _ := event["type"].(string)
	switch eventType {
	case "init":
		d.handleInit(event)
	case "accept":
		d.handleAccept(event)
	case "read":
		d.handleRead(event)
	case "write":
		d.handleWrite(event)
	case "backpressure":
		d.handleBackpressure(event)
	case "buffer_cleared":
		d.handleBufferCleared(event)
	case "close":
		d.handleClose(event)
	case "sse_connected":
		d.addLog("SSE Connected", "Connected to visualizer")
	}
}

func (d *Dashboard) handleInit(event map[string]interface{}) {
	port, _ := event["port"].(float64)
	listenFD, _ := event["listen_fd"].(float64)
	epollFD, _ := event["epoll_fd"].(float64)

	d.state.Port = int(port)
	d.state.ListenFD = int(listenFD)
	d.state.EpollFD = int(epollFD)
	d.state.StartedAt = time.Now()

	d.statusLabel.SetText(fmt.Sprintf("● Server Active | Port: %d | Clients: %d",
		d.state.Port, len(d.clientMap)))
	d.listenFDLbl.SetText(strconv.Itoa(d.state.ListenFD))
	d.epollFDLbl.SetText(strconv.Itoa(d.state.EpollFD))
	d.addLog("Server Init", fmt.Sprintf("Port: %d, Listen FD: %d, Epoll FD: %d", port, listenFD, epollFD))
}

func (d *Dashboard) handleAccept(event map[string]interface{}) {
	fd, _ := event["fd"].(float64)
	d.clientMap[int(fd)] = &ClientInfo{FD: int(fd), ConnectedAt: time.Now()}
	d.addLog("Client Connected", fmt.Sprintf("fd=%d", int(fd)))
	d.clientList.Refresh()
}

func (d *Dashboard) handleRead(event map[string]interface{}) {
	fd, _ := event["fd"].(float64)
	bytesRead, _ := event["bytes_read"].(float64)
	d.addLog("Read", fmt.Sprintf("fd=%d, bytes=%d", int(fd), int(bytesRead)))
}

func (d *Dashboard) handleWrite(event map[string]interface{}) {
	fd, _ := event["fd"].(float64)
	bytesWritten, _ := event["bytes_written"].(float64)
	d.addLog("Write", fmt.Sprintf("fd=%d, bytes=%d", int(fd), int(bytesWritten)))
}

func (d *Dashboard) handleBackpressure(event map[string]interface{}) {
	fd, _ := event["fd"].(float64)
	active, _ := event["active"].(bool)
	if active {
		d.addLog("BACKPRESSURE", fmt.Sprintf("fd=%d — write buffer full!", int(fd)))
	} else {
		d.addLog("Backpressure Cleared", fmt.Sprintf("fd=%d — buffer drained", int(fd)))
	}
}

func (d *Dashboard) handleBufferCleared(event map[string]interface{}) {
	fd, _ := event["fd"].(float64)
	d.addLog("Buffer Cleared", fmt.Sprintf("fd=%d", int(fd)))
}

func (d *Dashboard) handleClose(event map[string]interface{}) {
	fd, _ := event["fd"].(float64)
	delete(d.clientMap, int(fd))
	d.addLog("Client Disconnected", fmt.Sprintf("fd=%d", int(fd)))
	d.clientList.Refresh()
}

func (d *Dashboard) addLog(eventType, details string) {
	entry := LogEntry{
		Timestamp: time.Now(),
		Type:      eventType,
		Details:   details,
	}
	d.logEntries = append(d.logEntries, entry)
	if len(d.logEntries) > 500 {
		d.logEntries = d.logEntries[len(d.logEntries)-500:]
	}
	d.eventList.Refresh()
}

func (d *Dashboard) buildUI() {
	// Status bar
	d.statusLabel = widget.NewLabel("● Connecting...")
	d.listenFDLbl = widget.NewLabel("-")
	d.epollFDLbl = widget.NewLabel("-")

	statusBar := container.NewHBox(
		widget.NewLabel("Status:"),
		d.statusLabel,
		layout.NewSpacer(),
		widget.NewLabel("Listen FD:"),
		d.listenFDLbl,
		widget.NewLabel("Epoll FD:"),
		d.epollFDLbl,
	)

	// Control buttons
	d.spawnBtn = widget.NewButton("➕ Spawn Client", func() {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", serverPort))
		if err != nil {
			dialog.ShowError(err, d.wn)
			return
		}
		conn.Close()
	})

	d.sendBtn = widget.NewButton("💬 Send 1 KB", func() {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", serverPort))
		if err != nil {
			dialog.ShowError(err, d.wn)
			return
		}
		payload := make([]byte, 1024)
		for i := range payload {
			payload[i] = 'A'
		}
		conn.Write(payload)
		buf := make([]byte, 65536)
		conn.Read(buf)
		conn.Close()
	})

	d.floodBtn = widget.NewButton("💥 Send 16 KB", func() {
		conn, err := net.Dial("tcp", fmt.Sprintf("127.0.0.1:%d", serverPort))
		if err != nil {
			dialog.ShowError(err, d.wn)
			return
		}
		payload := make([]byte, 16384)
		for i := range payload {
			payload[i] = 'B'
		}
		conn.Write(payload)
		buf := make([]byte, 65536)
		conn.Read(buf)
		conn.Close()
	})

	d.chokeBtn = widget.NewButton("🛑 Toggle Choke", func() {
		d.addLog("Choke", "Socket read choke toggled")
	})

	d.closeBtn = widget.NewButton("🧹 Close All", func() {
		d.addLog("Close All", "All clients closed")
	})

	controls := container.NewVBox(
		d.spawnBtn,
		d.sendBtn,
		d.floodBtn,
		d.chokeBtn,
		d.closeBtn,
	)

	// Client list widget
	d.clientList = widget.NewList(
		func() int { return len(d.clientMap) },
		func() fyne.CanvasObject {
			return widget.NewLabel("Client")
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			i := 0
			var selectedFD int
			for fd := range d.clientMap {
				if i == int(id) {
					selectedFD = fd
					break
				}
				i++
			}
			c := d.clientMap[selectedFD]
			if c == nil {
				return
			}
			bp := "Ready"
			if c.Backpressure {
				bp = "BACKPRESSURE"
			}
			obj.(*widget.Label).SetText(fmt.Sprintf("fd=%d | %s | %d/%d bytes",
				c.FD, bp, c.WriteLen, c.Capacity))
		},
	)

	// Event log widget
	d.eventList = widget.NewList(
		func() int { return len(d.logEntries) },
		func() fyne.CanvasObject {
			return widget.NewLabel("Event")
		},
		func(id widget.ListItemID, obj fyne.CanvasObject) {
			entry := d.logEntries[id]
			obj.(*widget.Label).SetText(fmt.Sprintf("[%s] %s: %s",
				entry.Timestamp.Format("15:04:05"), entry.Type, entry.Details))
		},
	)

	// Main layout
	mainContent := container.NewBorder(
		statusBar,
		nil,
		controls,
		nil,
		container.NewHSplit(
			container.NewVBox(
				widget.NewLabel("Epoll Architecture"),
				d.clientList,
			),
			container.NewVBox(
				widget.NewLabel("Event Telemetry"),
				d.eventList,
			),
		),
	)

	d.wn.SetContent(mainContent)
	d.wn.Resize(fyne.NewSize(1200, 800))
}

func main() {
	d := NewDashboard()
	d.Run()
}
