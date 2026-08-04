package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"time"
)

const (
	serverAddr = "127.0.0.1:8080"
)

func runNormalScenario(numClients, durationSec int) {
	fmt.Printf("[Traffic] Running NORMAL scenario (%d clients, %ds)...\n", numClients, durationSec)

	sockets := make([]net.Conn, numClients)
	for i := 0; i < numClients; i++ {
		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			log.Fatalf("[Traffic] Failed to connect client %d: %v", i+1, err)
		}
		sockets[i] = conn
		fmt.Printf("  -> Connected client %d\n", i+1)
		time.Sleep(200 * time.Millisecond)
	}

	payload := []byte("Hello from Go traffic generator! ")
	start := time.Now()

	for time.Since(start) < time.Duration(durationSec)*time.Second {
		for i, conn := range sockets {
			conn.Write(payload)
			buf := make([]byte, 4096)
			conn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, _ := conn.Read(buf)
			fmt.Printf("  -> Client %d sent %d bytes, received %d bytes\n", i+1, len(payload), n)
			time.Sleep(500 * time.Millisecond)
		}
	}

	for _, conn := range sockets {
		conn.Close()
	}
	fmt.Println("[Traffic] Normal scenario complete!")
}

func runChurnScenario(count int) {
	fmt.Printf("[Traffic] Running RAPID CHURN scenario (%d connections)...\n", count)

	for i := 0; i < count; i++ {
		conn, err := net.Dial("tcp", serverAddr)
		if err != nil {
			log.Printf("[Traffic] Connect failed: %v", err)
			continue
		}
		conn.Write([]byte("PING"))
		conn.Close()

		if (i+1)%10 == 0 {
			fmt.Printf("  -> %d/%d connections completed\n", i+1, count)
		}
	}
	fmt.Println("[Traffic] Churn scenario complete!")
}

func main() {
	fmt.Println("=== Go Traffic Generator ===")
	fmt.Println("Usage: go run . <scenario> [args]")
	fmt.Println()
	fmt.Println("Scenarios:")
	fmt.Println("  normal <num_clients> <duration_seconds>")
	fmt.Println("  churn <num_connections>")
	fmt.Println()

	// Default: run normal scenario if no args
	numClients := 3
	duration := 10

	if len(os.Args) >= 2 {
		switch os.Args[1] {
		case "normal":
			if len(os.Args) >= 4 {
				fmt.Sscanf(os.Args[2], "%d", &numClients)
				fmt.Sscanf(os.Args[3], "%d", &duration)
			}
			runNormalScenario(numClients, duration)
		case "churn":
			count := 20
			if len(os.Args) >= 3 {
				fmt.Sscanf(os.Args[2], "%d", &count)
			}
			runChurnScenario(count)
		default:
			fmt.Printf("Unknown scenario: %s\n", os.Args[1])
			os.Exit(1)
		}
	} else {
		runNormalScenario(numClients, duration)
	}
}
