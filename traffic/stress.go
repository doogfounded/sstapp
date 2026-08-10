package main

import (
	"fmt"
	"net"
	"sync"
	"time"
)

// runStressScenario opens concurrent connections and sends rapid echo traffic.
func runStressScenario(conns int, durationSec int) {
	fmt.Printf("[Traffic] Running STRESS scenario (%d connections, %ds)...\n", conns, durationSec)

	var wg sync.WaitGroup
	start := time.Now()

	for i := 0; i < conns; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			conn, err := net.Dial("tcp", serverAddr)
			if err != nil {
				return
			}
			defer conn.Close()

			payload := []byte("STRESS_TEST_BURST_DATA")
			for time.Since(start) < time.Duration(durationSec)*time.Second {
				conn.Write(payload)
				buf := make([]byte, 1024)
				conn.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
				_, _ = conn.Read(buf)
				time.Sleep(50 * time.Millisecond)
			}
		}(i + 1)
	}

	wg.Wait()
	fmt.Println("[Traffic] Stress scenario complete!")
}
