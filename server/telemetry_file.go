package main

import (
	"os"
	"sync"
)

var fileLogMu sync.Mutex

// logEventToFile appends JSON telemetry to telemetry.log in an isolated manner.
func logEventToFile(data []byte) {
	fileLogMu.Lock()
	defer fileLogMu.Unlock()

	f, err := os.OpenFile("telemetry.log", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return
	}
	defer f.Close()

	_, _ = f.Write(append(data, '\n'))
}
