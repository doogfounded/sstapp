package main

import (
	"log"
	"net/http"
	"time"
)

// withLogging wraps an http.Handler with basic latency and request logging.
func withLogging(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		next.ServeHTTP(w, r)
		// Omit SSE events streaming logging to avoid log spam
		if r.URL.Path != "/events" {
			log.Printf("[HTTP] %s %s took %v", r.Method, r.URL.Path, time.Since(start))
		}
	})
}
