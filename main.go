package main

import (
	"log"
	"net/http"

	"github.com/prometheus/client_golang/prometheus/promhttp"
)

const (
	defaultListenAddress = ":9184"
)

func main() {
	// Register Prometheus metrics handler
	http.Handle("/metrics", promhttp.Handler())

	// Start HTTP server
	listenAddr := defaultListenAddress
	log.Printf("Starting Confluent Cloud Exporter on %s", listenAddr)
	log.Printf("Metrics endpoint: http://localhost%s/metrics", listenAddr)

	if err := http.ListenAndServe(listenAddr, nil); err != nil {
		log.Fatalf("Error starting HTTP server: %s", err)
	}
}
