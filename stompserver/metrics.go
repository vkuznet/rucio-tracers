package main

import (
	"log"
	"net/http"

	//rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	// load-balanced stomp manager

	// stomp library

	// prometheus apis
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// prometheus metrics
var (
	Received = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_fwjr_received",
		Help: "The number of received messages",
	})
	Send = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_fwjr_send",
		Help: "The number of send messages",
	})
	Traces = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_fwjr_traces",
		Help: "The number of traces messages",
	})
)

// httpServer complementary http server to serve the metrics
func httpServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(addr, nil))
}
