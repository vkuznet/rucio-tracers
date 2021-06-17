package main

// stompserver - Implementation of stomp server to consume, process and produce ActiveMQ messages
// The server has three tracers:
// 1. xrootdTracer for  XrootD: /topic/xrootd.cms.aaa.ng
// 2. cmsswpopTracer for CMSSW popularity: /topic/cms.swpop
// 3. fwjrTracer for WMArchive: /topic/cms.jobmon.wmarchive
//
// Authors: Yuyi Guo
// Created: Feb 2021

import (
	"flag"
	"fmt"
	"log"
	"net/http"

	// load-balanced stomp manager
	// lbstomp "github.com/vkuznet/lb-stomp"
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

// stompMgr defines the stomp manager for the producer.
//var stompMgr *lbstomp.StompManager

// httpServer complementary http server to serve the metrics
func httpServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(addr, nil))
}

//
func main() {
	// usage: ./RucioTracer -config stompserverconfig.json -sitemap ../etc/ruciositemap.json

	// use this line to print in logs the filene:lineNumber for each log entry
	log.SetFlags(log.LstdFlags | log.Lshortfile)
	var config string
	var fsitemap string
	flag.StringVar(&config, "config", "", "config file name")
	flag.StringVar(&fsitemap, "sitemap", "", "runcio sitemap file")
	flag.Parse()
	err2 := parseConfig(config)
	if err2 != nil {
		log.Fatalf("Unable to parse config file %s, error: %v", config, err2)
	}
	err2 = parseSitemap(fsitemap)
	if err2 != nil {
		log.Fatalf("Unable to parse rucio sitemap file %s, error: %v", fsitemap, err2)
	}
	if Config.Verbose > 3 {
		log.Printf("%v", Config)
		log.Printf("%v", sitemap)
	}
	//stompMgr = initStomp(Config.EndpointProducer)
	// start HTTP server which can be used for metrics
	go httpServer(fmt.Sprintf(":%d", Config.Port))
	// start AMQ server to handle rucio traces
	if Config.Producer == "wmarchive" {
		fwjrServer()
	}
}
