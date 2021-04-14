package main

import (
	"flag"
	"fmt"
	"log"

	lbstomp "github.com/vkuznet/lb-stomp"
)

// stompserver - Implementation of stomp server to consume, process and produce ActiveMQ messages
// The server will consumer message from below three topics:
// 1. XrootD: /topic/xrootd.cms.aaa.ng
// 2. CMSSW popularity: /topic/cms.swpop
// 3. WMArchive: /topic/cms.jobmon.wmarchive
// Process them, then produce a Ruci trace message and then it to topic:
// /topic/cms.rucio.tracer
//
// Authors: Yuyi Guo and Valentin Kuznetsov
// Created: Feb 2021

// stompMgr defines the stomp manager for the producer.
var stompMgr *lbstomp.StompManager

func main() {
	// usage: ./RucioTracer -config stompserverconfig.json -sitemap ../etc/ruciositemap.json

	// use this line to print in logs the filename:lineNumber for each log entry
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
	stompMgr = initStomp(Config.EndpointProducer)

	// start AMQ server to handle rucio traces
	go fwjrServer()

	// when we'll develop other servers we'll call them appropriately, e.g.
	// go cmsswServer()
	// go xrootdServer()

	// start HTTP server which can be used for metrics
	httpServer(fmt.Sprintf(":%d", Config.Port))
}
