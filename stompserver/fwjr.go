package main

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

import (
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"sync/atomic"
	"time"

	//rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	// load-balanced stomp manager

	// stomp library
	"github.com/go-stomp/stomp"
)

// sitemap  defines maps between the names from the data message and the name Ruci server has.
var sitemap map[string]string

// Lfnsite for the map of lfn and site
type Lfnsite struct {
	site string
	lfn  []string
}

// Receivedperk keeps number of messages per 1k
var Receivedperk uint64

// MetaData defines the metadata of FWJR record.
type MetaData struct {
	Ts      int64  `json:"ts"`
	JobType string `json:"jobtype"`
	WnName  string `json:"wn_name"`
}

// InputLst defines input structure of FWJR record.
type InputLst struct {
	Lfn    int    `json:"lfn"`
	Events int64  `json:"events"`
	GUID   string `json:"guid"`
}

// Step defines step structure of FWJR record.
type Step struct {
	Input []InputLst `json:"input"`
	Site  string     `json:"site"`
}

// FWJRRecord defines fwjr record structure.
type FWJRRecord struct {
	LFNArray      []string
	LFNArrayRef   []string
	FallbackFiles []int    `json:"fallbackFiles"`
	Metadata      MetaData `json:"meta_data"`
	Steps         []Step   `json:"steps"`
}

// FWJRconsumer Consumes for FWJR/WMArchive topic
func FWJRconsumer(msg *stomp.Message) ([]Lfnsite, int64, string, string, error) {
	//first to check to make sure there is something in msg,
	//otherwise we will get error:
	//Failed to continue - runtime error: invalid memory address or nil pointer dereference
	//[signal SIGSEGV: segmentation violation]
	//
	var lfnsite []Lfnsite
	var ls Lfnsite
	Received.Inc()
	atomic.AddUint64(&Receivedperk, 1)
	if msg == nil || msg.Body == nil {
		return lfnsite, 0, "", "", errors.New("Empty message")
	}
	//
	if Config.Verbose > 2 {
		log.Println("*****************Source AMQ message of wmarchive*********************")
		log.Println("Source AMQ message of wmarchive: ", string(msg.Body))
		log.Println("*******************End AMQ message of wmarchive**********************")
	}

	var rec FWJRRecord
	err := json.Unmarshal(msg.Body, &rec)
	if err != nil {
		log.Printf("Enable to Unmarchal input message. Error: %v", err)
		return lfnsite, 0, "", "", err
	}
	if Config.Verbose > 2 {
		log.Printf("******PARSED FWJR record******: %+v", rec)
	}
	// process received message, e.g. extract some fields
	var ts int64
	var jobtype string
	var wnname string
	// Check the data
	if rec.Metadata.Ts == 0 {
		ts = time.Now().Unix()
	} else {
		ts = rec.Metadata.Ts
	}

	if len(rec.Metadata.JobType) > 0 {
		jobtype = rec.Metadata.JobType
	} else {
		jobtype = "unknown"
	}

	if len(rec.Metadata.WnName) > 0 {
		wnname = rec.Metadata.WnName
	} else {
		wnname = "unknown"
	}
	//
	for _, v := range rec.Steps {
		ls.site = v.Site
		var goodlfn []string
		for _, i := range v.Input {
			if len(i.GUID) > 0 && i.Events != 0 {
				lfn := i.Lfn
				if !insliceint(rec.FallbackFiles, lfn) {
					if inslicestr(rec.LFNArrayRef, "lfn") {
						if lfn < len(rec.LFNArray) {
							goodlfn = append(goodlfn, rec.LFNArray[lfn])
						}
					}
				}
			}

		}
		if len(goodlfn) > 0 {
			ls.lfn = goodlfn
			lfnsite = append(lfnsite, ls)
		}
	}
	return lfnsite, ts, jobtype, wnname, nil
}

// FWJRtrace makes FWJR trace and send it to rucio endpoint
func FWJRtrace(msg *stomp.Message) ([]string, error) {
	var dids []string
	//get trace data
	lfnsite, ts, jobtype, wnname, err := FWJRconsumer(msg)
	if err != nil {
		log.Println("Bad FWJR message.")
		return nil, errors.New("Bad FWJR message")
	}
	for _, ls := range lfnsite {
		goodlfn := ls.lfn
		site := ls.site
		if len(goodlfn) > 0 && len(site) > 0 {
			if s, ok := sitemap[site]; ok {
				site = s
			}
			for _, glfn := range goodlfn {
				trc := NewTrace(glfn, site, ts, jobtype, wnname)
				data, err := json.Marshal(trc)
				if err != nil {
					if Config.Verbose > 0 {
						log.Printf("Unable to marshal back to JSON string , error: %v, data: %v\n", err, trc)
					} else {
						log.Printf("Unable to marshal back to JSON string, error: %v \n", err)
					}
					dids = append(dids, fmt.Sprintf("%v", trc.DID))
					continue
				}
				if Config.Verbose > 2 {
					log.Println("********* Rucio trace record ***************")
					log.Println("Rucio trace record: ", string(data))
					log.Println("******** Done Rucio trace record *************")
				}
				// send data to Stomp endpoint
				if Config.EndpointProducer != "" {
					err := stompMgr.Send(data, stomp.SendOpt.Header("appversion", "fwjrAMQ"))
					//totaltrace++
					if err != nil {
						dids = append(dids, fmt.Sprintf("%v", trc.DID))
						log.Printf("Failed to send %s to stomp.", trc.DID)
					} else {
						Send.Inc()
					}
				} else {
					log.Fatal("*** Config.Enpoint is empty, check config file! ***")
				}
			}
		}
	}
	return dids, nil
}

// fwjrServer gets messages from consumer AMQ end pointer, make tracers and send to AMQ producer end point.
func fwjrServer() {
	log.Println("Stomp broker URL: ", Config.StompURI)
	// get connection
	sub, err := subscribe(Config.EndpointConsumer)
	if err != nil {
		log.Fatal(err)
	}

	var tc uint64
	t1 := time.Now().Unix()
	var t2 int64

	for {
		// check first if subscription is still valid, otherwise get a new one
		if sub == nil {
			time.Sleep(time.Duration(Config.Interval) * time.Second)
			sub, err = subscribe(Config.EndpointConsumer)
			if err != nil {
				log.Println("unable to get new subscription", err)
				continue
			}
		}
		// get stomp messages from subscriber channel
		select {
		case msg := <-sub.C:
			if msg.Err != nil {
				log.Println("receive error message", msg.Err)
				sub, err = subscribe(Config.EndpointConsumer)
				if err != nil {
					log.Println("unable to subscribe to", Config.EndpointConsumer, err)
				}
				break
			}
			// process stomp messages
			dids, err := FWJRtrace(msg)
			if err == nil {
				Traces.Inc()
				atomic.AddUint64(&tc, 1)
				if Config.Verbose > 1 {
					log.Println("The number of traces processed in 1000 group: ", atomic.LoadUint64(&tc))
				}
			}

			if atomic.LoadUint64(&tc) == 1000 {
				atomic.StoreUint64(&tc, 0)
				t2 = time.Now().Unix() - t1
				t1 = time.Now().Unix()
				log.Printf("Processing 1000 messages while total received %d messages.\n", atomic.LoadUint64(&Receivedperk))
				log.Printf("Processing 1000 messages took %d seconds.\n", t2)
				atomic.StoreUint64(&Receivedperk, 0)
			}
			if err != nil && err.Error() != "Empty message" {
				log.Println("FWJR message processing error", err)
			}
			//got error message "FWJR message processing error unexpected end of JSON input".
			//Code stoped to loop??? YG 2/22/2021
			if len(dids) > 0 {
				log.Printf("DIDS in Error: %v .\n ", dids)
			}
		default:
			sleep := time.Duration(Config.Interval) * time.Millisecond
			if Config.Verbose > 3 {
				log.Println("waiting for ", sleep)
			}
			time.Sleep(sleep)
		}
	}
}
