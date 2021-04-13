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
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"sync/atomic"
	"time"

	//rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	// load-balanced stomp manager
	lbstomp "github.com/vkuznet/lb-stomp"
	// stomp library
	"github.com/go-stomp/stomp"
	// prometheus apis
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

// Configuration stores server configuration parameters and  options
type Configuration struct {
	// Interval of server
	Interval int `json:"interval"`
	// Verbose level for ddebugging
	Verbose int `json:"verbose"`
	// Port  defines http server port number for monitoring metrics.
	Port int `json:"port"`
	// StompURL defines StompAMQ URI for consumer and Producer.
	StompURI string `json:"stompURI"`
	// StompLogin defines StompAQM login name.
	StompLogin string `json:"stompLogin"`
	// StompPassword defines StompAQM password.
	StompPassword string `json:"stompPassword"`
	// StompIterations  defines Stomp iterations.
	StompIterations int `json:"stompIterations"`
	// StompSendTimeout defines heartbeat send timeout.
	StompSendTimeout int `json:"stompSendTimeout"`
	// StompRecvTimeout defines heartbeat recv timeout.
	StompRecvTimeout int `json:"stompRecvTimeout"`
	// EndpointConsumer defines StompAMQ endpoint Consumer.
	EndpointConsumer string `json:"endpointConsumer"`
	// EndpointProducer defines StompAMQ endpoint Producer.
	EndpointProducer string `json:"endpointProducer"`
	// ContentType of UDP packet
	ContentType string `json:"contentType"`
	// Protocol network protocol tcp4
	Protocol string `json:"Protocol"`
}

//Trace defines Rucio trace
type Trace struct {
	// EventVersion  default value API_1.21.6
	EventVersion string `json:"eventVersion"`
	// ClientState default value done
	ClientState string `json:"clientState"`
	// Scope  default value cms
	Scope string `json:"scope"`
	// EventType  default value get
	EventType string `json:"eventType"`
	// Usrdn default value /DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fwjr/CN=1/CN=fwjr/CN=0
	Usrdn string `json:"usrdn"`
	// Account default fwjr, other options are crab, cmspop, xrootd and so on.
	Account string `json:"account"`
	// Filename defines cms LFN.
	Filename string `json:"filename"`
	// RemoteSite defines where the file was read from.
	RemoteSite string `json:"remoteSite"`
	// DID is defined as cms:lfn
	DID string `json:"DID"`
	// FileReadts defines when the file is read.
	FileReadts int64 `json:"file_read_ts"`
	// Jobtype defines the type of job.
	Jobtype string `json:"jobtype"`
	// Wnname defines the name of worknode.
	Wnname string `json:"wn_name"`
	// Timestamp defines the file read timestamp, same as FileReadts.
	Timestamp int64 `json:"timestamp"`
	// TraceTimeentryUnix defines when the trace was enteried, same as FileReadts.
	TraceTimeentryUnix int64 `json:"traceTimeentryUnix"`
}

// sitemap  defines maps between the names from the data message and the name Ruci server has.
var sitemap map[string]string

// Config variable represents configuration object
var Config Configuration

// Lfnsite for the map of lfn and site
type Lfnsite struct {
	site string
	lfn  []string
}

// prometheus metrics
var (
	Received = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_received",
		Help: "The number of received messages",
	})
	Send = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_send",
		Help: "The number of send messages",
	})
	Traces = promauto.NewCounter(prometheus.CounterOpts{
		Name: "rucio_tracer_traces",
		Help: "The number of traces messages",
	})
)

// Receivedperk keeps number of messages per 1k
var Receivedperk uint64

// stompMgr defines the stomp manager for the producer.
var stompMgr *lbstomp.StompManager

// parseSitemap is a helper function to parse sitemap.
func parseSitemap(mapFile string) error {
	data, err := ioutil.ReadFile(mapFile)
	if err != nil {
		log.Println("Unable to read sitemap file", err)
		return err
	}
	//log.Println(string(data))
	err = json.Unmarshal(data, &sitemap)
	if err != nil {
		log.Println("Unable to parse sitemap", err)
		return err
	}
	return nil
}

// parseConfig is a helper function to parse configuration.
func parseConfig(configFile string) error {
	data, err := ioutil.ReadFile(configFile)
	if err != nil {
		log.Println("Unable to read config file", err)
		return err
	}
	//log.Println(string(data))
	err = json.Unmarshal(data, &Config)
	if err != nil {
		log.Println("Unable to parse config", err)
		return err
	}
	if Config.StompIterations == 0 {
		Config.StompIterations = 3 // number of Stomp attempts
	}
	if Config.ContentType == "" {
		Config.ContentType = "application/json"
	}
	if Config.StompSendTimeout == 0 {
		Config.StompSendTimeout = 5000 // miliseconds
	}
	if Config.StompRecvTimeout == 0 {
		Config.StompRecvTimeout = 5000 // miliseconds
	}
	if Config.Port == 0 {
		Config.Port = 8888 // default HTTP port
	}
	//log.Printf("%v", Config)
	return nil
}

// initStomp is a function to initialize a stomp object.
func initStomp(endpoint string) *lbstomp.StompManager {
	p := lbstomp.Config{
		URI:         Config.StompURI,
		Login:       Config.StompLogin,
		Password:    Config.StompPassword,
		Iterations:  Config.StompIterations,
		SendTimeout: Config.StompSendTimeout,
		RecvTimeout: Config.StompRecvTimeout,
		//Endpoint:    Config.EndpointProducer,
		Endpoint:    endpoint,
		ContentType: Config.ContentType,
		Protocol:    Config.Protocol,
		Verbose:     Config.Verbose,
	}
	stompManger := lbstomp.New(p)
	log.Println(stompManger.String())
	return stompManger
}

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

// NewTrace creates a new instance of Rucio Trace.
func NewTrace(lfn string, site string, ts int64, jobtype string, wnname string) Trace {
	trc := Trace{
		Account:            "fwjr",
		ClientState:        "DONE",
		Filename:           lfn,
		DID:                fmt.Sprintf("cms: %s", lfn),
		EventType:          "get",
		EventVersion:       "API_1.21.6",
		FileReadts:         ts,
		RemoteSite:         site,
		Scope:              "cms",
		Timestamp:          ts,
		TraceTimeentryUnix: ts,
		Usrdn:              "/DC=ch/DC=cern/OU=Organic Units/OU=Users/CN=fwjr/CN=0/CN=fwjr/CN=0",
		Wnname:             wnname,
	}
	return trc
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

// subscribe is a helper function to subscribe to StompAMQ end-point as a listener.
func subscribe(endpoint string) (*stomp.Subscription, error) {
	smgr := initStomp(endpoint)
	// get connection
	conn, addr, err := smgr.GetConnection()
	if err != nil {
		return nil, err
	}
	log.Println("stomp connection", conn, addr)
	// subscribe to ActiveMQ topic
	sub, err := conn.Subscribe(endpoint, stomp.AckAuto)
	if err != nil {
		log.Println("unable to subscribe to", endpoint, err)
		return nil, err
	}
	log.Println("stomp subscription", sub)
	return sub, err
}

// server gets messages from consumer AMQ end pointer, make tracers and send to AMQ producer end point.
func server() {
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

func inslicestr(s []string, v string) bool {
	for i := range s {
		if v == s[i] {
			return true
		}
	}
	return false
}
func insliceint(s []int, v int) bool {
	for i := range s {
		if v == s[i] {
			return true
		}
	}
	return false
}

// httpServer complementary http server to serve the metrics
func httpServer(addr string) {
	http.Handle("/metrics", promhttp.Handler())
	log.Fatal(http.ListenAndServe(addr, nil))
}

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
	// start HTTP server which can be used for metrics
	go httpServer(fmt.Sprintf(":%d", Config.Port))
	// start AMQ server to handle rucio traces
	server()
}
