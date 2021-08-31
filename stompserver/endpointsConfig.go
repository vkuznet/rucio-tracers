package main

// endpointConfig - Defines the consumer and producer end points, init and subscribe to end points,
// parse config files. Each tracer has its own config file.
//
// Authors: Yuyi Guo, Valentin Kuznetsov
// Created: Feb 2021

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"time"

	"github.com/go-stomp/stomp"
	lbstomp "github.com/vkuznet/lb-stomp"
)

// Configuration stores server configuration parameters and  options
type Configuration struct {
	// Interval of server
	Interval int `json:"interval"`
	// Verbose level for ddebugging
	Verbose int `json:"verbose"`
	// Port  defines http server port number for monitoring metrics.
	Port int `json:"port"`
	// StompURLTest defines StompAMQ URI testbed for consumer and Producer.
	StompURIProducer string `json:"stompURIProducer"`
	// StompURL defines StompAMQ URI for consumer and Producer.
	StompURIConsumer string `json:"stompURIConsumer"`
	// StompLogin defines StompAQM login name.
	StompLogin string `json:"stompLogin"`
	// StompPassword defines StompAQM password.
	StompPassword string `json:"stompPassword"`
	// Producer defines the system whoes data will be used to generate rucio trace, such as wmarchive
	// and cmsswpop
	Producer string `json:"producer"`
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

// Config variable represents configuration object
var Config Configuration

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

//
// initStomp is a function to initialize a stomp object of endpointProducer.
func initStomp(endpoint string, stompURI string) *lbstomp.StompManager {
	p := lbstomp.Config{
		URI:         stompURI,
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
	log.Println(stompManger.Addresses)
	return stompManger
}

// subscribe is a helper function to subscribe to StompAMQ end-point as a listener.
func subscribe(smgr *lbstomp.StompManager) (*stomp.Subscription, error) {
	// get connection
	conn, addr, err := smgr.GetConnection()
	if err != nil {
		return nil, err
	}
	log.Println("\n stomp connection", conn, addr)
	// subscribe to ActiveMQ topic
	sub, err := conn.Subscribe(smgr.Config.Endpoint, stomp.AckAuto)
	if err != nil {
		log.Println("unable to subscribe to", smgr.Config.Endpoint, err)
		return nil, err
	}
	log.Println("\n stomp subscription", sub)
	return sub, err
}

// subscribeAll is a helper function to subscribe to all StompAMQ brokers
func subscribeAll(smgr *lbstomp.StompManager) ([]*stomp.Subscription, error) {
	var subscriptions []*stomp.Subscription
	for idx, addr := range smgr.Addresses {
		conn := smgr.ConnectionPool[idx]
		log.Println("stomp connection", conn, addr)
		// subscribe to ActiveMQ topic
		sub, err := conn.Subscribe(smgr.Config.Endpoint, stomp.AckAuto)
		if err != nil {
			log.Println("unable to subscribe to: ", smgr.Config.Endpoint, ". Error message: n", err)
			return subscriptions, err
		}
		subscriptions = append(subscriptions, sub)
	}
	log.Println("stomp subscriptions", subscriptions)
	return subscriptions, nil
}

//
// listener is a function to get data from a subsciption and pass the data to a chan
func listener(smgr *lbstomp.StompManager, sub *stomp.Subscription, ch chan<- *stomp.Message, cpool int) {
	// get stomp messages from the subscriber channel
	sleep := time.Duration(Config.Interval) * time.Millisecond
	var err error
	for {
		select {
		case msg := <-sub.C:
			if Config.Verbose > 2 {
				log.Printf("********* conn pool # %d ************", cpool)
			}
			if msg.Err != nil {
				log.Println("receive error message: ", msg.Err)
				// subscription checking
				if sub != nil {
					if !sub.Active() {
						// we have to connect back to the same connection/broker with cpool
						conn := smgr.ConnectionPool[cpool]
						log.Println("stomp connection: ", conn)
						sub, err = conn.Subscribe(smgr.Config.Endpoint, stomp.AckAuto)
						if err != nil {
							log.Println("unable to subscribe to: ", Config.EndpointConsumer, ". Error message: ", err)
							break
							// wait
							//time.Sleep(sleep)
							// conn := smgr.ConnectionPool[cpool]
							//log.Println("stomp reconnected: ", conn)
							// subscribe to ActiveMQ topic
							//sub, err = conn.Subscribe(smgr.Config.Endpoint, stomp.AckAuto)
							// FIXME: listener is goroutin, exit(1) exit this gorountin ? all the entire server?
							//if err != nil {
							//log.Println("unable to subscribe to: ", Config.EndpointConsumer, ". Error message: ", err)
							//break
							//log.Fatalf("Cann't subscribe to connect %d of %s, exit(1).\n ", cpool, smgr.Config.Endpoint)
							//}
							//break
						}
					} else {
						time.Sleep(sleep)
					}
				} else {
					conn := smgr.ConnectionPool[cpool]
					sub, err = conn.Subscribe(smgr.Config.Endpoint, stomp.AckAuto)
					if err != nil {
						log.Println("unable to subscribe to: ", Config.EndpointConsumer, ". Error message: ", err)
						// wait
						time.Sleep(sleep)
						//conn := smgr.ConnectionPool[cpool]
						//log.Println("stomp reconnected: ", conn)
						// subscribe to ActiveMQ topic
						sub, err = conn.Subscribe(smgr.Config.Endpoint, stomp.AckAuto)
						// FIXME: listener is goroutin, exit(1) exit this gorountin ? or the entire server?
						if err != nil {
							log.Println("unable to subscribe to: ", Config.EndpointConsumer, ". Error message: ", err)
							break
							//log.Fatalf("Cann't subscribe to connect %d of %s, exit(1). \n", cpool, smgr.Config.Endpoint)
						}
						break
					}
				}
			} else {
				ch <- msg
			}
		default:
			time.Sleep(sleep)
		}
	}
}
