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
	// StompURL defines StompAMQ URI for consumer and Producer.
	StompURI string `json:"stompURI"`
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
