package main

import (
	"log"

	//rotatelogs "github.com/lestrrat-go/file-rotatelogs"
	// load-balanced stomp manager
	lbstomp "github.com/vkuznet/lb-stomp"
	// stomp library
	"github.com/go-stomp/stomp"
)

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
