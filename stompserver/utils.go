package main

// These are the utilities for stompserver.
//  Authors: Yuyi Guo
// Created: June 2021

import (
	"encoding/json"
	"io/ioutil"
	"log"
)

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
