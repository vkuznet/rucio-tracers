package main

// These are the utilities for stompserver.
//  Authors: Yuyi Guo
// Created: June 2021

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"strings"
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
	err = json.Unmarshal(data, &Sitemap)
	if err != nil {
		log.Println("Unable to parse sitemap", err)
		return err
	}
	return nil
}

//findRSEs: For a give domain, find its RSE list
func findRSEs(domain string) []string {
	var RSEs []string
	d := strings.ToUpper(strings.TrimSpace(domain))
	if len(d) == 0 {
		return RSEs
	}
	for _, v := range domainRSEMap {
		vd := strings.ToUpper(strings.TrimSpace(v.Domain))
		if vd == d || strings.Contains(vd, d) || strings.Contains(d, vd) {
			return v.RSEs
		}
	}
	return RSEs
}

// parseRSEMap: for given srver domain to find out the list of RSEs.
func parseRSEMap(RSEfile string) error {
	data, err := ioutil.ReadFile(RSEfile)
	if err != nil {
		log.Println("Unable to read domainRSEMap.txt file", err)
		return err
	}
	err = json.Unmarshal(data, &domainRSEMap)
	if err != nil {
		log.Println("Unable to unmarshal domainRSEMap", err)
		return err
	}
	return nil
}
