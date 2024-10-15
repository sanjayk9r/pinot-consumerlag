package main

import (
	"encoding/json"
	"fmt"
	"os"
)

// Config to hold multiple clusters
type Config struct {
	Clusters []Cluster `json:"clusters"`
}

// Spec for each cluster
type Cluster struct {
	Name               string `json:"name"`
	PinotControllerURL string `json:"pinotControllerURL"`
	LagThreshold       int64  `json:"lagThreshold"`
}

// loadConfig for All Pinot clusters in config
func loadConfig(configFile string) (*Config, error) {
	bytes, err := os.ReadFile(configFile)
	if err != nil {
		return nil, fmt.Errorf("error reading config file: %v", err)
	}
	var config Config
	err = json.Unmarshal(bytes, &config)
	if err != nil {
		return nil, fmt.Errorf("error parsing config file: %v", err)
	}
	return &config, nil
}
