package main

import (
	"encoding/base64"
	"errors"
	"os"
)

// buildBasicAuthHeader creates a Basic Auth header string from a username and password
func buildBasicAuthHeader() (string, error) {
	// Read username and password from environment variables
	username := os.Getenv("PINOT_USERNAME")
	password := os.Getenv("PINOT_PASSWORD")

	if username == "" || password == "" {
		return "", errors.New("error: PINOT_USERNAME or PINOT_PASSWORD environment variables are not set")
	}
	auth := username + ":" + password
	return "Basic " + base64.StdEncoding.EncodeToString([]byte(auth)), nil
}
