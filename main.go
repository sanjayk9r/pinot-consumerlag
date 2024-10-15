package main

import (
	"fmt"
	"log"
)

func main() {

	// Create a basic auth header
	authHeader, err := buildBasicAuthHeader()
	if err != nil {
		fmt.Println(err)
		return
	}

	// Read Config for clusters
	config, err := loadConfig("config.json")
	if err != nil {
		log.Fatalf("Error loading config: %v", err)
	}
	var lagSummary string
	// fetch consumer lag from each cluster
	for _, cluster := range config.Clusters {
		fmt.Printf("Processing cluster: %s with baseURL %s\n", cluster.Name, cluster.PinotControllerURL)
		resultResp, err := fetchTables(cluster, authHeader)
		if err != nil {
			fmt.Println("An error occurred!!")
		}
		// Only Consider lag with configured threshold
		lagThreshold := int64(cluster.LagThreshold)
		for _, tables := range resultResp {
			for _, table := range tables {
				fmt.Printf("Fetch ConsumerLag for Table: %s\n", table)
				tableLag, err := fetchConsumerLag(cluster.PinotControllerURL, table, authHeader, lagThreshold)
				if err != nil {
					fmt.Println(err)
				}
				// Print the result
				for _, tableSummary := range tableLag {
					lagSummary += fmt.Sprintf("Table: %s, Cumulative total lag: %d\n", tableSummary.TableName, tableSummary.TotalLag)
				}
			}
		}

		fmt.Println("---------\n", lagSummary)
	}
}
