package main

import (
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

// fetchTables get list of all realtime tables in pinot
func fetchTables(cluster Cluster, authHeader string) (map[string][]string, error) {
	url := fmt.Sprintf("%s/tables?type=realtime", cluster.PinotControllerURL)

	// Create a new HTTP request with basic auth
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	// Add the Authorization header for basic auth
	req.Header.Add("Authorization", authHeader)

	// Send the HTTP request
	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	// Read the response body
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, err
	}

	// Parse the JSON response
	var result map[string][]string
	err = json.Unmarshal(body, &result)
	if err != nil {
		return nil, err
	}

	return result, nil
}

type PartitionOffsetInfo struct {
	CurrentOffsetsMap       map[string]string `json:"currentOffsetsMap"`
	LatestUpstreamOffsetMap map[string]string `json:"latestUpstreamOffsetMap"`
	RecordsLagMap           map[string]string `json:"recordsLagMap"`
	AvailabilityLagMsMap    map[string]string `json:"availabilityLagMsMap"`
}

type ConsumingInfo struct {
	ServerName            string              `json:"serverName"`
	ConsumerState         string              `json:"consumerState"`
	LastConsumedTimestamp int64               `json:"lastConsumedTimestamp"`
	PartitionToOffsetMap  map[string]string   `json:"partitionToOffsetMap"`
	PartitionOffsetInfo   PartitionOffsetInfo `json:"partitionOffsetInfo"`
}

type SegmentToConsumingInfoMap struct {
	SegmentToConsumingInfo map[string][]ConsumingInfo `json:"_segmentToConsumingInfoMap"`
}

type ConsumingSegment struct {
	RecordsLagMap map[string]int64 `json:"recordsLagMap"`
}

// Prepare for storing the result
type PartitionLag struct {
	PartitionID string
	RecordLag   int64
}

type TableLagSummary struct {
	TableName  string
	Partitions []PartitionLag
	TotalLag   int64
}

// fetchConsumerLag fetches the consumer lag for a specific table and stores it with partition details
func fetchConsumerLag(pinotControllerURL, tableName, authHeader string, lagThreshold int64) ([]TableLagSummary, error) {
	req, err := http.NewRequest("GET", fmt.Sprintf("%s/tables/%s/consumingSegmentsInfo", pinotControllerURL, tableName), nil)
	if err != nil {
		return nil, err
	}

	// Add the Authorization header
	req.Header.Add("Authorization", authHeader)

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var consumingInfo SegmentToConsumingInfoMap
	if err := json.NewDecoder(resp.Body).Decode(&consumingInfo); err != nil {
		return nil, err
	}

	// Iterate over segments and collect table name, partition names, and their respective lags
	tableLagMap := make(map[string]*TableLagSummary)
	for _, consumingInfos := range consumingInfo.SegmentToConsumingInfo {

		// Process each ConsumingInfo
		for _, consumingInfo := range consumingInfos {
			// Check if we already have an entry for this table
			tableLagSummary, exists := tableLagMap[tableName]
			if !exists {
				// Initialize a new TableLagSummary if it doesn't exist
				tableLagSummary = &TableLagSummary{
					TableName:  tableName,
					Partitions: []PartitionLag{},
					TotalLag:   0,
				}
				tableLagMap[tableName] = tableLagSummary
			}

			// Collect partition lags
			for partitionID, recordLag := range consumingInfo.PartitionOffsetInfo.RecordsLagMap {
				lagValue := stringToInt64(recordLag)

				// Add the partition's lag to the total lag
				tableLagSummary.TotalLag += lagValue

				// Append partition lag to the Partitions slice
				tableLagSummary.Partitions = append(tableLagSummary.Partitions, PartitionLag{
					PartitionID: partitionID,
					RecordLag:   lagValue,
				})
			}
		}
	}

	// Prepare the final results
	var tableLagSummaries []TableLagSummary
	for _, summary := range tableLagMap {
		// Only consider tables where the total lag exceeds the threshold
		if summary.TotalLag >= lagThreshold {
			tableLagSummaries = append(tableLagSummaries, *summary)
		}
	}

	return tableLagSummaries, nil
}
