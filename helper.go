package main

import "fmt"

// Helper function to convert string to int64
func stringToInt64(s string) int64 {
	var result int64
	fmt.Sscanf(s, "%d", &result)
	return result
}
