package main

import (
	"bufio"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
)

// Record represents a telemetry record
type Record struct {
	VIN  string                 `json:"vin"`
	Time string                 `json:"time"`
	TxID string                 `json:"txid"`
	Type string                 `json:"type,omitempty"`
	Data map[string]interface{} `json:"data"`
}

func main() {
	inputFile := flag.String("input", "", "Input delta-encoded JSONL file")
	outputFile := flag.String("output", "", "Output expanded JSONL file (default: input_expanded.jsonl)")
	flag.Parse()

	if *inputFile == "" {
		fmt.Println("Usage: delta_expander -input <file> [-output <file>]")
		os.Exit(1)
	}

	if *outputFile == "" {
		ext := filepath.Ext(*inputFile)
		base := (*inputFile)[:len(*inputFile)-len(ext)]
		*outputFile = base + "_expanded" + ext
	}

	if err := expandDeltaFile(*inputFile, *outputFile); err != nil {
		fmt.Fprintf(os.Stderr, "Error: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("Successfully expanded %s to %s\n", *inputFile, *outputFile)
}

func expandDeltaFile(inputPath, outputPath string) error {
	// Open input file
	inFile, err := os.Open(inputPath)
	if err != nil {
		return fmt.Errorf("failed to open input file: %w", err)
	}
	defer inFile.Close()

	// Create output file
	outFile, err := os.Create(outputPath)
	if err != nil {
		return fmt.Errorf("failed to create output file: %w", err)
	}
	defer outFile.Close()

	// Track state per VIN
	state := make(map[string]map[string]interface{})

	scanner := bufio.NewScanner(inFile)
	encoder := json.NewEncoder(outFile)

	lineNum := 0
	for scanner.Scan() {
		lineNum++
		var record Record

		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			fmt.Fprintf(os.Stderr, "Warning: skipping invalid JSON at line %d: %v\n", lineNum, err)
			continue
		}

		// Initialize state for this VIN if needed
		if _, exists := state[record.VIN]; !exists {
			state[record.VIN] = make(map[string]interface{})
		}

		// Apply delta/snapshot to state
		for key, value := range record.Data {
			if value == nil {
				// nil means field was removed
				delete(state[record.VIN], key)
			} else {
				state[record.VIN][key] = value
			}
		}

		// Create full snapshot record
		fullRecord := Record{
			VIN:  record.VIN,
			Time: record.Time,
			TxID: record.TxID,
			Type: "expanded",
			Data: copyMap(state[record.VIN]),
		}

		// Write expanded record
		if err := encoder.Encode(fullRecord); err != nil {
			return fmt.Errorf("failed to write record at line %d: %w", lineNum, err)
		}
	}

	if err := scanner.Err(); err != nil {
		return fmt.Errorf("error reading input file: %w", err)
	}

	return nil
}

func copyMap(src map[string]interface{}) map[string]interface{} {
	dst := make(map[string]interface{}, len(src))
	for k, v := range src {
		dst[k] = v
	}
	return dst
}
