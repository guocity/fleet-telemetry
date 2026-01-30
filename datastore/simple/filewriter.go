package simple

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/teslamotors/fleet-telemetry/datastore/simple/transformers"
	logrus "github.com/teslamotors/fleet-telemetry/logger"
	"github.com/teslamotors/fleet-telemetry/messages"
	"github.com/teslamotors/fleet-telemetry/protos"
	"github.com/teslamotors/fleet-telemetry/telemetry"
	"google.golang.org/protobuf/proto"
)

// FileWriterConfig for the file writer
type FileWriterConfig struct {
	// BasePath is the root directory where files will be written
	BasePath string `json:"base_path"`
	// Verbose controls whether types are explicitly shown in the logs. Only applicable for record type 'V'.
	Verbose bool `json:"verbose"`
}

// FileProducer writes telemetry data to JSON files
type FileProducer struct {
	Config *FileWriterConfig
	logger *logrus.Logger
	mu     sync.Mutex
}

// NewFileWriter initializes the file writer
func NewFileWriter(config *FileWriterConfig, logger *logrus.Logger) (telemetry.Producer, error) {
	if config.BasePath == "" {
		config.BasePath = "/data/telemetry"
	}

	// Create base directory if it doesn't exist
	if err := os.MkdirAll(config.BasePath, 0755); err != nil {
		return nil, fmt.Errorf("failed to create base directory: %w", err)
	}

	logger.ActivityLog("filewriter_initialized", logrus.LogInfo{
		"base_path": config.BasePath,
	})

	return &FileProducer{
		Config: config,
		logger: logger,
	}, nil
}

// Close the producer
func (f *FileProducer) Close() error {
	return nil
}

// ProcessReliableAck noop method
func (f *FileProducer) ProcessReliableAck(_ *telemetry.Record) {
}

// Produce writes the data to a JSON file
func (f *FileProducer) Produce(entry *telemetry.Record) {
	data, err := f.recordToLogMap(entry, entry.Vin)
	if err != nil {
		f.logger.ErrorLog("record_logging_error", err, logrus.LogInfo{"vin": entry.Vin, "txtype": entry.TxType, "metadata": entry.Metadata()})
		return
	}

	// Create the complete record structure with both decoded data and raw payload
	record := map[string]interface{}{
		"vin":      entry.Vin,
		"metadata": entry.Metadata(),
		"data":     data,
		"time":     time.Now().UTC().Format(time.RFC3339),
	}

	// Include raw payload in base64 for debugging/verification
	if len(entry.Raw()) > 0 {
		rawBytes := entry.Raw()
		record["payload"] = base64.StdEncoding.EncodeToString(rawBytes)
		
		// Decode the payload and add decoded version
		if decodedPayload := f.decodePayload(rawBytes, entry.TxType); decodedPayload != nil {
			record["decoded_payload"] = decodedPayload
		}
	}

	// Write to file
	if err := f.writeToFile(record, entry); err != nil {
		f.logger.ErrorLog("file_write_error", err, logrus.LogInfo{"vin": entry.Vin, "txtype": entry.TxType})
		return
	}

	f.logger.ActivityLog("record_written_to_file", logrus.LogInfo{
		"vin":    entry.Vin,
		"txtype": entry.TxType,
		"txid":   entry.Txid,
	})
}

// ReportError noop method
func (f *FileProducer) ReportError(_ string, _ error, _ logrus.LogInfo) {
}

// getFilePath returns the file path based on organization strategy
func (f *FileProducer) getFilePath(entry *telemetry.Record) string {
	now := time.Now().UTC()
	yearMonth := now.Format("2006/01") // YYYY/MM format

	// /data/telemetry/{VIN}/{YYYY}/{MM}/
	dirPath := filepath.Join(f.Config.BasePath, entry.Vin, yearMonth)

	// Create directory if it doesn't exist
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		f.logger.ErrorLog("mkdir_error", err, logrus.LogInfo{"path": dirPath})
	}

	// Filename: {txtype}.jsonl - one file per month per type
	filename := fmt.Sprintf("%s.jsonl", entry.TxType)
	return filepath.Join(dirPath, filename)
}

// writeToFile writes a single record to the appropriate file
func (f *FileProducer) writeToFile(record map[string]interface{}, entry *telemetry.Record) error {
	f.mu.Lock()
	defer f.mu.Unlock()

	filePath := f.getFilePath(entry)

	// Open file in append mode, create if doesn't exist
	file, err := os.OpenFile(filePath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// Encode as JSON and write with newline
	encoder := json.NewEncoder(file)
	if err := encoder.Encode(record); err != nil {
		return fmt.Errorf("failed to encode JSON: %w", err)
	}

	return nil
}

// decodePayload decodes the raw FlatBuffers payload
func (f *FileProducer) decodePayload(rawBytes []byte, txType string) map[string]interface{} {
	streamMsg, err := messages.StreamMessageFromBytes(rawBytes)
	if err != nil {
		return nil
	}

	result := map[string]interface{}{
		"topic":       streamMsg.Topic(),
		"txid":        string(streamMsg.TXID),
		"device_type": string(streamMsg.DeviceType),
		"device_id":   string(streamMsg.DeviceID),
		"created_at":  streamMsg.CreatedAt,
	}

	// Decode the inner payload based on type
	switch txType {
	case "alerts":
		alerts := &protos.VehicleAlerts{}
		if err := proto.Unmarshal(streamMsg.Payload, alerts); err == nil {
			result["data"] = alerts
		}
	case "V":
		payload := &protos.Payload{}
		if err := proto.Unmarshal(streamMsg.Payload, payload); err == nil {
			result["data"] = payload
		}
	case "connectivity":
		conn := &protos.VehicleConnectivity{}
		if err := proto.Unmarshal(streamMsg.Payload, conn); err == nil {
			result["data"] = conn
		}
	case "errors":
		errors := &protos.VehicleErrors{}
		if err := proto.Unmarshal(streamMsg.Payload, errors); err == nil {
			result["data"] = errors
		}
	}

	return result
}

// recordToLogMap converts the data of a record to a map or slice of maps
func (f *FileProducer) recordToLogMap(record *telemetry.Record, vin string) (interface{}, error) {
	switch payload := record.GetProtoMessage().(type) {
	case *protos.Payload:
		return transformers.PayloadToMap(payload, f.Config.Verbose, vin, f.logger), nil
	case *protos.VehicleAlerts:
		alertMaps := make([]map[string]interface{}, len(payload.Alerts))
		for i, alert := range payload.Alerts {
			alertMaps[i] = transformers.VehicleAlertToMap(alert)
		}
		return alertMaps, nil
	case *protos.VehicleErrors:
		errorMaps := make([]map[string]interface{}, len(payload.Errors))
		for i, vehicleError := range payload.Errors {
			errorMaps[i] = transformers.VehicleErrorToMap(vehicleError)
		}
		return errorMaps, nil
	case *protos.VehicleConnectivity:
		return transformers.VehicleConnectivityToMap(payload), nil
	default:
		return nil, fmt.Errorf("unknown txType: %s", record.TxType)
	}
}
