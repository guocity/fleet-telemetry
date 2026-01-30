package main

import (
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"os"

	"github.com/teslamotors/fleet-telemetry/messages/tesla"
	"github.com/teslamotors/fleet-telemetry/protos"
	"google.golang.org/protobuf/encoding/protojson"
)

func main() {
	payloadFlag := flag.String("payload", "", "Base64 encoded FlatBuffers payload")
	prettyFlag := flag.Bool("pretty", true, "Pretty print JSON output")
	flag.Parse()

	if *payloadFlag == "" {
		fmt.Println("Usage: decode_payload -payload <base64-encoded-payload>")
		fmt.Println("\nExample:")
		fmt.Println(`  decode_payload -payload "FAAAAAAADgAYABQAEAAPAAgABAAOAAAAFAAAAFgAAAAAAAAEEAAAABQAAAAAAAAAAAAAAAEAAABWAAAA..."`)
		os.Exit(1)
	}

	// Decode base64
	payloadBytes, err := base64.StdEncoding.DecodeString(*payloadFlag)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Error decoding base64: %v\n", err)
		os.Exit(1)
	}

	// Parse FlatBuffers envelope
	envelope := tesla.GetRootAsFlatbuffersEnvelope(payloadBytes, 0)
	
	// Get payload type
	payloadType := envelope.PayloadType()
	payloadData := envelope.PayloadBytes()

	fmt.Printf("=== FlatBuffers Envelope ===\n")
	fmt.Printf("Payload Type: %s\n", string(payloadType))
	fmt.Printf("Device Type: %s\n", envelope.DeviceType())
	fmt.Printf("VIN: %s\n", envelope.Vin())
	fmt.Printf("Created At: %d\n", envelope.CreatedAt())
	fmt.Printf("Hermes Version: %d\n", envelope.HermesVersion())
	fmt.Printf("\n")

	// Decode based on payload type
	switch string(payloadType) {
	case "V":
		decodeVehicleData(payloadData, *prettyFlag)
	case "alerts":
		decodeAlerts(payloadData, *prettyFlag)
	case "errors":
		decodeErrors(payloadData, *prettyFlag)
	default:
		fmt.Printf("Unknown payload type: %s\n", payloadType)
		fmt.Printf("Raw payload (hex): %x\n", payloadData)
	}
}

func decodeVehicleData(data []byte, pretty bool) {
	stream := tesla.GetRootAsFlatbuffersStream(data, 0)
	
	payload := &protos.Payload{}
	
	// Basic info
	fmt.Printf("=== Vehicle Data (V) ===\n")
	fmt.Printf("Trip ID: %s\n", stream.TripId())
	fmt.Printf("Session ID: %s\n", stream.SessionId())
	
	// Iterate through signals
	signalsLen := stream.SignalsLength()
	fmt.Printf("Number of Signals: %d\n\n", signalsLen)
	
	for i := 0; i < signalsLen; i++ {
		var signal tesla.FlatbuffersSignal
		if stream.Signals(&signal, i) {
			signalName := string(signal.Name())
			fmt.Printf("Signal #%d: %s\n", i+1, signalName)
			
			// Get value union type
			valueType := signal.ValueType()
			switch valueType {
			case tesla.FlatbuffersSignalValueStringValue:
				var strValue tesla.FlatbuffersSignalValueString
				if signal.Value(&strValue) != nil {
					fmt.Printf("  Type: String\n")
					fmt.Printf("  Value: %s\n", strValue.Value())
				}
			case tesla.FlatbuffersSignalValueLongValue:
				var longValue tesla.FlatbuffersSignalValueLong
				if signal.Value(&longValue) != nil {
					fmt.Printf("  Type: Long\n")
					fmt.Printf("  Value: %d\n", longValue.Value())
				}
			case tesla.FlatbuffersSignalValueFloatValue:
				var floatValue tesla.FlatbuffersSignalValueFloat
				if signal.Value(&floatValue) != nil {
					fmt.Printf("  Type: Float\n")
					fmt.Printf("  Value: %f\n", floatValue.Value())
				}
			case tesla.FlatbuffersSignalValueDoubleValue:
				var doubleValue tesla.FlatbuffersSignalValueDouble
				if signal.Value(&doubleValue) != nil {
					fmt.Printf("  Type: Double\n")
					fmt.Printf("  Value: %f\n", doubleValue.Value())
				}
			case tesla.FlatbuffersSignalValueBooleanValue:
				var boolValue tesla.FlatbuffersSignalValueBoolean
				if signal.Value(&boolValue) != nil {
					fmt.Printf("  Type: Boolean\n")
					fmt.Printf("  Value: %t\n", boolValue.Value())
				}
			default:
				fmt.Printf("  Type: Unknown (%d)\n", valueType)
			}
			fmt.Println()
		}
	}
	
	// Convert to protobuf for full JSON representation
	if err := tesla.FlatbuffersStreamToProto(stream, payload); err == nil {
		fmt.Printf("\n=== Full Protobuf JSON ===\n")
		printProtoJSON(payload, pretty)
	}
}

func decodeAlerts(data []byte, pretty bool) {
	stream := tesla.GetRootAsFlatbuffersStream(data, 0)
	
	alerts := &protos.VehicleAlerts{}
	
	fmt.Printf("=== Vehicle Alerts ===\n")
	
	if err := tesla.FlatbuffersStreamToVehicleAlerts(stream, alerts); err == nil {
		fmt.Printf("Number of Alerts: %d\n\n", len(alerts.Alerts))
		
		for i, alert := range alerts.Alerts {
			fmt.Printf("Alert #%d:\n", i+1)
			fmt.Printf("  Name: %s\n", alert.Name)
			fmt.Printf("  Started At: %d\n", alert.StartedAt.Seconds)
			fmt.Printf("  Ended At: %d\n", alert.EndedAt.Seconds)
			fmt.Printf("  Audiences: %v\n", alert.Audiences)
			fmt.Println()
		}
		
		fmt.Printf("\n=== Full Protobuf JSON ===\n")
		printProtoJSON(alerts, pretty)
	} else {
		fmt.Printf("Error decoding alerts: %v\n", err)
	}
}

func decodeErrors(data []byte, pretty bool) {
	stream := tesla.GetRootAsFlatbuffersStream(data, 0)
	
	errors := &protos.VehicleErrors{}
	
	fmt.Printf("=== Vehicle Errors ===\n")
	
	if err := tesla.FlatbuffersStreamToVehicleErrors(stream, errors); err == nil {
		fmt.Printf("Number of Errors: %d\n\n", len(errors.Errors))
		
		for i, vehicleError := range errors.Errors {
			fmt.Printf("Error #%d:\n", i+1)
			fmt.Printf("  Name: %s\n", vehicleError.Name)
			fmt.Printf("  Body: %s\n", vehicleError.Body)
			fmt.Printf("  Tags: %v\n", vehicleError.Tags)
			fmt.Println()
		}
		
		fmt.Printf("\n=== Full Protobuf JSON ===\n")
		printProtoJSON(errors, pretty)
	} else {
		fmt.Printf("Error decoding errors: %v\n", err)
	}
}

func printProtoJSON(msg interface{}, pretty bool) {
	options := protojson.MarshalOptions{
		UseProtoNames:   false,
		EmitUnpopulated: true,
	}
	
	if pretty {
		options.Indent = "  "
	}
	
	var jsonBytes []byte
	var err error
	
	switch v := msg.(type) {
	case *protos.Payload:
		jsonBytes, err = options.Marshal(v)
	case *protos.VehicleAlerts:
		jsonBytes, err = options.Marshal(v)
	case *protos.VehicleErrors:
		jsonBytes, err = options.Marshal(v)
	default:
		fmt.Printf("Unknown message type\n")
		return
	}
	
	if err != nil {
		fmt.Printf("Error marshaling to JSON: %v\n", err)
		return
	}
	
	// Pretty print if requested
	if pretty {
		var prettyJSON interface{}
		if err := json.Unmarshal(jsonBytes, &prettyJSON); err == nil {
			if formatted, err := json.MarshalIndent(prettyJSON, "", "  "); err == nil {
				fmt.Println(string(formatted))
				return
			}
		}
	}
	
	fmt.Println(string(jsonBytes))
}
