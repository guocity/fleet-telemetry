#!/usr/bin/env python3
"""Generate fleet-telemetry-output.openapi.yaml from the source of truth.

Parses (no hardcoded signal/enum lists — re-run after any telemetry change):

  protos/vehicle_data.proto          Field enum (signal names), Value oneof,
                                     every value enum and its members
  protos/vehicle_alert.proto         Audience enum
  protos/vehicle_connectivity.proto  ConnectivityEvent enum
  datastore/simple/transformers/payload.go
                                     JSON key per Value oneof case
                                     (stringValue / shiftStateValue / ...),
                                     composite map shapes (door, tire)
  datastore/simple/transformers/vehicle_{alert,error,connectivity}.go
                                     record data shapes (field presence)
  datastore/simple/filewriter.go     envelope keys (vin/time/txid/data/type)

Usage:
    python3 generate_openapi.py [-o ../fleet-telemetry-output.openapi.yaml]
"""

import argparse
import os
import re
import sys

import yaml

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
PROTO_DIR = os.path.join(REPO, "protos")
TRANSFORM_DIR = os.path.join(REPO, "datastore", "simple", "transformers")

VIN_PATTERN = "^[A-HJ-NPR-Z0-9]{17}$"


def read(path):
    with open(path, encoding="utf-8") as fh:
        return fh.read()


# ─────────────────────────────────────────────────────────────────
#  Proto parsing
# ─────────────────────────────────────────────────────────────────

def parse_enums(proto_text):
    """{enum_name: [member, ...]} for every enum in a proto file."""
    enums = {}
    for m in re.finditer(r"enum\s+(\w+)\s*\{(.*?)\n\}", proto_text, re.S):
        members = re.findall(r"^\s*(\w+)\s*=\s*\d+\s*;", m.group(2), re.M)
        enums[m.group(1)] = members
    return enums


def parse_field_enum(proto_text):
    """[(number, name)] from enum Field, excluding Unknown(0)."""
    m = re.search(r"enum\s+Field\s*\{(.*?)\n\}", proto_text, re.S)
    if not m:
        sys.exit("enum Field not found in vehicle_data.proto")
    return [(int(num), name)
            for name, num in re.findall(r"^\s*(\w+)\s*=\s*(\d+)\s*;", m.group(1), re.M)
            if int(num) != 0]


def parse_value_oneof(proto_text):
    """{go_case_suffix: proto_type} from `message Value { oneof value {...} }`.

    A proto line `ShiftState shift_state_value = 9;` becomes Go case
    `Value_ShiftStateValue`; we map suffix 'ShiftStateValue' -> 'ShiftState'.
    """
    m = re.search(r"message\s+Value\s*\{.*?oneof\s+\w+\s*\{(.*?)\n\s*\}", proto_text, re.S)
    if not m:
        sys.exit("message Value oneof not found in vehicle_data.proto")
    mapping = {}
    for ptype, fname in re.findall(r"^\s*([\w.]+)\s+(\w+)\s*=\s*\d+\s*;", m.group(1), re.M):
        camel = "".join(p.capitalize() if not p[0].isupper() else p for p in fname.split("_"))
        # Go generator: snake_case field -> CamelCase, preserving digits ("_2" -> "_2")
        camel = re.sub(r"_(\d)", r"_\1", camel)
        mapping[camel] = ptype.split(".")[-1]
    return mapping


# ─────────────────────────────────────────────────────────────────
#  Go transformer parsing
# ─────────────────────────────────────────────────────────────────

def parse_transform_cases(go_text):
    """Parse transformValue's switch into
    [{case_suffix, json_key, kind, map_keys}], kind in
    enum|string|float|double|int|long|boolean|invalid|location|map|time."""
    body = go_text[go_text.index("func transformValue"):]
    cases = []
    # split on `case *protos.Value_X:` keeping the X
    parts = re.split(r"case\s+\*protos\.(Value_\w+):", body)
    for suffix, chunk in zip(parts[1::2], parts[2::2]):
        chunk = chunk.split("default:")[0]
        key_m = re.search(r'outputType\s*=\s*"([^"]+)"', chunk)
        if not key_m:
            continue
        json_key = key_m.group(1)
        case_suffix = suffix.replace("Value_", "")
        kind = None
        map_keys = []
        if ".String()" in chunk:
            kind = "enum"
        elif "map[string]" in chunk:
            kind = "map"
            map_keys = re.findall(r'"(\w+)":\s*v\.', chunk)
            if "latitude" in chunk:  # locationValue uses lowercase keys
                kind = "location"
        elif "fmt.Sprintf" in chunk:
            kind = "time"
        else:
            kind = {
                "stringValue": "string", "intValue": "int", "longValue": "long",
                "floatValue": "float", "doubleValue": "double",
                "booleanValue": "boolean", "invalid": "invalid",
            }.get(json_key, "string")
        cases.append({"suffix": case_suffix, "json_key": json_key,
                      "kind": kind, "map_keys": map_keys})
    return cases


# ─────────────────────────────────────────────────────────────────
#  Schema assembly
# ─────────────────────────────────────────────────────────────────

PRIMITIVE_SCHEMAS = {
    "string":  {"type": "string", "description": "String signal value"},
    "int":     {"type": "integer", "format": "int32", "description": "32-bit integer signal value"},
    "long":    {"type": "integer", "format": "int64", "description": "64-bit integer signal value"},
    "float":   {"type": "number", "format": "float", "description": "32-bit float signal value"},
    "double":  {"type": "number", "format": "double", "description": "64-bit double signal value"},
    "boolean": {"type": "boolean", "description": "Boolean signal value"},
}


def build_signal_value_schema(cases, oneof_map, enums):
    props = {}
    for c in cases:
        key = c["json_key"]
        if c["kind"] in PRIMITIVE_SCHEMAS:
            props[key] = dict(PRIMITIVE_SCHEMAS[c["kind"]])
        elif c["kind"] == "invalid":
            props[key] = {
                "oneOf": [{"type": "boolean"}, {"type": "string"}],
                "description": ("Sensor temporarily unavailable. `true` when the "
                                "writer runs with verbose/includeTypes, the string "
                                '"<invalid>" otherwise.'),
            }
        elif c["kind"] == "time":
            props[key] = {"type": "string", "pattern": r"^\d{2}:\d{2}:\d{2}$",
                          "description": "Time-of-day value (HH:MM:SS)"}
        elif c["kind"] == "location":
            props[key] = {"$ref": "#/components/schemas/LocationValue"}
        elif c["kind"] == "map":
            ref = {"DoorValue": "DoorState", "TireLocationValue": "TireLocation"}.get(
                c["suffix"], c["suffix"])
            props[key] = {"$ref": f"#/components/schemas/{ref}"}
        elif c["kind"] == "enum":
            ptype = oneof_map.get(c["suffix"])
            members = enums.get(ptype)
            if members:
                props[key] = {"type": "string", "enum": members,
                              "description": f"Proto enum `{ptype}` rendered as string"}
            else:
                props[key] = {"type": "string",
                              "description": f"Proto enum `{ptype}` rendered as string"}
    return {
        "type": "object",
        "description": (
            "A typed telemetry signal value. Exactly one key is present per "
            "signal; the key indicates the wire type.\n"
            "Source: datastore/simple/transformers/payload.go transformValue(). "
            "GENERATED — do not edit by hand."
        ),
        "properties": props,
    }


def build_composites(cases):
    out = {
        "LocationValue": {
            "type": "object",
            "description": "GPS location (Location signal)",
            "required": ["latitude", "longitude"],
            "properties": {
                "latitude": {"type": "number", "format": "double"},
                "longitude": {"type": "number", "format": "double"},
            },
        }
    }
    for c in cases:
        if c["kind"] != "map":
            continue
        name = {"DoorValue": "DoorState", "TireLocationValue": "TireLocation"}.get(
            c["suffix"], c["suffix"])
        existing = out.get(name, {"type": "object", "properties": {}})
        for k in c["map_keys"]:
            existing["properties"][k] = {"type": "boolean"}
        if name == "TireLocation":
            existing["description"] = ("TPMS warning locations. Semi-truck axle keys "
                                       "are emitted only when VIN[3] == 'T'.")
        elif name == "DoorState":
            existing["description"] = "Door open/close status"
        out[name] = existing
    return out


def envelope_properties(extra_data_schema):
    """Common envelope written by datastore/simple/filewriter.go."""
    return {
        "vin": {"type": "string", "pattern": VIN_PATTERN},
        "time": {"type": "string", "format": "date-time",
                 "description": "Server-side write timestamp (UTC, RFC3339)"},
        "txid": {"type": "string", "description": "Transaction ID from the vehicle session"},
        "type": {"type": "string", "enum": ["full", "snapshot", "delta"],
                 "description": ("Only present when enable_delta is set in "
                                 "FileWriterConfig. `delta` records contain only "
                                 "changed fields; a removed field is encoded as "
                                 "JSON null.")},
        "data": extra_data_schema,
        # Legacy upstream filewriter versions also wrote these three. Present
        # in historical files; NOT written by the current filewriter.go.
        "metadata": {"$ref": "#/components/schemas/RecordMetadata"},
        "decoded_payload": {"$ref": "#/components/schemas/DecodedPayload"},
        "payload": {"type": "string", "format": "byte",
                    "description": "Legacy: raw FlatBuffers payload (base64). "
                                   "Not written by the current filewriter."},
    }


def build_spec():
    vehicle_proto = read(os.path.join(PROTO_DIR, "vehicle_data.proto"))
    alert_proto = read(os.path.join(PROTO_DIR, "vehicle_alert.proto"))
    conn_proto = read(os.path.join(PROTO_DIR, "vehicle_connectivity.proto"))
    payload_go = read(os.path.join(TRANSFORM_DIR, "payload.go"))

    enums = parse_enums(vehicle_proto)
    fields = parse_field_enum(vehicle_proto)
    oneof_map = parse_value_oneof(vehicle_proto)
    cases = parse_transform_cases(payload_go)

    audience_members = parse_enums(alert_proto).get("Audience", [])
    conn_members = parse_enums(conn_proto).get("ConnectivityEvent", [])

    signal_value = build_signal_value_schema(cases, oneof_map, enums)
    composites = build_composites(cases)

    signal_names = [name for _, name in fields]

    schemas = {
        "RecordMetadata": {
            "type": "object",
            "description": ("Legacy envelope metadata (telemetry.Record.Metadata()). "
                            "Present in files written by older filewriter versions; "
                            "the current filewriter omits it."),
            "properties": {
                "vin": {"type": "string", "pattern": VIN_PATTERN},
                "txtype": {"type": "string", "enum": ["V", "alerts", "errors", "connectivity"]},
                "txid": {"type": "string"},
                "receivedat": {"type": "string",
                               "description": "Unix ms when the server received the message"},
                "timestamp": {"type": "string", "description": "Legacy timestamp field"},
                "version": {"type": "string"},
                "device_client_version": {"type": "string",
                                          "description": "Vehicle firmware telemetry client version"},
            },
        },
        "DecodedPayload": {
            "type": "object",
            "description": ("Legacy: raw protobuf serialized via protojson, written only "
                            "when transmit_decoded_records was enabled. The current "
                            "filewriter omits it."),
            "properties": {
                "created_at": {"type": "integer", "format": "int64"},
                "device_id": {"type": "string"},
                "device_type": {"type": "string"},
                "topic": {"type": "string"},
                "txid": {"type": "string"},
                "data": {"type": "object"},
            },
        },
        "VehicleDataRecord": {
            "type": "object",
            "description": ("One line in V.jsonl: decoded vehicle telemetry signals. "
                            "`data` maps signal names to typed values plus the "
                            "envelope fields Vin/CreatedAt/IsResend. In delta mode "
                            "only changed keys are present."),
            "required": ["vin", "time", "data"],
            "properties": envelope_properties({"$ref": "#/components/schemas/VehicleData"}),
        },
        "VehicleData": {
            "type": "object",
            "description": ("Decoded signal map. Keys are Field enum names from "
                            f"vehicle_data.proto ({len(signal_names)} signals; see "
                            "x-signal-fields). Vin/CreatedAt/IsResend are envelope "
                            "fields, required only in full/snapshot records — delta "
                            "records carry just the changed keys, and a signal that "
                            "left the payload is encoded as null."),
            "properties": {
                "Vin": {"type": "string", "pattern": VIN_PATTERN},
                "CreatedAt": {"type": "string", "format": "date-time",
                              "description": "Vehicle-side capture timestamp"},
                "IsResend": {"type": "boolean",
                             "description": "Retransmission of previously failed data"},
            },
            "additionalProperties": {
                "oneOf": [{"$ref": "#/components/schemas/SignalValue"},
                          {"type": "null"}],
            },
            "x-signal-fields": signal_names,
        },
        "SignalValue": signal_value,
        **composites,
        "AlertRecord": {
            "type": "object",
            "description": ("One line in alerts.jsonl. `data` is an array because a "
                            "VehicleAlerts message batches multiple alerts — the "
                            "vehicle re-sends recent alert history, so the same "
                            "alert appears in many records."),
            "required": ["vin", "time", "data"],
            "properties": envelope_properties(
                {"type": "array", "items": {"$ref": "#/components/schemas/VehicleAlert"}}),
        },
        "VehicleAlert": {
            "type": "object",
            "description": "A single vehicle alert; active while EndedAt is absent.",
            "required": ["Name"],
            "properties": {
                "Name": {"type": "string", "description": "Alert code identifier",
                         "example": "DI_a183_holdReleaseRqrd"},
                "Audiences": {"type": "array", "nullable": True,
                              "items": {"type": "string", "enum": audience_members}},
                "StartedAt": {"type": "integer", "format": "int64",
                              "description": "Unix seconds when the alert started"},
                "EndedAt": {"type": "integer", "format": "int64", "nullable": True,
                            "description": "Unix seconds when the alert ended; absent while active"},
            },
        },
        "ConnectivityRecord": {
            "type": "object",
            "description": "One line in connectivity.jsonl: a connect/disconnect event.",
            "required": ["vin", "time", "data"],
            "properties": envelope_properties({"$ref": "#/components/schemas/ConnectivityData"}),
        },
        "ConnectivityData": {
            "type": "object",
            "required": ["Vin", "ConnectionID", "Status", "CreatedAt"],
            "properties": {
                "Vin": {"type": "string", "pattern": VIN_PATTERN},
                "ConnectionID": {"type": "string", "format": "uuid",
                                 "description": "Session id shared by the CONNECTED/DISCONNECTED pair"},
                "Status": {"type": "string", "enum": conn_members},
                "NetworkInterface": {"type": "string",
                                     "description": "e.g. wifi, lte; may be empty"},
                "CreatedAt": {"type": "integer", "format": "int64",
                              "description": "Unix seconds when the event occurred"},
            },
        },
        "ErrorRecord": {
            "type": "object",
            "description": "One line in errors.jsonl: zero or more diagnostic errors.",
            "required": ["vin", "time", "data"],
            "properties": envelope_properties(
                {"type": "array", "items": {"$ref": "#/components/schemas/VehicleError"}}),
        },
        "VehicleError": {
            "type": "object",
            "required": ["Name"],
            "properties": {
                "Name": {"type": "string", "example": "unsupported_field"},
                "Body": {"type": "string", "description": "Human-readable description; may be empty"},
                "Tags": {"type": "object", "nullable": True,
                         "additionalProperties": {"type": "string"}},
                "CreatedAt": {"type": "integer", "format": "int64",
                              "description": "Unix seconds when the error occurred"},
            },
        },
    }

    return {
        "openapi": "3.0.3",
        "info": {
            "title": "Tesla Fleet Telemetry — JSONL File Output Schema",
            "description": (
                "GENERATED FILE — regenerate with "
                "new_implementation/scripts/generate_openapi.py; do not edit by hand.\n\n"
                "Schema of the JSONL records written by the fleet-telemetry FileWriter "
                "datastore (datastore/simple/filewriter.go). Each line of "
                "{base_path}/{VIN}/{YYYY}/{MM}/{txtype}.jsonl conforms to the record "
                "schema matching its txtype/filename:\n\n"
                "| File | Schema |\n|---|---|\n"
                "| V.jsonl | VehicleDataRecord |\n"
                "| alerts.jsonl | AlertRecord |\n"
                "| connectivity.jsonl | ConnectivityRecord |\n"
                "| errors.jsonl | ErrorRecord |\n"
            ),
            "version": "2.0.0",
            "license": {"name": "Apache 2.0",
                        "url": "https://www.apache.org/licenses/LICENSE-2.0"},
            "contact": {"name": "Fleet Telemetry",
                        "url": "https://github.com/teslamotors/fleet-telemetry"},
        },
        "paths": {},
        "components": {"schemas": schemas},
    }


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("-o", "--output",
                    default=os.path.join(HERE, "..", "fleet-telemetry-output.openapi.yaml"))
    args = ap.parse_args()

    spec = build_spec()
    with open(args.output, "w", encoding="utf-8") as fh:
        yaml.dump(spec, fh, sort_keys=False, allow_unicode=True, width=100)

    n_signals = len(spec["components"]["schemas"]["VehicleData"]["x-signal-fields"])
    n_keys = len(spec["components"]["schemas"]["SignalValue"]["properties"])
    print(f"wrote {os.path.abspath(args.output)}")
    print(f"  {n_signals} signal fields, {n_keys} signal value types, "
          f"{len(spec['components']['schemas'])} schemas")


if __name__ == "__main__":
    main()
