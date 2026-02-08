# Compact Telemetry Storage Format

## Overview
Reduces storage by 70-80% using delta encoding and compact representation.

## File Structure

### Session File: `{txtype}_session_{timestamp}.jsonl`
First line contains session metadata:
```json
{
  "type": "session_start",
  "vin": "5YJ3E1EA2JF013888",
  "session_id": "2378188ff14345fc84c107",
  "start_time": "2026-01-31T22:53:11Z",
  "device_client_version": "1.1.0",
  "field_map": {
    "1": "BrickVoltageMax",
    "2": "BrickVoltageMin",
    "3": "PackCurrent",
    "4": "PackVoltage",
    "5": "NumBrickVoltageMin",
    "6": "NumModuleTempMax",
    "7": "NumModuleTempMin",
    "8": "ModuleTempMax",
    "9": "InsideTemp",
    "10": "DoorState"
  }
}
```

### Data Records (delta-encoded)
Only changed values are stored:
```json
{"t": 1769899991, "d": {"1": 3.506, "2": 3.502, "3": -0.8, "4": 335.91, "5": 45, "6": 10, "7": 1, "8": 22}}
{"t": 1769899997, "d": {"9": 13.0}}
{"t": 1769899999, "d": {"10": {"df": 0, "dr": 0, "pf": 0, "pr": 0, "tf": 0, "tr": 0}}}
{"t": 1769900006, "d": {"8": 22}}
```

**Keys:**
- `t` = timestamp (unix epoch)
- `d` = changed data fields only
- Field IDs from field_map

## Storage Savings Example

**Original format (5 records):**
```
Record 1: 1024 bytes (full metadata + payload + decoded)
Record 2: 856 bytes
Record 3: 892 bytes  
Record 4: 865 bytes
Total: ~3.6KB
```

**Compact format:**
```
Session header: 380 bytes (one-time)
Record 1: 95 bytes (8 fields changed)
Record 2: 28 bytes (1 field changed)
Record 3: 52 bytes (1 field changed)
Record 4: 28 bytes (1 field changed)
Total: ~583 bytes
```

**Savings: ~84% reduction**

## Additional Optimizations

### 1. Compression
Enable gzip compression (already supported by most tools):
- Further 60-70% reduction on compressed size
- Transparent decompression when reading

### 2. Periodic Snapshots
Every N records (e.g., 100), write full snapshot for:
- Recovery point
- Easier querying
- Avoid long delta chains

### 3. Type-specific encoding
- Booleans as bits
- Common values as enum codes
- Timestamps as offsets from session start

## Implementation Options

### Option A: Compact Delta (Recommended)
- Best compression ratio
- Requires field mapping
- Ideal for historical storage

### Option B: Readable Delta
- JSON with full field names but only changed values
- 40-50% savings
- Easier debugging

### Option C: Hybrid
- Compact for long-term storage
- Readable for recent/active data
- Conversion tool provided
