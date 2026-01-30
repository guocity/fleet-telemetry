# Tesla Fleet Telemetry - Save to File System Guide

## 📁 What Was Changed

I've configured your fleet-telemetry server to save all vehicle data to organized JSON files.

### Files Created/Modified:

1. **`datastore/simple/filewriter.go`** - New file writer that saves telemetry to disk
2. **`config/lg-server-config.json`** - Updated to use filewriter
3. **`docker-compose.yml`** - Added volume mount for data persistence
4. **`tools/decode_payload.go`** - Utility to decode FlatBuffers payloads
5. **`telemetry/producer.go`** - Added FileWriter dispatcher type

---

## 🚀 How to Use

### Step 1: Rebuild the Docker Container

```bash
cd /Users/home/Documents/github/fleet-telemetry
docker-compose down
docker build -t fleet-telemetry-integration-tests:latest .
docker-compose up -d
```

### Step 2: Data Will Be Saved Automatically

Once your vehicle connects, data will be saved to:
```
~/Data/tesla-telemetry-data/YYYY/MM/{txtype}.jsonl
```

**Example structure:**
```
~/Data/tesla-telemetry-data/
├── 2026/
│   └── 01/
│       ├── V.jsonl          # All vehicle data for January
│       ├── alerts.jsonl     # All alerts for January  
│       └── connectivity.jsonl # All connections for January
```

### Step 3: View the Data

```bash
# View vehicle data
cat ~/Data/tesla-telemetry-data/2026/01/V.jsonl | jq

# View alerts
cat ~/Data/tesla-telemetry-data/2026/01/alerts.jsonl | jq

# Count records
wc -l ~/Data/tesla-telemetry-data/2026/01/*.jsonl
```

---

## 📊 File Organization

Files are organized by year and month:

**Structure:** `~/Data/tesla-telemetry-data/YYYY/MM/{txtype}.jsonl`

- One file per month per data type
- All data for a given month and type in a single `.jsonl` file
- Easy to archive by month

**Example:**
```
~/Data/tesla-telemetry-data/
├── 2026/
│   ├── 01/
│   │   ├── V.jsonl
│   │   ├── alerts.jsonl
│   │   └── connectivity.jsonl
│   └── 02/
│       ├── V.jsonl
│       ├── alerts.jsonl
│       └── connectivity.jsonl
```

---

## 🔐 Decoding FlatBuffers Payloads

The `dispatching_message` logs contain Base64-encoded FlatBuffers data. Use the decoder:

### Build the Decoder:
```bash
cd /Users/home/Documents/github/fleet-telemetry
go build -o decode_payload ./tools/decode_payload.go
```

### Decode a Payload:
```bash
./decode_payload -payload "FAAAAAAADgAYABQAEAAPAAgABAAOAAAAFAAAAFgAAAAAAAAEEAAAABQAAAAAAAAAAAAAAAEAAABWAAAAIAAAAGU1Mjc0MTQyYzJiNDRlZmI4NGYzNjAtMDAwMDAwMDAxAAAAABAAIAAYABwAFAAQAAwABAAQAAAAzZE4EJwBAAAUAAAAKAAAADgAAAAC/HxpYAAAABEAAAA3U0FZR0RFRTJQRjg3NTEyMgAAAA4AAAB2ZWhpY2xlX2RldmljZQAAKQAAAAoNCAQSCSkAAAAAAAAAAAoKCAISBgoESWRsZRIMCIL488sGEIeUlPoBAAAAIAAAAHZlaGljbGVfZGV2aWNlLjdTQVlHREVFMlBGODc1MTIyAAAAAA=="
```

**Output:**
```
=== FlatBuffers Envelope ===
Payload Type: V
Device Type: vehicle_device
VIN: 7SAYGDEE2PF875122
Created At: 1769798668

=== Vehicle Data (V) ===
Trip ID: 
Session ID: vehicle_device.7SAYGDEE2PF875122
Number of Signals: 2

Signal #1: ChargeState
  Type: String
  Value: Idle

Signal #2: VehicleSpeed
  Type: Double
  Value: 0.000000
```

---

## 📋 Understanding the Telemetry Data

### 1. **Connectivity Records** (`txtype: connectivity`)

Tracks vehicle connection/disconnection events.

**Example:**
```json
{
  "vin": "7SAYGDEE2PF875122",
  "metadata": {
    "vin": "7SAYGDEE2PF875122",
    "txtype": "connectivity",
    "txid": "47027af9-7c7c-440b-8a91-0e71c0122427",
    "device_client_version": "1.2.0",
    "receivedat": "1769798668000"
  },
  "data": {
    "ConnectionID": "47027af9-7c7c-440b-8a91-0e71c0122427",
    "CreatedAt": 1769798668,
    "NetworkInterface": "wifi",
    "Status": "CONNECTED",
    "Vin": "7SAYGDEE2PF875122"
  },
  "time": "2026-01-30T18:44:28Z"
}
```

**Fields:**
- `Status`: `CONNECTED` or `DISCONNECTED`
- `NetworkInterface`: `wifi` or `lte`
- `ConnectionID`: Unique session identifier
- `CreatedAt`: Unix timestamp

---

### 2. **Vehicle Data Records** (`txtype: V`)

Real-time vehicle telemetry (speed, charge state, battery, etc.).

**Example:**
```json
{
  "vin": "7SAYGDEE2PF875122",
  "metadata": {
    "txtype": "V",
    "txid": "e5274142c2b44efb84f360-000000001",
    "receivedat": "1769798668000"
  },
  "data": {
    "ChargeState": {"stringValue": "Idle"},
    "VehicleSpeed": {"doubleValue": 0},
    "CreatedAt": "2026-01-30T18:44:18Z",
    "IsResend": false,
    "Vin": "7SAYGDEE2PF875122"
  },
  "time": "2026-01-30T18:44:28Z"
}
```

**Common Fields:**
- `ChargeState`: `Idle`, `Charging`, `Complete`, `Disconnected`
- `VehicleSpeed`: Speed in mph
- `BatteryLevel`: Battery percentage (0-100)
- `IsResend`: `true` if this is a retry of failed data
- `{field}: {"invalid": true}`: Sensor temporarily unavailable

---

### 3. **Alert Records** (`txtype: alerts`)

Vehicle alerts and notifications.

**Example:**
```json
{
  "vin": "7SAYGDEE2PF875122",
  "metadata": {
    "txtype": "alerts",
    "txid": "e5274142c2b44efb84f360-000000002"
  },
  "data": [{
    "Audiences": ["Customer", "Service"],
    "EndedAt": 1769282426,
    "Name": "DI_a183_holdReleaseRqrd",
    "StartedAt": 1769282424
  }],
  "time": "2026-01-30T18:44:28Z"
}
```

**Fields:**
- `Name`: Alert code (e.g., `DI_a183_holdReleaseRqrd` = "Press brake to release")
- `Audiences`: Who sees the alert (`Customer`, `Service`)
- `StartedAt/EndedAt`: Unix timestamps

**Common Alert Codes:**
- `DI_a183_holdReleaseRqrd`: Press brake to shift from Park
- `DI_vehicleSpeed_High`: Speeding alert
- `DI_lowBattery`: Low battery warning

---

### 4. **Error Records** (`txtype: errors`)

Vehicle diagnostic errors.

**Example:**
```json
{
  "vin": "7SAYGDEE2PF875122",
  "metadata": {
    "txtype": "errors"
  },
  "data": [{
    "Name": "DTC_BATTERY_TEMP_HIGH",
    "Body": "Battery temperature exceeded normal range",
    "Tags": {"severity": "warning", "system": "battery"},
    "CreatedAt": 1769798668
  }]
}
```

---

## 🔍 Analyzing Your Session

Based on your logs, here's what happened:

**Session Summary:**
- **VIN:** 7SAYGDEE2PF875122
- **Duration:** 865 seconds (~14 minutes)
- **Connection:** WiFi
- **Records Received:**
  - Vehicle Data (V): 545 records
  - Alerts: 916 records
  - Connectivity: 2 records
  - **Total:** 1,461 records

**Vehicle Status:**
- Charge State: Idle (not charging)
- Vehicle Speed: 0 mph (parked)
- Active Alert: "Press brake to release" (vehicle in Park)

---

## 🛠️ Useful Commands

### Monitor Live Data
```bash
# Watch files being created
watch -n 1 'find telemetry-data -type f -name "*.jsonl" | wc -l'

# Tail latest vehicle data
tail -f telemetry-data/7SAYGDEE2PF875122/$(date +%Y-%m-%d)/V/*.jsonl | jq
```~/Data/tesla-telemetry-data -type f -name "*.jsonl" | wc -l'

# Tail latest vehicle data
tail -f ~/Data/tesla-telemetry-data/2026/01/V.jsonl | jq
```

### Query Data
```bash
# Find all charging sessions
grep -r "Charging" ~/Data/tesla-telemetry-data/2026/01/V.jsonl | jq

# Count alerts by type
cat ~/Data/tesla-telemetry-data/2026/01/alerts.jsonl | jq -r '.data[].Name' | sort | uniq -c

# Get average speed
cat ~/Data/tesla-telemetry-data/2026/01/V.jsonl | \
  jq -r '.data.VehicleSpeed.doubleValue // 0' | \
  awk '{sum+=$1; count++} END {print sum/count}'
```

### Backup Data
```bash
# Compress monthly data
tar -czf telemetry-backup-2026-01.tar.gz ~/Data/tesla-telemetry-data/2026/01/

# Backup to remote server
rsync -avz ~/Data/tesla-
## 🎯 Next Steps

1. **Rebuild and restart** the container:
   ```bash
   docker-compose down
   docker build -t fleet-telemetry-integration-tests:latest .
   docker-compose up -d
   ```

2. **Connect your vehicle** - It will automatically start saving data

3. **Check the data**:
   ```bash
   ls -lR ~/Data/tesla-telemetry-data/
   ```

4. **Decode a payload** to understand the raw FlatBuffers:
   ```bash
   go build -o decode_payload ./tools/decode_payload.go
   ./decode_payload -payload "<paste-base64-from-logs>"
   ```

---

## 🔧 Troubleshooting

### Data not being saved?
```bash
# Check container logs
docker logs lg-fleet-telemetry | grep filewriter

# Check permissions
ls -ld telemetry-data/
```

### Disk space issues?
```bash
# Check size
du -sh ~/Data/tesla-telemetry-data/

# Clean old data (older than 30 days)
find ~/Data/tesla-telemetry-data -name "*.jsonl" -mtime +30 -delete
```

---

**Your telemetry server is now configured to save all vehicle data to organized JSON files!** 🎉
