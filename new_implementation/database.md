# TimescaleDB Storage Guide for Fleet Telemetry

This guide describes how to efficiently store Tesla fleet telemetry data
(from the JSONL file output) in [TimescaleDB](https://www.timescale.com/).

See [fleet-telemetry-output.openapi.yaml](fleet-telemetry-output.openapi.yaml)
for the complete schema of each JSONL record type.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Why TimescaleDB?](#why-timescaledb)
- [Schema Design](#schema-design)
  - [Why EAV Over Wide Table](#why-eav-over-wide-table)
  - [Table: vehicle_signals](#table-vehicle_signals)
  - [Table: vehicle_alerts](#table-vehicle_alerts)
  - [Table: vehicle_connectivity](#table-vehicle_connectivity)
  - [Table: vehicle_errors](#table-vehicle_errors)
- [Compression](#compression)
- [Retention Policies](#retention-policies)
- [Continuous Aggregates](#continuous-aggregates)
- [Ingestion Strategy](#ingestion-strategy)
  - [Transforming V.jsonl](#transforming-vjsonl)
  - [Transforming alerts.jsonl](#transforming-alertsjsonl)
  - [Transforming connectivity.jsonl](#transforming-connectivityjsonl)
  - [Transforming errors.jsonl](#transforming-errorsjsonl)
  - [Batch Loading with COPY](#batch-loading-with-copy)
- [Example Queries](#example-queries)
- [Docker Setup](#docker-setup)
- [Storage Estimates](#storage-estimates)

---

## Architecture Overview

```
┌──────────────┐     ┌──────────────────┐     ┌──────────────────────┐
│   Vehicle    │────▶│  Fleet Telemetry │────▶│  JSONL Files         │
│  (via TLS)   │     │  Server          │     │  V.jsonl             │
│              │     │  (filewriter.go) │     │  alerts.jsonl        │
└──────────────┘     └──────────────────┘     │  connectivity.jsonl  │
                                              │  errors.jsonl        │
                                              └──────────┬───────────┘
                                                         │
                                                   Ingestion Script
                                                   (Go or Python)
                                                         │
                                              ┌──────────▼───────────┐
                                              │  TimescaleDB         │
                                              │  ┌────────────────┐  │
                                              │  │vehicle_signals │  │  ← highest volume
                                              │  │vehicle_alerts  │  │
                                              │  │vehicle_conn.   │  │
                                              │  │vehicle_errors  │  │
                                              │  └────────────────┘  │
                                              └──────────────────────┘
```

---

## Why TimescaleDB?

| Feature | Benefit for Telemetry |
|---------|----------------------|
| **Hypertables** | Automatic time-based partitioning — old chunks compress/drop without table locks |
| **Compression** | 90-95% storage reduction for historical telemetry data |
| **Continuous aggregates** | Pre-computed rollups (hourly/daily) for dashboards |
| **Full SQL** | JOINs, window functions, CTEs — no query language limitations |
| **PostgreSQL ecosystem** | Works with every PostgreSQL tool (psql, pgAdmin, Grafana, etc.) |
| **Retention policies** | Automatic old-data cleanup without cron jobs |

---

## Schema Design

### Why EAV Over Wide Table

Vehicle telemetry has **259 possible signal fields** (defined in `protos/vehicle_data.proto`),
but each record typically contains only **2–5 signals**. This makes a wide table (one column
per signal) extremely wasteful:

| Criterion | Wide Table (259 cols) | EAV (signal per row) |
|-----------|----------------------|---------------------|
| **Sparsity** | 254+ NULL columns per row | No NULLs — only present signals stored |
| **Schema evolution** | `ALTER TABLE ADD COLUMN` for new signals | Zero schema changes |
| **Compression** | Poor — NULLs compress but waste catalog space | Excellent — homogeneous data per segment |
| **Query: "ChargeState for VIN X"** | Scans wide rows | Direct index hit on `(vin, signal_name, time)` |
| **Query: "latest state for VIN X"** | Returns 259 cols, most NULL | `DISTINCT ON (signal_name)` |
| **Disk usage** | ~60% wasted on NULLs | Compact, real data only |

> **Note:** If you need a wide view, create it as a **continuous aggregate** or **pivot view**
> on top of the EAV table. Don't make the base table wide.

---

### Table: `vehicle_signals`

Stores decoded vehicle telemetry from `V.jsonl`. **Highest volume table.**

```sql
CREATE EXTENSION IF NOT EXISTS timescaledb;

CREATE TABLE vehicle_signals (
    time           TIMESTAMPTZ      NOT NULL,  -- server write time (envelope "time")
    created_at     TIMESTAMPTZ      NOT NULL,  -- vehicle-side timestamp (data.CreatedAt)
    vin            TEXT             NOT NULL,   -- 17-char VIN
    txid           TEXT             NOT NULL,   -- transaction ID
    signal_name    TEXT             NOT NULL,   -- Field enum name: 'ChargeState', 'VehicleSpeed', etc.
    value_type     TEXT             NOT NULL,   -- 'string','int','long','float','double',
                                               -- 'boolean','invalid','location','door',
                                               -- 'tire','time','enum'
    value_string   TEXT,                        -- for string & enum type values
    value_numeric  DOUBLE PRECISION,            -- for int/long/float/double values
    value_boolean  BOOLEAN,                     -- for boolean values
    value_json     JSONB,                       -- for complex: location, door, tire
    is_resend      BOOLEAN          DEFAULT FALSE
);

-- Convert to hypertable with 7-day chunks
SELECT create_hypertable('vehicle_signals', 'time',
    chunk_time_interval => INTERVAL '7 days'
);

-- Primary query pattern: signal history for a specific VIN
CREATE INDEX idx_vs_vin_signal_time
    ON vehicle_signals (vin, signal_name, time DESC);
```

**Column mapping from JSONL:**

| JSONL field | Column | Notes |
|-------------|--------|-------|
| `time` | `time` | Parse RFC3339 string |
| `data.CreatedAt` | `created_at` | Parse RFC3339 string |
| `vin` | `vin` | Direct |
| `txid` | `txid` | Direct |
| `data.IsResend` | `is_resend` | Direct |
| `data.{SignalName}` | `signal_name` | Map key becomes the signal name |
| `data.{SignalName}.{typeKey}` | `value_type` + appropriate column | See [type mapping](#signal-value-type-mapping) |

#### Signal Value Type Mapping

| JSONL type key | `value_type` | Store in |
|----------------|-------------|----------|
| `stringValue` | `string` | `value_string` |
| `intValue` | `int` | `value_numeric` |
| `longValue` | `long` | `value_numeric` |
| `floatValue` | `float` | `value_numeric` |
| `doubleValue` | `double` | `value_numeric` |
| `booleanValue` | `boolean` | `value_boolean` |
| `invalid` | `invalid` | `value_boolean = true` |
| `locationValue` | `location` | `value_json` — `{"latitude": ..., "longitude": ...}` |
| `doorValue` | `door` | `value_json` — `{"DriverFront": false, ...}` |
| `tireLocation` | `tire` | `value_json` — `{"FrontLeft": true, ...}` |
| `time` | `time` | `value_string` — `"14:30:00"` |
| `shiftStateValue`, `sentryModeState`, etc. | `enum` | `value_string` — enum string |

---

### Table: `vehicle_alerts`

Stores decoded alerts from `alerts.jsonl`.

```sql
CREATE TABLE vehicle_alerts (
    time           TIMESTAMPTZ   NOT NULL,  -- server write time
    vin            TEXT          NOT NULL,
    txid           TEXT          NOT NULL,
    alert_name     TEXT          NOT NULL,   -- e.g. "DI_a183_holdReleaseRqrd"
    audiences      TEXT[],                   -- PostgreSQL array: {'Customer','Service'}
    started_at     TIMESTAMPTZ,              -- converted from Unix seconds
    ended_at       TIMESTAMPTZ               -- NULL = still active
);

SELECT create_hypertable('vehicle_alerts', 'time',
    chunk_time_interval => INTERVAL '7 days'
);

CREATE INDEX idx_alerts_vin_time
    ON vehicle_alerts (vin, time DESC);

CREATE INDEX idx_alerts_name
    ON vehicle_alerts (alert_name, time DESC);
```

**Column mapping from JSONL:**

| JSONL field | Column | Notes |
|-------------|--------|-------|
| `time` | `time` | Parse RFC3339 |
| `vin` | `vin` | Direct |
| `txid` | `txid` | Direct |
| `data[].Name` | `alert_name` | Unnest array — 1 row per alert |
| `data[].Audiences` | `audiences` | JSON array → PostgreSQL `TEXT[]` |
| `data[].StartedAt` | `started_at` | `to_timestamp(unix_seconds)` |
| `data[].EndedAt` | `ended_at` | `to_timestamp(unix_seconds)` or NULL |

---

### Table: `vehicle_connectivity`

Stores connection events from `connectivity.jsonl`. Very low volume.

```sql
CREATE TABLE vehicle_connectivity (
    time              TIMESTAMPTZ   NOT NULL,  -- server write time
    vin               TEXT          NOT NULL,
    txid              TEXT          NOT NULL,
    connection_id     TEXT          NOT NULL,   -- UUID session identifier
    status            TEXT          NOT NULL,   -- 'CONNECTED' | 'DISCONNECTED'
    network_interface TEXT,                     -- 'wifi' | 'lte'
    created_at        TIMESTAMPTZ   NOT NULL    -- vehicle-side event timestamp
);

SELECT create_hypertable('vehicle_connectivity', 'time',
    chunk_time_interval => INTERVAL '30 days'  -- low volume → larger chunks
);

CREATE INDEX idx_conn_vin_time
    ON vehicle_connectivity (vin, time DESC);
```

---

### Table: `vehicle_errors`

Stores diagnostic errors from `errors.jsonl`.

```sql
CREATE TABLE vehicle_errors (
    time           TIMESTAMPTZ   NOT NULL,  -- server write time
    vin            TEXT          NOT NULL,
    txid           TEXT          NOT NULL,
    error_name     TEXT          NOT NULL,   -- DTC code
    body           TEXT,                     -- human-readable description
    tags           JSONB,                    -- {"severity":"warning","system":"battery"}
    created_at     TIMESTAMPTZ              -- vehicle-side timestamp
);

SELECT create_hypertable('vehicle_errors', 'time',
    chunk_time_interval => INTERVAL '30 days'
);

CREATE INDEX idx_errors_vin_time
    ON vehicle_errors (vin, time DESC);

CREATE INDEX idx_errors_name
    ON vehicle_errors (error_name, time DESC);
```

---

## Compression

TimescaleDB native compression achieves **90–95% storage reduction** on telemetry data.
Compress chunks older than a configurable threshold (recent data stays uncompressed for
fast writes and ad-hoc queries).

```sql
-- ── Vehicle signals (compress after 2 days) ──────────────────
ALTER TABLE vehicle_signals SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'vin, signal_name',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('vehicle_signals', INTERVAL '2 days');

-- ── Alerts (compress after 7 days) ───────────────────────────
ALTER TABLE vehicle_alerts SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'vin',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('vehicle_alerts', INTERVAL '7 days');

-- ── Connectivity (compress after 7 days) ─────────────────────
ALTER TABLE vehicle_connectivity SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'vin',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('vehicle_connectivity', INTERVAL '7 days');

-- ── Errors (compress after 7 days) ───────────────────────────
ALTER TABLE vehicle_errors SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'vin',
    timescaledb.compress_orderby = 'time DESC'
);
SELECT add_compression_policy('vehicle_errors', INTERVAL '7 days');
```

### Why these `segmentby` choices?

- **`vin, signal_name`** for `vehicle_signals`: Queries almost always filter on
  both VIN and signal name. Segmenting by both means TimescaleDB can skip
  irrelevant segments entirely during decompression.
- **`vin`** for other tables: Lower volume, VIN is the primary filter dimension.

---

## Retention Policies

Automatically drop data older than a configurable period:

```sql
-- Keep 1 year of data (adjust as needed)
SELECT add_retention_policy('vehicle_signals',      INTERVAL '1 year');
SELECT add_retention_policy('vehicle_alerts',       INTERVAL '1 year');
SELECT add_retention_policy('vehicle_connectivity', INTERVAL '1 year');
SELECT add_retention_policy('vehicle_errors',       INTERVAL '1 year');
```

> **Tip:** If you need long-term analytics, set up continuous aggregates *before*
> retention drops the raw data. The aggregates survive independently.

---

## Continuous Aggregates

Pre-computed materialized views that TimescaleDB keeps up-to-date automatically.
Essential for dashboards and reporting queries.

### Hourly Signal Summary

```sql
CREATE MATERIALIZED VIEW vehicle_signals_hourly
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 hour', time)  AS bucket,
    vin,
    signal_name,
    avg(value_numeric)           AS avg_value,
    min(value_numeric)           AS min_value,
    max(value_numeric)           AS max_value,
    count(*)                     AS sample_count
FROM vehicle_signals
WHERE value_numeric IS NOT NULL
GROUP BY bucket, vin, signal_name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('vehicle_signals_hourly',
    start_offset    => INTERVAL '3 hours',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);
```

### Daily Alert Summary

```sql
CREATE MATERIALIZED VIEW alerts_daily
WITH (timescaledb.continuous) AS
SELECT
    time_bucket('1 day', time)   AS bucket,
    vin,
    alert_name,
    count(*)                     AS alert_count
FROM vehicle_alerts
GROUP BY bucket, vin, alert_name
WITH NO DATA;

SELECT add_continuous_aggregate_policy('alerts_daily',
    start_offset    => INTERVAL '2 days',
    end_offset      => INTERVAL '1 hour',
    schedule_interval => INTERVAL '1 hour'
);
```

### Latest Vehicle State (Pivot View)

If you need a "wide" view showing the latest value for every signal:

```sql
CREATE VIEW vehicle_latest_state AS
SELECT DISTINCT ON (vin, signal_name)
    vin,
    signal_name,
    time,
    value_type,
    value_string,
    value_numeric,
    value_boolean,
    value_json
FROM vehicle_signals
ORDER BY vin, signal_name, time DESC;
```

---

## Ingestion Strategy

### Transforming `V.jsonl`

Each JSONL line produces **multiple rows** — one per signal field in `data`.
Skip the envelope fields (`Vin`, `CreatedAt`, `IsResend`) as signal rows;
they become metadata columns on every row.

**Input (1 JSONL line):**
```json
{
  "vin": "7SAYGDEE2PF875122",
  "time": "2026-01-30T21:26:53Z",
  "txid": "252624a96e834cc69602ac-000000001",
  "data": {
    "Vin": "7SAYGDEE2PF875122",
    "CreatedAt": "2026-01-30T21:26:40Z",
    "IsResend": false,
    "ChargeState": {"stringValue": "Idle"},
    "VehicleSpeed": {"doubleValue": 0.0}
  }
}
```

**Output (2 rows):**

| time | created_at | vin | txid | signal_name | value_type | value_string | value_numeric | is_resend |
|------|-----------|-----|------|-------------|-----------|-------------|--------------|-----------|
| 2026-01-30T21:26:53Z | 2026-01-30T21:26:40Z | 7SAY... | 252624... | ChargeState | string | Idle | NULL | false |
| 2026-01-30T21:26:53Z | 2026-01-30T21:26:40Z | 7SAY... | 252624... | VehicleSpeed | double | NULL | 0.0 | false |

**Pseudocode:**
```python
for line in open("V.jsonl"):
    record = json.loads(line)
    envelope_time = record["time"]
    vin = record["vin"]
    txid = record["txid"]
    data = record["data"]
    created_at = data.pop("CreatedAt")
    is_resend = data.pop("IsResend", False)
    data.pop("Vin", None)

    for signal_name, value_wrapper in data.items():
        value_type, value = next(iter(value_wrapper.items()))
        row = {
            "time": envelope_time,
            "created_at": created_at,
            "vin": vin,
            "txid": txid,
            "signal_name": signal_name,
            "value_type": value_type.replace("Value", ""),
            "value_string": value if isinstance(value, str) else None,
            "value_numeric": value if isinstance(value, (int, float)) and not isinstance(value, bool) else None,
            "value_boolean": value if isinstance(value, bool) else None,
            "value_json": json.dumps(value) if isinstance(value, dict) else None,
            "is_resend": is_resend,
        }
        # INSERT or batch for COPY
```

### Transforming `alerts.jsonl`

Unnest the `data[]` array — one row per alert per JSONL line.

```python
for line in open("alerts.jsonl"):
    record = json.loads(line)
    for alert in record["data"]:
        row = {
            "time": record["time"],
            "vin": record["vin"],
            "txid": record["txid"],
            "alert_name": alert["Name"],
            "audiences": alert.get("Audiences"),          # → TEXT[]
            "started_at": to_timestamp(alert.get("StartedAt")),
            "ended_at": to_timestamp(alert.get("EndedAt")),
        }
```

### Transforming `connectivity.jsonl`

Direct 1:1 mapping — each JSONL line becomes one row.

```python
for line in open("connectivity.jsonl"):
    record = json.loads(line)
    data = record["data"]
    row = {
        "time": record["time"],
        "vin": record["vin"],
        "txid": record["txid"],
        "connection_id": data["ConnectionID"],
        "status": data["Status"],
        "network_interface": data["NetworkInterface"],
        "created_at": to_timestamp(data["CreatedAt"]),
    }
```

### Transforming `errors.jsonl`

Unnest the `data[]` array (may be empty).

```python
for line in open("errors.jsonl"):
    record = json.loads(line)
    for error in record["data"]:
        row = {
            "time": record["time"],
            "vin": record["vin"],
            "txid": record["txid"],
            "error_name": error["Name"],
            "body": error.get("Body"),
            "tags": json.dumps(error.get("Tags")) if error.get("Tags") else None,
            "created_at": to_timestamp(error.get("CreatedAt")),
        }
```

### Batch Loading with COPY

For bulk historical loading, use PostgreSQL `COPY` for maximum throughput:

```bash
# Convert JSONL to CSV, then load
python3 transform_v_jsonl.py V.jsonl | \
  psql -c "COPY vehicle_signals FROM STDIN WITH CSV HEADER" \
       -d fleet_telemetry
```

For real-time ingestion, use batched `INSERT` with `unnest()`:

```sql
INSERT INTO vehicle_signals (time, created_at, vin, txid, signal_name,
                             value_type, value_string, value_numeric,
                             value_boolean, value_json, is_resend)
SELECT * FROM unnest(
    $1::timestamptz[],  -- time
    $2::timestamptz[],  -- created_at
    $3::text[],         -- vin
    $4::text[],         -- txid
    $5::text[],         -- signal_name
    $6::text[],         -- value_type
    $7::text[],         -- value_string
    $8::float8[],       -- value_numeric
    $9::boolean[],      -- value_boolean
    $10::jsonb[],       -- value_json
    $11::boolean[]      -- is_resend
);
```

---

## Example Queries

### Get the latest state of a vehicle

```sql
SELECT DISTINCT ON (signal_name)
    signal_name,
    value_type,
    COALESCE(value_string, value_numeric::text, value_boolean::text) AS value,
    time
FROM vehicle_signals
WHERE vin = '7SAYGDEE2PF875122'
ORDER BY signal_name, time DESC;
```

### ChargeState history for a vehicle

```sql
SELECT time, value_string AS charge_state
FROM vehicle_signals
WHERE vin = '7SAYGDEE2PF875122'
  AND signal_name = 'ChargeState'
  AND time >= NOW() - INTERVAL '7 days'
ORDER BY time DESC;
```

### Average speed over time (hourly buckets)

```sql
SELECT
    time_bucket('1 hour', time) AS hour,
    avg(value_numeric)          AS avg_speed,
    max(value_numeric)          AS max_speed
FROM vehicle_signals
WHERE vin = '7SAYGDEE2PF875122'
  AND signal_name = 'VehicleSpeed'
  AND time >= NOW() - INTERVAL '24 hours'
GROUP BY hour
ORDER BY hour;
```

### Active alerts for a vehicle

```sql
SELECT alert_name, audiences, started_at
FROM vehicle_alerts
WHERE vin = '7SAYGDEE2PF875122'
  AND ended_at IS NULL
ORDER BY started_at DESC;
```

### Connection sessions with duration

```sql
SELECT
    c1.connection_id,
    c1.network_interface,
    c1.created_at AS connected_at,
    c2.created_at AS disconnected_at,
    c2.created_at - c1.created_at AS duration
FROM vehicle_connectivity c1
LEFT JOIN vehicle_connectivity c2
    ON c1.connection_id = c2.connection_id
    AND c2.status = 'DISCONNECTED'
WHERE c1.vin = '7SAYGDEE2PF875122'
  AND c1.status = 'CONNECTED'
ORDER BY c1.created_at DESC;
```

### Alert frequency report

```sql
SELECT
    alert_name,
    count(*) AS occurrences,
    min(started_at) AS first_seen,
    max(started_at) AS last_seen
FROM vehicle_alerts
WHERE vin = '7SAYGDEE2PF875122'
  AND time >= NOW() - INTERVAL '30 days'
GROUP BY alert_name
ORDER BY occurrences DESC;
```

### Battery level trend (using continuous aggregate)

```sql
SELECT bucket, avg_value AS avg_battery, min_value, max_value
FROM vehicle_signals_hourly
WHERE vin = '7SAYGDEE2PF875122'
  AND signal_name = 'BatteryLevel'
  AND bucket >= NOW() - INTERVAL '7 days'
ORDER BY bucket;
```

---

## Docker Setup

Add TimescaleDB to your existing `docker-compose.yml`:

```yaml
services:
  timescaledb:
    image: timescale/timescaledb:latest-pg16
    container_name: fleet-timescaledb
    restart: unless-stopped
    ports:
      - "5432:5432"
    environment:
      POSTGRES_DB: fleet_telemetry
      POSTGRES_USER: fleet
      POSTGRES_PASSWORD: ${TIMESCALE_PASSWORD:-fleet_telemetry_dev}
    volumes:
      - timescaledb_data:/var/lib/postgresql/data
      - ./doc/schema.sql:/docker-entrypoint-initdb.d/01-schema.sql:ro
    # Performance tuning for telemetry workloads
    command:
      - postgres
      - -c
      - shared_buffers=256MB
      - -c
      - work_mem=16MB
      - -c
      - maintenance_work_mem=256MB
      - -c
      - max_wal_size=2GB
      - -c
      - checkpoint_completion_target=0.9
      - -c
      - effective_cache_size=1GB

volumes:
  timescaledb_data:
```

### Connection string

```
postgresql://fleet:fleet_telemetry_dev@localhost:5432/fleet_telemetry
```

---

## Storage Estimates

Based on observed data rates (~2.3 records/sec per vehicle, ~3 signals/record):

### Per Vehicle

| Period | Signal rows | Uncompressed | Compressed (~10:1) |
|--------|------------|-------------|-------------------|
| 1 hour | ~24,840 | ~5 MB | ~500 KB |
| 1 day | ~596,160 | ~120 MB | ~12 MB |
| 1 month | ~18M | ~3.6 GB | ~360 MB |
| 1 year | ~216M | ~43 GB | ~4.3 GB |

### Fleet Scaling

| Vehicles | 1-year compressed | 1-year uncompressed |
|----------|------------------|-------------------|
| 1 | 4.3 GB | 43 GB |
| 10 | 43 GB | 430 GB |
| 100 | 430 GB | 4.3 TB |
| 1,000 | 4.3 TB | 43 TB |

> **Recommendation:** For 100+ vehicles, consider:
> - Adding `vin` as a **space partition** dimension on the hypertable
> - Using TimescaleDB's **tiered storage** to move old compressed data to S3
> - Adjusting chunk intervals (smaller for high-volume tables)

### Checking Compression Stats

```sql
-- Overall compression ratio
SELECT
    hypertable_name,
    pg_size_pretty(before_compression_total_bytes) AS before,
    pg_size_pretty(after_compression_total_bytes) AS after,
    round(
        (1 - after_compression_total_bytes::numeric
           / before_compression_total_bytes::numeric) * 100, 1
    ) AS compression_pct
FROM hypertable_compression_stats('vehicle_signals');
```

---

## Performance Tips

1. **Batch inserts**: Never insert one row at a time. Use `COPY` or batch
   `INSERT` with `unnest()` arrays. Aim for 1,000–10,000 rows per batch.

2. **Avoid frequent decompression**: Don't update or delete rows in compressed
   chunks. If you need to backfill, decompress → insert → recompress.

3. **Index sparingly**: The `(vin, signal_name, time DESC)` index covers most
   query patterns. Add more indexes only after profiling actual queries.

4. **Use continuous aggregates for dashboards**: Don't run expensive `GROUP BY`
   queries on raw data for recurring dashboard panels.

5. **Monitor chunk sizes**: Run `SELECT * FROM timescaledb_information.chunks`
   to verify chunk counts and sizes are reasonable.

6. **Connection pooling**: Use PgBouncer for high-throughput ingestion to avoid
   exhausting PostgreSQL connection slots.
