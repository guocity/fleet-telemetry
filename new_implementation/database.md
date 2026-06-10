# Fleet Telemetry — Compact TimescaleDB Storage

Design and operations guide for storing Tesla fleet-telemetry data in
TimescaleDB **without storing repetitive values**. Everything here was
validated against the real data in `telemetry-data/` (2 vehicles,
Jan–Jun 2026, 45.4 MB of JSONL) on the actual server
(`192.168.18.2:5432`, PostgreSQL 16.14 + TimescaleDB 2.27.1, database
`fleet_telemetry`).

> The JSONL **FileWriter was a temporary workaround** while no database
> existed. The server now writes directly to TimescaleDB via the
> `datastore/timescale` Go producer (`"records": {"V": ["timescale"], ...}`,
> see `timescale-server-config.json`). The JSONL files are only needed once
> more, for historical backfill via `scripts/ingest_jsonl.py`.

---

## Measured redundancy (why this design)

`scripts/inspect_telemetry.py` over the real data:

| Finding | Number |
|---|---|
| JSONL bytes that are duplicate envelope blobs (`decoded_payload` + `payload` + `metadata`) | **71.8%** of all bytes |
| Signal observations that repeat the previous value unchanged | **71.8%** (77,857 of 108,449) |
| Alert entries that are re-sends of an already-known episode | **98.3%** (54,034 entries → 893 distinct, 60.5× duplication) |
| Connectivity / error duplication | none |

So the two big levers are: **(1) don't store the envelope blobs at all,
(2) store a signal row only when the value changes** ("change-only"
storage). Alerts collapse to one row per episode.

### Validated result

| | Raw JSONL | Database |
|---|---|---|
| 2 vehicles × ~5 months | 45.4 MB | **~4.2 MB** of table+index data |
| Signal rows | 108,449 observations | 30,573 change rows (2,712 kB incl. index, compressed) |
| Alert rows | 54,034 entries | 640 episodes (128 kB) |

≈ **11× smaller than the JSONL**, and ≈ **40× smaller** than the naive
"one row per observation + every alert entry" SQL design. At the
observed telemetry rate this is **~3.3 MB per vehicle-year**, i.e.
~1 GB/year for 300 vehicles — hundreds of cars fit comfortably; you
will not be wasting hundreds of gigabytes on repeats.

---

## Architecture

```
┌──────────┐    ┌─────────────────────┐
│ Vehicle  │───▶│ Fleet Telemetry     │   datastore/timescale producer
│ (TLS)    │    │ Server ("timescale" │──────────────▶  TimescaleDB  fleet_telemetry
└──────────┘    │  record dispatcher) │              ├ signal_changes   (hypertable, change-only)
                └─────────────────────┘              ├ signal_latest    (current state, O(1))
                                                     ├ alert_episodes   (deduplicated lifecycle)
   old JSONL files ──▶ scripts/ingest_jsonl.py ────▶ ├ connectivity_events
   (one-time historical backfill)                    ├ error_events
                                                     └ vehicles / signals / alert_types (dims)
```

The Go producer ([datastore/timescale/producer.go](../datastore/timescale/producer.go))
does the same change-only dedup as the backfill script: it loads
`signal_latest` into memory at startup, drops unchanged observations,
batches inserts (`COPY`, default 500 rows / 2 s flush), and upserts
alert episodes and natural-key events. Configure it with:

```json
"timescale": {
  "dsn": "postgresql://postgres:postgres@192.168.18.2:5432/fleet_telemetry",
  "batch_size": 500,
  "flush_interval_ms": 2000
},
"records": { "V": ["timescale"], "alerts": ["timescale"],
             "errors": ["timescale"], "connectivity": ["timescale"] }
```

---

## Schema (`schema.sql`)

### Principles

1. **Change-only fact table.** `signal_changes` gets a row only when a
   signal's value differs from its previous value for that vehicle.
   Repeats are dropped at ingest (measured: −71.8% of rows). This also
   means the table *is* the delta encoding — the filewriter's
   `enable_delta` becomes unnecessary.
2. **No envelope blobs.** `payload`, `decoded_payload`, `metadata` are
   never stored (−72% of bytes). The only useful scrap of metadata,
   `device_client_version`, lives on the `vehicles` row.
3. **Integer surrogate keys.** `vehicle_id smallint` + `signal_id
   smallint` (4 bytes total) instead of repeating a 17-char VIN and a
   ~15-char signal name per row. `signal_id` equals the proto `Field`
   enum number (1–259, seeded from `protos/vehicle_data.proto`), so ids
   are stable and meaningful; unknown future signals auto-register at
   ids ≥ 1000.
4. **No per-row type tag.** One nullable column per storage class
   (`v_num float8`, `v_long bigint`, `v_bool`, `v_text`, `v_loc point`,
   `v_json jsonb`, `invalid bool`); a NULL costs one bit. `v_long`
   keeps int64 exact (no float8 precision loss). Each signal always
   uses the same column, recorded informationally in `signals`.
5. **Current state is a table, not a scan.** `signal_latest` (one row
   per vehicle × signal, PK-indexed) is upserted at ingest. "What's the
   car's state now" never touches the hypertable — important at fleet
   scale where a `DISTINCT ON` over all history would scan every chunk.
6. **Alerts are episodes.** PK `(vehicle_id, alert_id, started_at)`;
   the closing re-send fills `ended_at` via upsert. Open alerts =
   `ended_at IS NULL` (partial index).
7. **Idempotency without bookkeeping.** Connectivity and errors insert
   on natural keys (`ON CONFLICT DO NOTHING`), alerts upsert per
   episode, and a signal observation is stored only when it is strictly
   newer than the stored latest *and* its value changed — so replaying
   any JSONL file (or a vehicle retransmission) writes nothing.

### Hypertable & compression

```sql
SELECT create_hypertable('signal_changes', 'ts', chunk_time_interval => INTERVAL '7 days');
ALTER TABLE signal_changes SET (
    timescaledb.compress,
    timescaledb.compress_segmentby = 'vehicle_id, signal_id',
    timescaledb.compress_orderby   = 'ts DESC');
SELECT add_compression_policy('signal_changes', INTERVAL '14 days');
```

* Partition column is `ts` = the **vehicle-side** `CreatedAt` (what you
  filter on analytically), not the server write time.
* Segment-by `(vehicle_id, signal_id)` stores each series contiguously;
  measured compression on the real data: 4,680 kB → 1,864 kB (2.5×) *on
  top of* the 3.5× from change-only dedup.
* **Chunk sizing rule:** aim for chunks of 5–25M rows that fit in RAM.
  At the observed rate (~100 change-rows/vehicle/day), 7-day chunks are
  fine up to several thousand vehicles. If you raise the telemetry
  config to second-level streaming, recompute: `rows/day = vehicles ×
  signals-changing/sec × 86,400 × 0.28` and shrink
  `chunk_time_interval` to keep chunks in that envelope.

### Only `signal_changes` is a hypertable

Alerts (640 rows), connectivity (5.7k rows) and errors (361 rows) are
plain tables: their volume is negligible, and plain tables allow the
natural-key PKs that give us dedup/upserts (hypertables require the
time column in every unique constraint). Even at 300 vehicles for 5
years, connectivity is only ~25M small rows — revisit then; migrating
is one `create_hypertable()` call.

---

## Setup & operation

```bash
cd new_implementation
python3 -m venv .venv && .venv/bin/pip install -r requirements.txt

# 1. create DB, apply schema.sql, seed 259 signals from the proto
.venv/bin/python scripts/setup_db.py            # --drop to recreate

# 2. one-time backfill of the historical JSONL files
.venv/bin/python scripts/ingest_jsonl.py

# 3. run the server with the timescale producer for live data
#    (see timescale-server-config.json) — no more JSONL files
```

Re-running the backfill at any time is safe (verified: a full replay
writes 0 duplicate rows and picks up only lines appended since). The
ingester accepts every record shape the server ever produced:
typed wrappers (`{"doubleValue": 1.5}`) and bare values, full records,
`snapshot`/`delta` records (a `null` value = signal dropped from the
delta baseline → ignored), and old-format files (`V_old.jsonl`).

### Retention (optional)

Change-only rows are so small that you may simply keep everything. If
you do want a cap:

```sql
SELECT add_retention_policy('signal_changes', INTERVAL '3 years');
```

`signal_latest`, `alert_episodes`, etc. are unaffected — current state
and alert history survive raw-data retention.

---

## Querying change-only data

The one conceptual shift: **absence of rows means "value unchanged",
not "no data".** For point-in-time and charting queries use
TimescaleDB's `locf()` (last observation carried forward).

Current state of a car — instant, no hypertable scan:

```sql
SELECT signal, ts, value FROM v_current_state WHERE vin = '5YJ3E1EA2JF013888';
```

History of one signal (only the changes, which is usually what you want):

```sql
SELECT ts, v_text FROM v_signal_history
WHERE vin = '5YJ3E1EA2JF013888' AND signal = 'ChargeState'
ORDER BY ts DESC LIMIT 100;
```

Battery level chart, gap-filled hourly (validated on real data):

```sql
SELECT time_bucket_gapfill('1 hour', ts) AS hour,
       locf(avg(v_num)) AS battery_pct
FROM signal_changes c
JOIN vehicles v USING (vehicle_id)
JOIN signals  s USING (signal_id)
WHERE v.vin = '5YJ3E1EA2JF013888' AND s.name = 'BatteryLevel'
  AND ts BETWEEN now() - INTERVAL '7 days' AND now()
GROUP BY hour ORDER BY hour;
```

Value of a signal at an arbitrary past instant:

```sql
SELECT v_num FROM signal_changes
WHERE vehicle_id = 1 AND signal_id = 42      -- BatteryLevel
  AND ts <= '2026-04-01 12:00+00'
ORDER BY ts DESC LIMIT 1;
```

Open alerts / alert frequency / connection sessions:

```sql
SELECT * FROM v_open_alerts;

SELECT t.name, count(*), min(started_at), max(started_at)
FROM alert_episodes a JOIN alert_types t USING (alert_id)
WHERE a.vehicle_id = 1 AND started_at >= now() - INTERVAL '30 days'
GROUP BY t.name ORDER BY count(*) DESC;

SELECT * FROM v_connection_sessions WHERE vin = '5YJ3E1EA2JF013888'
ORDER BY connected_at DESC LIMIT 20;
```

> **Continuous-aggregate caveat:** don't build naive `avg(value)`
> rollups over `signal_changes` — with repeats removed, a plain average
> over-weights volatile periods. Use `locf()`/`time_weight()` (the
> `time_weight('locf', ...)` aggregate from the timescaledb_toolkit) if
> you need statistically correct rollups.

---

## Scaling to hundreds of cars

| Lever | Setting |
|---|---|
| Rows | change-only ingest already drops ~72%; the duplication factor *grows* with reporting frequency, so faster telemetry configs gain even more |
| Chunk interval | 7 days now; shrink if you raise telemetry frequency (rule above) |
| Compression | policy compresses chunks > 14 days old automatically |
| Current-state queries | `signal_latest` keeps them O(vehicles × signals), independent of history size |
| Ingest throughput | the Go producer batches `COPY` inserts (500 rows / 2 s flush) with in-memory change detection — no file I/O at all |
| Postgres tuning | the container runs 128MB `shared_buffers`; for a 300-car fleet give it 2–4 GB RAM, `shared_buffers` ≈ 25% of that |

Estimated storage at observed per-car rates: **~3.3 MB/vehicle-year**
(compressed, incl. indexes). 300 cars ≈ 1 GB/year. If your telemetry
config streams 100× more frequently, plan ~100 GB/year — still far from
"hundreds of gigabytes of repetitive values", because repeats never
reach disk.

---

## Files in this folder

| File | Purpose |
|---|---|
| `schema.sql` | Complete DDL: tables, hypertable, compression, views |
| `timescale-server-config.json` | Example server config using the `timescale` producer |
| `../datastore/timescale/producer.go` | Go producer: server → TimescaleDB directly (lives in the Go module) |
| `scripts/setup_db.py` | Create DB + apply schema + seed signal dictionary from the proto |
| `scripts/ingest_jsonl.py` | One-time historical backfill of the old JSONL files |
| `scripts/inspect_telemetry.py` | Redundancy/shape analyzer for the JSONL data |
| `scripts/generate_openapi.py` | Regenerates `fleet-telemetry-output.openapi.yaml` from protos + transformer source |
| `fleet-telemetry-output.openapi.yaml` | **Generated** JSONL output schema — do not edit by hand |

When the protos or transformers change (new firmware signals, new value
types), re-run:

```bash
.venv/bin/python scripts/generate_openapi.py   # refresh the spec
.venv/bin/python scripts/setup_db.py           # upsert new signal names/ids
```

---

## Switching the server to direct database writes

`datastore/timescale/producer.go` implements `telemetry.Producer`:

1. Build the server (the standard `Dockerfile` / `make` — pgx is a pure
   Go dependency, nothing extra to install).
2. Point the config at the database and route all record types to
   `timescale` (full example: `timescale-server-config.json`).
3. Remove the `filewriter` entries from `records` — JSONL output stops.

On startup the producer loads `signal_latest` once (state survives
restarts), then: V payloads go through the same typed-value
classification as the transformers, unchanged values are dropped in
memory, changes are buffered and flushed with `COPY` (batch_size /
flush_interval_ms), and `signal_latest` is upserted in the same flush.
Alerts, errors and connectivity use the upsert/natural-key paths, so
vehicle retransmissions remain harmless. If both the old filewriter and
the new producer ran in parallel for a while, the backfill script can
replay the overlap — idempotency guarantees no duplicates.
