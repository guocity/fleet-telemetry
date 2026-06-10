#!/usr/bin/env python3
"""Idempotent JSONL -> TimescaleDB ingester for fleet-telemetry output.

Reads telemetry-data/{VIN}/{YYYY}/{MM}/{txtype}.jsonl and loads it into the
compact schema (see ../schema.sql):

  * V*.jsonl       -> signal_changes (change-only) + signal_latest (upsert)
  * alerts.jsonl   -> alert_episodes (upsert; collapses ~60x re-sent history)
  * connectivity   -> connectivity_events (natural-key, ON CONFLICT DO NOTHING)
  * errors.jsonl   -> error_events (natural-key, ON CONFLICT DO NOTHING)

Redundant envelope blobs (payload, decoded_payload, metadata) are not stored;
only metadata.device_client_version is kept, on the vehicles row.

Handles every observed/possible record shape:
  * typed signal wrappers ({"doubleValue": 1.5}) and bare values (verbose=false)
  * full records and delta records ("type": snapshot|delta, missing envelope
    fields, null = signal dropped from the delta baseline -> ignored)
  * old-format files (V_old.jsonl) with extra envelope keys

Idempotency (no bookkeeping tables — the JSONL files are a temporary bridge):
  * signal data: an observation is stored only if it is strictly newer than
    the stored latest timestamp for that (vehicle, signal) AND its value
    changed — replaying any already-ingested file is a no-op
  * alerts/connectivity/errors: natural-key upserts (ON CONFLICT)

This is a one-shot backfill tool; live data is written directly to the
database by the Go producer (datastore/timescale). Re-running is always safe.

Usage:
    python3 ingest_jsonl.py [--data-dir ../../telemetry-data]
                            [--dsn postgresql://postgres:postgres@192.168.18.2:5432/fleet_telemetry]
                            [--dry-run]
"""

import argparse
import json
import os
import sys
from datetime import datetime, timezone

import psycopg

HERE = os.path.dirname(os.path.abspath(__file__))
DEFAULT_DATA = os.path.abspath(os.path.join(HERE, "..", "..", "telemetry-data"))
DEFAULT_DSN = "postgresql://postgres:postgres@192.168.18.2:5432/fleet_telemetry"

ENVELOPE_DATA_KEYS = {"Vin", "CreatedAt", "IsResend"}
NUM_KEYS = {"floatValue", "doubleValue"}
LONG_KEYS = {"intValue", "longValue"}

BATCH = 5000


def ts_utc(unix_seconds):
    return datetime.fromtimestamp(unix_seconds, tz=timezone.utc)


def parse_rfc3339(s):
    return datetime.fromisoformat(s.replace("Z", "+00:00"))


def fmt_loc(lon, lat):
    """Canonical text form for a point; must round-trip PG's point output."""
    return f"({float(lon)!r},{float(lat)!r})"


def norm_loc_text(s):
    if s is None:
        return None
    x, y = s.strip("()").split(",")
    return fmt_loc(x, y)


def norm_json_text(s):
    if s is None:
        return None
    return json.dumps(json.loads(s), sort_keys=True)


def classify(wrapper):
    """Map a JSONL signal value to schema columns.

    Returns (v_num, v_long, v_bool, v_text, v_loc, v_json, invalid)
    or None when the value carries no information (delta removal).
    """
    n = l = b = t = loc = j = None
    inv = False

    if wrapper is None:
        return None  # delta encoding: signal left the payload — not a value

    if isinstance(wrapper, dict) and len(wrapper) == 1:
        key, val = next(iter(wrapper.items()))
        if key == "invalid":
            inv = True
        elif key in NUM_KEYS:
            n = float(val)
        elif key in LONG_KEYS:
            l = int(val)
        elif key == "booleanValue":
            b = bool(val)
        elif key == "locationValue":
            loc = fmt_loc(val["longitude"], val["latitude"])
        elif isinstance(val, dict):       # doorValue, tireLocation
            j = json.dumps(val, sort_keys=True)
        elif isinstance(val, bool):
            b = val
        elif isinstance(val, (int, float)):
            n = float(val)
        else:                              # stringValue, enums, time
            t = str(val)
    else:
        # verbose=false: bare values
        if isinstance(wrapper, bool):
            b = wrapper
        elif isinstance(wrapper, (int, float)):
            n = float(wrapper)
        elif isinstance(wrapper, str):
            if wrapper == "<invalid>":
                inv = True
            else:
                t = wrapper
        elif isinstance(wrapper, dict):
            if "latitude" in wrapper and "longitude" in wrapper:
                loc = fmt_loc(wrapper["longitude"], wrapper["latitude"])
            else:
                j = json.dumps(wrapper, sort_keys=True)
        else:
            j = json.dumps(wrapper, sort_keys=True)

    return (n, l, b, t, loc, j, inv)


class Ingester:
    def __init__(self, conn, dry_run=False):
        self.conn = conn
        self.dry = dry_run
        self.vehicle_ids = {}     # vin -> vehicle_id
        self.signal_ids = {}      # name -> signal_id
        self.alert_ids = {}       # name -> alert_id
        self.latest = {}          # (vehicle_id, signal_id) -> value tuple
        self.latest_ts = {}       # (vehicle_id, signal_id) -> ts
        self.stats = {"signal_obs": 0, "signal_rows": 0, "alerts_in": 0,
                      "alert_upserts": 0, "conn_rows": 0, "err_rows": 0,
                      "lines": 0, "skipped_dup": 0}
        self._load_dims()

    def _load_dims(self):
        cur = self.conn.cursor()
        self.signal_ids = dict(cur.execute("SELECT name, signal_id FROM signals").fetchall())
        self.vehicle_ids = dict(cur.execute("SELECT vin, vehicle_id FROM vehicles").fetchall())
        self.alert_ids = dict(cur.execute("SELECT name, alert_id FROM alert_types").fetchall())
        for row in cur.execute(
            "SELECT vehicle_id, signal_id, ts, v_num, v_long, v_bool, v_text, "
            "v_loc::text, v_json::text, invalid FROM signal_latest"
        ):
            key = (row[0], row[1])
            self.latest_ts[key] = row[2]
            # normalize text forms so they compare equal to classify() output
            self.latest[key] = (row[3], row[4], row[5], row[6],
                                norm_loc_text(row[7]), norm_json_text(row[8]), row[9])

    # ── dimension helpers ─────────────────────────────────────────
    def vehicle_id(self, vin):
        vid = self.vehicle_ids.get(vin)
        if vid is None:
            vid = self.conn.execute(
                "INSERT INTO vehicles (vin) VALUES (%s) "
                "ON CONFLICT (vin) DO UPDATE SET vin = EXCLUDED.vin RETURNING vehicle_id",
                (vin,),
            ).fetchone()[0]
            self.vehicle_ids[vin] = vid
        return vid

    def signal_id(self, name):
        sid = self.signal_ids.get(name)
        if sid is None:
            # unknown (future-firmware) signal: register with id >= 1000
            sid = self.conn.execute(
                "INSERT INTO signals (signal_id, name) "
                "SELECT GREATEST(1000, max(signal_id) + 1), %s FROM signals "
                "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING signal_id",
                (name,),
            ).fetchone()[0]
            self.signal_ids[name] = sid
        return sid

    def alert_id(self, name):
        aid = self.alert_ids.get(name)
        if aid is None:
            aid = self.conn.execute(
                "INSERT INTO alert_types (name) VALUES (%s) "
                "ON CONFLICT (name) DO UPDATE SET name = EXCLUDED.name RETURNING alert_id",
                (name,),
            ).fetchone()[0]
            self.alert_ids[name] = aid
        return aid

    def note_client_version(self, vin, meta):
        ver = (meta or {}).get("device_client_version")
        if ver:
            self.conn.execute(
                "UPDATE vehicles SET device_client_version = %s "
                "WHERE vin = %s AND device_client_version IS DISTINCT FROM %s",
                (ver, vin, ver),
            )

    # ── record handlers ───────────────────────────────────────────
    def handle_v(self, rec, vin_dir):
        data = rec.get("data")
        if not isinstance(data, dict):
            return
        vin = rec.get("vin") or data.get("Vin") or vin_dir
        vid = self.vehicle_id(vin)
        self.note_client_version(vin, rec.get("metadata"))

        created = data.get("CreatedAt")
        if created:
            ts = parse_rfc3339(created)
        else:  # delta record without CreatedAt — fall back to envelope time
            ts = parse_rfc3339(rec["time"])

        rows = []
        for name, wrapper in data.items():
            if name in ENVELOPE_DATA_KEYS:
                continue
            self.stats["signal_obs"] += 1
            vals = classify(wrapper)
            if vals is None:
                continue
            sid = self.signal_id(name)
            key = (vid, sid)
            prev = self.latest.get(key)
            prev_ts = self.latest_ts.get(key)

            if prev == vals:
                self.stats["skipped_dup"] += 1
                if prev_ts is None or ts > prev_ts:
                    self.latest_ts[key] = ts
                continue

            # value differs — but only a strictly newer observation counts as
            # a change; older/equal timestamps are replays or resends
            if prev_ts is not None and ts <= prev_ts:
                self.stats["skipped_dup"] += 1
                continue

            rows.append((ts, vid, sid) + vals)
            self.latest[key] = vals
            self.latest_ts[key] = ts
            self._upsert_latest(ts, vid, sid, vals)

        if rows:
            self.conn.cursor().executemany(
                "INSERT INTO signal_changes "
                "(ts, vehicle_id, signal_id, v_num, v_long, v_bool, v_text, v_loc, v_json, invalid) "
                "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)",
                rows,
            )
            self.stats["signal_rows"] += len(rows)

    def _upsert_latest(self, ts, vid, sid, vals):
        self.conn.execute(
            "INSERT INTO signal_latest "
            "(vehicle_id, signal_id, ts, v_num, v_long, v_bool, v_text, v_loc, v_json, invalid) "
            "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s) "
            "ON CONFLICT (vehicle_id, signal_id) DO UPDATE SET "
            "ts=EXCLUDED.ts, v_num=EXCLUDED.v_num, v_long=EXCLUDED.v_long, "
            "v_bool=EXCLUDED.v_bool, v_text=EXCLUDED.v_text, v_loc=EXCLUDED.v_loc, "
            "v_json=EXCLUDED.v_json, invalid=EXCLUDED.invalid "
            "WHERE EXCLUDED.ts >= signal_latest.ts",
            (vid, sid, ts) + vals,
        )

    def handle_alerts(self, rec, vin_dir):
        data = rec.get("data")
        if not isinstance(data, list):
            return
        vin = rec.get("vin") or vin_dir
        vid = self.vehicle_id(vin)
        self.note_client_version(vin, rec.get("metadata"))
        reported = parse_rfc3339(rec["time"]) if rec.get("time") else None

        # collapse duplicates within the message before upserting
        episodes = {}
        for a in data:
            self.stats["alerts_in"] += 1
            name, started = a.get("Name"), a.get("StartedAt")
            if name is None or started is None:
                continue
            key = (self.alert_id(name), ts_utc(started))
            ended = ts_utc(a["EndedAt"]) if a.get("EndedAt") is not None else None
            aud = sorted(a["Audiences"]) if a.get("Audiences") else None
            cur = episodes.get(key)
            if cur is None or (cur[0] is None and ended is not None):
                episodes[key] = (ended, aud)

        rows = [(vid, aid, started, ended, aud, reported)
                for (aid, started), (ended, aud) in episodes.items()]
        if rows:
            self.conn.cursor().executemany(
                "INSERT INTO alert_episodes "
                "(vehicle_id, alert_id, started_at, ended_at, audiences, last_reported) "
                "VALUES (%s,%s,%s,%s,%s,%s) "
                "ON CONFLICT (vehicle_id, alert_id, started_at) DO UPDATE SET "
                "ended_at      = COALESCE(alert_episodes.ended_at, EXCLUDED.ended_at), "
                "audiences     = COALESCE(EXCLUDED.audiences, alert_episodes.audiences), "
                "last_reported = GREATEST(alert_episodes.last_reported, EXCLUDED.last_reported)",
                rows,
            )
            self.stats["alert_upserts"] += len(rows)

    def handle_connectivity(self, rec, vin_dir):
        data = rec.get("data")
        if not isinstance(data, dict) or "ConnectionID" not in data:
            return
        vin = rec.get("vin") or data.get("Vin") or vin_dir
        vid = self.vehicle_id(vin)
        self.note_client_version(vin, rec.get("metadata"))
        created = data.get("CreatedAt")
        ts = ts_utc(created) if isinstance(created, (int, float)) else parse_rfc3339(rec["time"])
        self.conn.execute(
            "INSERT INTO connectivity_events "
            "(vehicle_id, connection_id, status, network_interface, ts) "
            "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
            (vid, data["ConnectionID"], data.get("Status", "UNKNOWN"),
             data.get("NetworkInterface") or None, ts),
        )
        self.stats["conn_rows"] += 1

    def handle_errors(self, rec, vin_dir):
        data = rec.get("data")
        if not isinstance(data, list):
            return
        vin = rec.get("vin") or vin_dir
        vid = self.vehicle_id(vin)
        self.note_client_version(vin, rec.get("metadata"))
        for e in data:
            created = e.get("CreatedAt")
            ts = ts_utc(created) if isinstance(created, (int, float)) else parse_rfc3339(rec["time"])
            self.conn.execute(
                "INSERT INTO error_events (vehicle_id, ts, name, body, tags) "
                "VALUES (%s,%s,%s,%s,%s) ON CONFLICT DO NOTHING",
                (vid, ts, e.get("Name", ""), e.get("Body") or None,
                 json.dumps(e["Tags"], sort_keys=True) if e.get("Tags") else None),
            )
            self.stats["err_rows"] += 1

    # ── file walker ───────────────────────────────────────────────
    def ingest_file(self, path, rel):
        txtype = os.path.basename(path).replace(".jsonl", "")
        vin_dir = rel.split(os.sep)[0]
        if txtype.startswith("V"):
            handler = self.handle_v
        elif txtype == "alerts":
            handler = self.handle_alerts
        elif txtype == "connectivity":
            handler = self.handle_connectivity
        elif txtype == "errors":
            handler = self.handle_errors
        else:
            print(f"  skipping unknown txtype: {rel}")
            return

        n = 0
        with open(path, encoding="utf-8", errors="replace") as fh:
            for i, line in enumerate(fh):
                line = line.strip()
                if line:
                    try:
                        handler(json.loads(line), vin_dir)
                    except (json.JSONDecodeError, KeyError, ValueError, TypeError) as ex:
                        print(f"  bad line {rel}:{i + 1}: {ex}", file=sys.stderr)
                n = i + 1
                self.stats["lines"] += 1

        print(f"  {rel}: {n} lines")
        if self.dry:
            self.conn.rollback()
        else:
            self.conn.commit()


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=DEFAULT_DATA)
    ap.add_argument("--dsn", default=DEFAULT_DSN)
    ap.add_argument("--dry-run", action="store_true")
    args = ap.parse_args()
    root = os.path.abspath(args.data_dir)

    files = []
    for dirpath, _, names in os.walk(root):
        for nme in names:
            if nme.endswith(".jsonl"):
                files.append(os.path.join(dirpath, nme))
    files.sort()  # chronological-ish: VIN/YYYY/MM order matters for change detection

    with psycopg.connect(args.dsn) as conn:
        ing = Ingester(conn, dry_run=args.dry_run)
        for path in files:
            ing.ingest_file(path, os.path.relpath(path, root))
        s = ing.stats
        print(f"\nprocessed lines        : {s['lines']:,}")
        print(f"signal observations    : {s['signal_obs']:,}")
        print(f"signal rows written    : {s['signal_rows']:,} "
              f"({100.0 * s['skipped_dup'] / max(1, s['signal_obs']):.1f}% deduped)")
        print(f"alert entries seen     : {s['alerts_in']:,}")
        print(f"alert episode upserts  : {s['alert_upserts']:,}")
        print(f"connectivity rows      : {s['conn_rows']:,}")
        print(f"error rows             : {s['err_rows']:,}")


if __name__ == "__main__":
    main()
