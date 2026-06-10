#!/usr/bin/env python3
"""Inspect fleet-telemetry JSONL output and quantify redundancy.

Walks telemetry-data/{VIN}/{YYYY}/{MM}/{txtype}.jsonl and reports, without
ever dumping raw lines:

  1. Record counts, line-size stats, and which envelope keys each file uses
     (the output format changed over time: some files carry raw `payload`,
     `decoded_payload`, and `metadata` blobs, some don't).
  2. Byte share of each top-level key — how much disk goes to redundant
     envelope blobs vs actual data.
  3. Per-signal stats for V.jsonl: occurrences, distinct values, and the
     consecutive-repeat ratio (a repeat = same value as the previous report
     for that VIN+signal). The repeat ratio is exactly the fraction of rows
     a change-only store would drop.
  4. Alert/error/connectivity dedup potential: alerts are re-sent in batches,
     so the same (name, started_at, ended_at) tuple appears many times.

Usage:
    python3 inspect_telemetry.py [--data-dir ../../telemetry-data] [--json out.json]
"""

import argparse
import json
import os
import sys
from collections import Counter, defaultdict

ENVELOPE_DATA_KEYS = {"Vin", "CreatedAt", "IsResend"}


def fmt_bytes(n):
    for unit in ("B", "KB", "MB", "GB"):
        if n < 1024 or unit == "GB":
            return f"{n:,.1f} {unit}" if unit != "B" else f"{n:,} B"
        n /= 1024.0


def value_key(wrapper):
    """Normalize a signal value (typed wrapper or bare) to a hashable key."""
    if isinstance(wrapper, dict):
        # typed: {"doubleValue": 1.5} or composite {"latitude":..}
        return json.dumps(wrapper, sort_keys=True)
    return json.dumps(wrapper)


class SignalStats:
    __slots__ = ("count", "values", "repeats", "last", "type_keys", "bytes")

    def __init__(self):
        self.count = 0
        self.values = set()
        self.repeats = 0
        self.last = {}  # vin -> last value key
        self.type_keys = Counter()
        self.bytes = 0

    def add(self, vin, name, wrapper):
        k = value_key(wrapper)
        self.count += 1
        self.bytes += len(name) + len(k) + 4
        if len(self.values) <= 50:
            self.values.add(k)
        if self.last.get(vin) == k:
            self.repeats += 1
        self.last[vin] = k
        if isinstance(wrapper, dict) and len(wrapper) == 1:
            self.type_keys[next(iter(wrapper))] += 1
        elif wrapper is None:
            self.type_keys["null(delta-removed)"] += 1
        else:
            self.type_keys[type(wrapper).__name__] += 1


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--data-dir", default=os.path.join(os.path.dirname(__file__), "..", "..", "telemetry-data"))
    ap.add_argument("--json", help="also write machine-readable summary to this path")
    args = ap.parse_args()
    root = os.path.abspath(args.data_dir)

    files = []
    for dirpath, _, names in os.walk(root):
        for n in sorted(names):
            if n.endswith(".jsonl"):
                files.append(os.path.join(dirpath, n))
    if not files:
        sys.exit(f"no .jsonl files under {root}")

    # ── aggregates ────────────────────────────────────────────────
    per_file = []
    key_bytes = Counter()        # top-level key -> serialized bytes
    total_bytes = 0
    records = Counter()          # txtype -> record count
    bad_lines = Counter()
    type_field = Counter()       # value of "type" field (delta encoding)
    vins = set()

    signals = defaultdict(SignalStats)   # signal name -> stats
    v_signal_rows = 0

    alert_total = 0
    alert_distinct = set()       # (vin, name, started_at, ended_at)
    alert_open = 0
    err_total = 0
    err_distinct = set()
    conn_total = 0
    conn_distinct = set()

    for path in files:
        rel = os.path.relpath(path, root)
        vin_dir = rel.split(os.sep)[0]
        txtype = os.path.basename(path).replace(".jsonl", "")
        fsize = os.path.getsize(path)
        total_bytes += fsize
        n_lines = 0
        keysets = Counter()

        with open(path, "r", encoding="utf-8", errors="replace") as fh:
            for line in fh:
                line = line.strip()
                if not line:
                    continue
                try:
                    rec = json.loads(line)
                except json.JSONDecodeError:
                    bad_lines[rel] += 1
                    continue
                n_lines += 1
                records[txtype] += 1
                keysets[",".join(sorted(rec.keys()))] += 1
                vin = rec.get("vin") or vin_dir
                vins.add(vin)
                if "type" in rec:
                    type_field[rec["type"]] += 1

                for k, v in rec.items():
                    key_bytes[k] += len(json.dumps(v)) + len(k) + 5

                data = rec.get("data")
                if txtype.startswith("V") and isinstance(data, dict):
                    for name, wrapper in data.items():
                        if name in ENVELOPE_DATA_KEYS:
                            continue
                        signals[name].add(vin, name, wrapper)
                        v_signal_rows += 1
                elif txtype == "alerts" and isinstance(data, list):
                    for a in data:
                        alert_total += 1
                        key = (vin, a.get("Name"), a.get("StartedAt"), a.get("EndedAt"))
                        alert_distinct.add(key)
                        if a.get("EndedAt") is None:
                            alert_open += 1
                elif txtype == "errors" and isinstance(data, list):
                    for e in data:
                        err_total += 1
                        err_distinct.add((vin, e.get("Name"), e.get("CreatedAt"),
                                          json.dumps(e.get("Tags"), sort_keys=True)))
                elif txtype == "connectivity" and isinstance(data, dict):
                    conn_total += 1
                    conn_distinct.add((vin, data.get("ConnectionID"), data.get("Status")))

        per_file.append({
            "file": rel, "txtype": txtype, "bytes": fsize, "records": n_lines,
            "envelope_shapes": dict(keysets),
        })

    # ── report ────────────────────────────────────────────────────
    print(f"telemetry-data root : {root}")
    print(f"total size          : {fmt_bytes(total_bytes)}  in {len(files)} files, {sum(records.values()):,} records")
    print(f"VINs                : {', '.join(sorted(vins))}")
    print(f"records by type     : {dict(records)}")
    if type_field:
        print(f"delta 'type' field  : {dict(type_field)}")
    if bad_lines:
        print(f"unparseable lines   : {dict(bad_lines)}")

    print("\n── disk share by top-level key (where the bytes go) ──")
    for k, b in key_bytes.most_common():
        print(f"  {k:<18} {fmt_bytes(b):>12}  {100.0*b/sum(key_bytes.values()):5.1f}%")

    print("\n── envelope shapes per file ──")
    for pf in per_file:
        shapes = "; ".join(f"[{s}]×{c}" for s, c in pf["envelope_shapes"].items())
        print(f"  {pf['file']:<50} {pf['records']:>7,} rec  {fmt_bytes(pf['bytes']):>10}  {shapes}")

    print(f"\n── V.jsonl signals: {v_signal_rows:,} signal observations, {len(signals)} distinct signals ──")
    print(f"{'signal':<42}{'count':>9}{'distinct':>9}{'repeat%':>9}  type")
    total_repeats = 0
    for name, st in sorted(signals.items(), key=lambda kv: -kv[1].count):
        total_repeats += st.repeats
        distinct = f"{len(st.values)}" if len(st.values) <= 50 else ">50"
        tk = st.type_keys.most_common(1)[0][0]
        print(f"{name:<42}{st.count:>9,}{distinct:>9}{100.0*st.repeats/st.count:>8.1f}%  {tk}")
    if v_signal_rows:
        print(f"\n  >>> change-only storage would drop {total_repeats:,}/{v_signal_rows:,} "
              f"signal rows = {100.0*total_repeats/v_signal_rows:.1f}% <<<")

    print("\n── alerts ──")
    print(f"  alert entries (rows if stored naively) : {alert_total:,}")
    print(f"  distinct (vin,name,start,end) episodes : {len(alert_distinct):,}")
    if alert_total:
        print(f"  duplication factor                     : {alert_total/max(1,len(alert_distinct)):.1f}x "
              f"({100.0*(1-len(alert_distinct)/alert_total):.1f}% redundant)")
    print(f"  entries with EndedAt=null (still open) : {alert_open:,}")

    print("\n── errors ──")
    print(f"  error entries / distinct : {err_total:,} / {len(err_distinct):,}")

    print("\n── connectivity ──")
    print(f"  events / distinct (vin,conn_id,status) : {conn_total:,} / {len(conn_distinct):,}")

    if args.json:
        out = {
            "total_bytes": total_bytes,
            "records": dict(records),
            "type_field": dict(type_field),
            "key_bytes": dict(key_bytes),
            "files": per_file,
            "signals": {
                n: {"count": s.count,
                    "distinct": (len(s.values) if len(s.values) <= 50 else None),
                    "repeats": s.repeats,
                    "type_keys": dict(s.type_keys)}
                for n, s in signals.items()
            },
            "alerts": {"total": alert_total, "distinct": len(alert_distinct), "open": alert_open},
            "errors": {"total": err_total, "distinct": len(err_distinct)},
            "connectivity": {"total": conn_total, "distinct": len(conn_distinct)},
        }
        with open(args.json, "w") as fh:
            json.dump(out, fh, indent=2)
        print(f"\nJSON summary written to {args.json}")


if __name__ == "__main__":
    main()
