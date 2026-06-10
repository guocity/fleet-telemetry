#!/usr/bin/env python3
"""Create the fleet_telemetry database, apply schema.sql, seed the signal
dictionary from protos/vehicle_data.proto.

signal_id == proto Field enum number, so ids are stable across rebuilds and
match Tesla's documentation. Re-runnable (idempotent).

Usage:
    python3 setup_db.py [--dsn postgresql://postgres:postgres@192.168.18.2:5432] \
                        [--dbname fleet_telemetry] [--drop]
"""

import argparse
import os
import re
import sys

import psycopg
from psycopg import sql

HERE = os.path.dirname(os.path.abspath(__file__))
REPO = os.path.abspath(os.path.join(HERE, "..", ".."))
SCHEMA_SQL = os.path.join(HERE, "..", "schema.sql")
PROTO = os.path.join(REPO, "protos", "vehicle_data.proto")

# Field enum number -> storage class, derived from the Value oneof type used
# by transformers/payload.go. Anything not classified is 'text' (enums) —
# value_kind is informational only; the ingester stores by actual JSON type.


def parse_field_enum(proto_path):
    """Return [(number, name)] from `enum Field { ... }`."""
    text = open(proto_path, encoding="utf-8").read()
    m = re.search(r"enum\s+Field\s*\{(.*?)\n\}", text, re.S)
    if not m:
        sys.exit(f"enum Field not found in {proto_path}")
    fields = []
    for name, num in re.findall(r"^\s*(\w+)\s*=\s*(\d+)\s*;", m.group(1), re.M):
        if int(num) == 0:  # Unknown
            continue
        fields.append((int(num), name))
    return fields


def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--dsn", default="postgresql://postgres:postgres@192.168.18.2:5432")
    ap.add_argument("--dbname", default="fleet_telemetry")
    ap.add_argument("--drop", action="store_true", help="drop and recreate the database")
    args = ap.parse_args()

    admin_dsn = f"{args.dsn.rstrip('/')}/template1"

    with psycopg.connect(admin_dsn, autocommit=True) as conn:
        cur = conn.cursor()
        cur.execute("SELECT 1 FROM pg_database WHERE datname = %s", (args.dbname,))
        exists = cur.fetchone() is not None
        if exists and args.drop:
            cur.execute(sql.SQL("DROP DATABASE {} WITH (FORCE)").format(sql.Identifier(args.dbname)))
            exists = False
            print(f"dropped database {args.dbname}")
        if not exists:
            cur.execute(sql.SQL("CREATE DATABASE {}").format(sql.Identifier(args.dbname)))
            print(f"created database {args.dbname}")

    db_dsn = f"{args.dsn.rstrip('/')}/{args.dbname}"
    schema = open(SCHEMA_SQL, encoding="utf-8").read()

    with psycopg.connect(db_dsn, autocommit=True) as conn:
        conn.execute(schema)
        print("schema applied")

        fields = parse_field_enum(PROTO)
        with conn.cursor() as cur:
            cur.executemany(
                """INSERT INTO signals (signal_id, name)
                   VALUES (%s, %s)
                   ON CONFLICT (signal_id) DO UPDATE SET name = EXCLUDED.name""",
                fields,
            )
        print(f"seeded {len(fields)} signals from {os.path.relpath(PROTO, REPO)}")

        n = conn.execute("SELECT count(*) FROM signals").fetchone()[0]
        print(f"signals in dictionary: {n}")


if __name__ == "__main__":
    main()
