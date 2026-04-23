"""Verifier for the blue/green example.

Given a marker UUID, locates the aligned Iceberg snapshots for each tracked
table on blue and green, and compares their snapshot-level row counts
(`total-records` from the snapshot summary).

This is intentionally a lightweight check — it catches gross divergence
(one side dropped rows, one side's pipeline is stuck) without needing a
reader that handles equality deletes. For byte-level row-by-row equality
(iceberg-diff's "row-hash bucket scan" stage), point a reader that speaks
merge-on-read Iceberg (Trino, Spark, ClickHouse, eventually iceberg-diff)
at the same snapshot ids the verifier printed.

Usage:
    python verify.py <marker_uuid>

Requires:
    pip install pyiceberg[pyarrow,s3fs]
"""
from __future__ import annotations

import argparse
import os
import sys
from typing import Iterable

from pyiceberg.catalog import load_catalog


CATALOG_URI = os.environ.get("CATALOG_URI", "http://localhost:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://localhost:9610")
S3_KEY = os.environ.get("S3_ACCESS_KEY", "admin")
S3_SECRET = os.environ.get("S3_SECRET_KEY", "password")

BLUE_DATA_NS = "app_blue"
GREEN_DATA_NS = "app_green"
BLUE_META_NS = "_pg2iceberg_blue"
GREEN_META_NS = "_pg2iceberg_green"

MARKERS_TABLE = "markers"


def make_catalog():
    return load_catalog(
        "demo",
        **{
            "type": "rest",
            "uri": CATALOG_URI,
            "s3.endpoint": S3_ENDPOINT,
            "s3.access-key-id": S3_KEY,
            "s3.secret-access-key": S3_SECRET,
            "s3.path-style-access": "true",
            "s3.region": "us-east-1",
        },
    )


def load_markers_for(cat, meta_ns: str, marker_uuid: str) -> list[dict]:
    """Return the rows in <meta_ns>.markers for the given marker_uuid."""
    tbl = cat.load_table(f"{meta_ns}.{MARKERS_TABLE}")
    df = tbl.scan().to_pandas()
    if df.empty:
        return []
    matched = df[df["marker_uuid"] == marker_uuid]
    return matched.to_dict(orient="records")


def total_records_at(cat, data_ns: str, pg_qualified: str, snapshot_id: int) -> int | None:
    """Read the Iceberg snapshot summary's `total-records` at the given snapshot."""
    iceberg_name = pg_qualified.split(".")[-1]
    tbl = cat.load_table(f"{data_ns}.{iceberg_name}")
    snap = tbl.snapshot_by_id(snapshot_id)
    if snap is None:
        return None
    # Iceberg summary is a plain dict[str, str]; 'total-records' is the
    # running total of records in the table at this snapshot.
    return int(snap.summary.additional_properties.get("total-records", "0"))


def main(argv: Iterable[str]) -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("marker_uuid")
    args = ap.parse_args(list(argv))

    cat = make_catalog()

    blue_rows = load_markers_for(cat, BLUE_META_NS, args.marker_uuid)
    green_rows = load_markers_for(cat, GREEN_META_NS, args.marker_uuid)

    print(f"\nmarker UUID: {args.marker_uuid}")
    print(f"blue-side rows : {len(blue_rows)}  tables={[r['table_name'] for r in blue_rows]}")
    print(f"green-side rows: {len(green_rows)} tables={[r['table_name'] for r in green_rows]}")

    if not blue_rows or not green_rows:
        print("\nERROR: marker not yet recorded on one or both sides. "
              "Give pg2iceberg a few more seconds to observe the COMMIT.")
        return 2

    by_table_blue = {r["table_name"]: int(r["snapshot_id"]) for r in blue_rows}
    by_table_green = {r["table_name"]: int(r["snapshot_id"]) for r in green_rows}

    tables = sorted(set(by_table_blue) | set(by_table_green))
    missing_blue = [t for t in tables if t not in by_table_blue]
    missing_green = [t for t in tables if t not in by_table_green]
    if missing_blue or missing_green:
        print(f"\nWARNING: mismatched tracked-table sets — blue missing {missing_blue}, green missing {missing_green}")

    all_equal = True
    for t in sorted(by_table_blue.keys() & by_table_green.keys()):
        blue_sid = by_table_blue[t]
        green_sid = by_table_green[t]
        blue_n = total_records_at(cat, BLUE_DATA_NS, t, blue_sid)
        green_n = total_records_at(cat, GREEN_DATA_NS, t, green_sid)
        equal = (blue_n is not None and green_n is not None and blue_n == green_n)
        status = "OK " if equal else "FAIL"
        if not equal:
            all_equal = False
        print(f"  [{status}] {t}: blue@{blue_sid} total-records={blue_n}  vs  green@{green_sid} total-records={green_n}")

    print()
    if all_equal and not missing_blue and not missing_green:
        print("aligned — snapshot total-records match on every tracked table.")
        return 0
    return 1


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
