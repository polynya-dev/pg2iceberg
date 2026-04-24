"""Dump marker pairs for a given UUID as TSV lines.

Reads the append-only `_pg2iceberg_blue.markers` and `_pg2iceberg_green.markers`
Iceberg tables via PyIceberg (no equality-delete handling needed, since the
markers table is append-only), joins on `table_name`, and prints one line per
shared table:

    <pg_qualified_table>\t<blue_snapshot_id>\t<green_snapshot_id>

The wrapper shell script feeds each line into iceberg-diff.
"""
from __future__ import annotations

import argparse
import os
import sys

from pyiceberg.catalog import load_catalog

CATALOG_URI = os.environ.get("CATALOG_URI", "http://iceberg-rest:8181")
S3_ENDPOINT = os.environ.get("S3_ENDPOINT", "http://minio:9000")
S3_KEY = os.environ.get("S3_ACCESS_KEY", "admin")
S3_SECRET = os.environ.get("S3_SECRET_KEY", "password")

BLUE_META_NS = "_pg2iceberg_blue"
GREEN_META_NS = "_pg2iceberg_green"
MARKERS_TABLE = "markers"


def load_pairs(uuid: str) -> dict[str, tuple[int, int]]:
    cat = load_catalog(
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
    blue = cat.load_table(f"{BLUE_META_NS}.{MARKERS_TABLE}").scan().to_pandas()
    green = cat.load_table(f"{GREEN_META_NS}.{MARKERS_TABLE}").scan().to_pandas()
    blue = blue[blue["marker_uuid"] == uuid]
    green = green[green["marker_uuid"] == uuid]
    if blue.empty or green.empty:
        return {}
    blue_map = {r["table_name"]: int(r["snapshot_id"]) for _, r in blue.iterrows()}
    green_map = {r["table_name"]: int(r["snapshot_id"]) for _, r in green.iterrows()}
    return {
        t: (blue_map[t], green_map[t])
        for t in sorted(blue_map.keys() & green_map.keys())
    }


def main(argv):
    ap = argparse.ArgumentParser()
    ap.add_argument("marker_uuid")
    args = ap.parse_args(argv)

    pairs = load_pairs(args.marker_uuid)
    if not pairs:
        print(
            f"[dump-markers] no markers for uuid={args.marker_uuid!r} on one or both sides",
            file=sys.stderr,
        )
        return 2
    for table, (blue_sid, green_sid) in pairs.items():
        print(f"{table}\t{blue_sid}\t{green_sid}")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
