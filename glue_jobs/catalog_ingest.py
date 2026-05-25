"""
catalog_ingest.py  -  Glue Python Shell Job (Catalog Loader)
=============================================================
Purpose
-------
Loads the songs.csv reference catalog into the songs-catalog DynamoDB
table so downstream applications can query track metadata by track_id.

Idempotency
-----------
The job checks whether the table already has data before writing.
If the table contains >= IDEMPOTENCY_THRESHOLD items, it exits immediately
so it can run safely on every pipeline execution without duplicating work.
On the first run it performs a full load of ~89,741 songs.

Why Python Shell?
-----------------
The CSV is ~30 MB.  Python Shell (0.0625 DPU) starts in ~15 s vs ~90 s
for a Spark cluster and is more than sufficient for a sequential S3 read
followed by DynamoDB batch writes.
"""

import sys
import csv
import io
import json
import logging
import boto3
from datetime import datetime, timezone, timedelta
from decimal import Decimal, InvalidOperation
from awsglue.utils import getResolvedOptions

logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)


def log_event(event_type: str, **kwargs):
    logger.info(json.dumps({"event": event_type, "ts": datetime.utcnow().isoformat(), **kwargs}))


SONGS_CSV_KEY        = "songs/songs.csv"
IDEMPOTENCY_THRESHOLD = 1000


def get_args() -> dict:
    args = getResolvedOptions(sys.argv, ["JOB_NAME", "raw_bucket", "catalog_table"])
    log_event("args_parsed", raw_bucket=args["raw_bucket"], catalog_table=args["catalog_table"])
    return args


def table_already_loaded(table) -> bool:
    """Return True if the table already contains any items.

    Uses Limit=1 so DynamoDB evaluates exactly one item rather than
    scanning up to IDEMPOTENCY_THRESHOLD items on every pipeline run.
    Any non-zero count means a previous load completed (or is in progress),
    so we skip to avoid duplicate writes.
    """
    resp  = table.scan(Limit=1, Select="COUNT")
    count = resp.get("Count", 0)
    log_event("idempotency_check", item_count_sampled=count, threshold=IDEMPOTENCY_THRESHOLD)
    return count >= 1


def safe_decimal(value) -> Decimal:
    """Convert any value to Decimal; returns Decimal("0") for empty/non-numeric inputs."""
    if value is None or str(value).strip() == "":
        return Decimal("0")
    try:
        return Decimal(str(round(float(value), 6)))
    except (ValueError, InvalidOperation):
        return Decimal("0")


def read_songs_csv(s3_client, bucket: str) -> list:
    resp = s3_client.get_object(Bucket=bucket, Key=SONGS_CSV_KEY)
    text = resp["Body"].read().decode("utf-8", errors="replace")
    rows = list(csv.DictReader(io.StringIO(text)))
    log_event("songs_csv_read", bucket=bucket, row_count=len(rows))
    return rows


def build_ddb_item(row: dict, ttl: int):
    """Convert a CSV row to a DynamoDB item dict.  Returns None if track_id is missing."""
    track_id = row.get("track_id", "").strip()
    if not track_id:
        return None
    return {
        "track_id":     track_id,
        "track_name":   str(row.get("track_name", "")),
        "artists":      str(row.get("artists", "")),
        "album_name":   str(row.get("album_name", "")),
        "track_genre":  str(row.get("track_genre", "")),
        "duration_ms":  safe_decimal(row.get("duration_ms")),
        "popularity":   safe_decimal(row.get("popularity")),
        "danceability": safe_decimal(row.get("danceability")),
        "energy":       safe_decimal(row.get("energy")),
        "tempo":        safe_decimal(row.get("tempo")),
        "explicit":     str(row.get("explicit", "")),
        "expires_at":   ttl,
    }


def batch_write_songs(table, items: list, table_name: str) -> int:
    """Write a list of DynamoDB item dicts using batch_writer(); returns count written."""
    written = 0
    skipped = 0
    with table.batch_writer() as writer:
        for item in items:
            if item is None:
                skipped += 1
                continue
            writer.put_item(Item=item)
            written += 1
    log_event("catalog_written", table=table_name, written=written, skipped=skipped)
    return written


def main():
    args     = get_args()
    s3       = boto3.client("s3")
    dynamodb = boto3.resource("dynamodb")
    bucket   = args["raw_bucket"]
    tbl_name = args["catalog_table"]
    table    = dynamodb.Table(tbl_name)
    ttl      = int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp())

    log_event("catalog_ingest_started", bucket=bucket, table=tbl_name)

    if table_already_loaded(table):
        log_event("catalog_ingest_skipped", reason="table already populated")
        logger.info("SKIPPED - songs-catalog table already populated.")
        return

    rows    = read_songs_csv(s3, bucket)
    items   = [build_ddb_item(row, ttl) for row in rows]
    written = batch_write_songs(table, items, tbl_name)

    log_event("catalog_ingest_completed", total_rows=len(rows), items_written=written)
    logger.info("SUCCESS - %d songs loaded into %s", written, tbl_name)


if __name__ == "__main__":
    main()
