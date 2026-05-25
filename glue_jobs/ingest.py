"""
ingest.py  –  Glue Python Shell Job (Stage 3 of 4)
===================================================
Purpose
-------
Reads the three Parquet datasets produced by transform.py and writes
them into the three DynamoDB KPI tables using batched PutItem calls.

Why Python Shell (not PySpark)?
--------------------------------
At this stage the data is already aggregated – typically a few hundred
to a few thousand rows per daily batch, far too small to justify
spinning up a Spark cluster.  Python Shell (0.0625 DPU) starts in
~15 seconds vs ~90 seconds for a Spark job, cuts cost by ~93%, and
is simpler to reason about for sequential DynamoDB writes.

DynamoDB write strategy
-----------------------
We use batch_writer() from boto3, which automatically handles:
  - Chunking into groups of 25 (DynamoDB's BatchWriteItem limit).
  - Exponential backoff on ProvisionedThroughputExceededException.
  - Retrying unprocessed items transparently.

TTL (expires_at)
----------------
Each item gets a Unix-epoch TTL set to 1 year from now.  DynamoDB
will automatically purge items older than 365 days, keeping storage
costs predictable without a separate cleanup job.

Key structure reminder
----------------------
  genre_kpis  :  PK = genre_date ("acoustic#2024-06-25")  SK = metric
  top_songs   :  PK = genre_date                           SK = rank ("1","2","3")
  top_genres  :  PK = date       ("2024-06-25")            SK = rank ("1"…"5")
"""

import sys
import json
import logging
import boto3
import pyarrow.parquet as pq
import pyarrow.fs as pafs
import time

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from awsglue.utils import getResolvedOptions

# ── Logging ───────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

def log_event(event_type: str, **kwargs):
    logger.info(json.dumps({"event": event_type, "ts": datetime.utcnow().isoformat(), **kwargs}))


# ── Helpers ────────────────────────────────────────────────

def ttl_one_year() -> int:
    """Return a Unix timestamp 365 days from now, used as DynamoDB TTL."""
    return int((datetime.now(timezone.utc) + timedelta(days=365)).timestamp())


def to_decimal(value) -> Decimal:
    """
    DynamoDB does not accept Python float – all numeric values must be
    Decimal.  We round to 4 decimal places to avoid overly long strings.
    """
    if value is None:
        return Decimal("0")
    return Decimal(str(round(float(value), 4)))


def read_parquet_from_s3(bucket: str, prefix: str) -> list[dict]:
    """
    Read all Parquet files under the given prefix using PyArrow's S3
    filesystem.  Returns a list of plain Python dicts (one per row)
    so the DynamoDB writer never has to think about Parquet types.
    """
    s3_fs  = pafs.S3FileSystem()
    # Glue writes coalesce(1) Parquet, but we glob the prefix to be safe
    table  = pq.read_table(f"{bucket}/{prefix}", filesystem=s3_fs)
    rows   = table.to_pylist()
    log_event("parquet_read", bucket=bucket, prefix=prefix, row_count=len(rows))
    return rows


def batch_write(dynamo_table, items: list[dict], table_name: str, pk_keys: tuple):
    """
    Write items using DynamoDB's batch_writer context manager.
    Deduplicates by composite primary key before writing to avoid
    ValidationException from BatchWriteItem on duplicate keys.
    """
    seen = {}
    for item in items:
        key = tuple(item.get(k) for k in pk_keys)
        seen[key] = item  # last write wins on duplicate
    deduped = list(seen.values())
    log_event("dynamo_dedup", table=table_name, before=len(items), after=len(deduped))

    written = 0
    start   = time.time()
    with dynamo_table.batch_writer() as writer:
        for item in deduped:
            writer.put_item(Item=item)
            written += 1
    elapsed = round(time.time() - start, 2)
    log_event(
        "dynamo_batch_write",
        table         = table_name,
        items_written = written,
        elapsed_s     = elapsed,
        items_per_s   = round(written / elapsed, 1) if elapsed > 0 else 0,
    )


# ── Per-table write logic ──────────────────────────────────

def write_genre_kpis(rows: list[dict], table, table_name: str):
    """
    Each row in genre_kpis Parquet becomes FOUR DynamoDB items – one per
    metric type – so analysts can query a single metric across all genres
    with a begins_with on the SK.

    Item shape:
      PK = genre_date  e.g. "acoustic#2024-06-25"
      SK = metric      e.g. "listen_count"
      value            e.g. Decimal("4201")
      genre, date      denormalised for readability
      expires_at       Unix TTL
    """
    metrics = [
        ("listen_count",                  "listen_count"),
        ("unique_listeners",              "unique_listeners"),
        ("total_listening_time_ms",       "total_listening_time_ms"),
        ("avg_listening_time_per_user_ms","avg_listening_time_per_user_ms"),
    ]
    items = []
    ttl   = ttl_one_year()
    for row in rows:
        genre_date = row.get("genre_date") or f"{row['track_genre']}#{row['date']}"
        for metric_key, col in metrics:
            items.append({
                "genre_date":  genre_date,
                "metric":      metric_key,
                "value":       to_decimal(row.get(col, 0)),
                "genre":       str(row.get("track_genre", "")),
                "date":        str(row.get("date", "")),
                "expires_at":  ttl,
            })
    log_event("genre_kpis_items_prepared", count=len(items))
    batch_write(table, items, table_name, pk_keys=("genre_date", "metric"))


def write_top_songs(rows: list[dict], table, table_name: str):
    """
    Item shape:
      PK = genre_date  e.g. "pop#2024-06-25"
      SK = rank        e.g. "1"
      track_id, track_name, artists, play_count, genre, date
      expires_at
    """
    ttl   = ttl_one_year()
    items = []
    for row in rows:
        genre_date = row.get("genre_date") or f"{row['track_genre']}#{row['date']}"
        items.append({
            "genre_date":  genre_date,
            "rank":        str(int(row.get("rank", 0))),
            "track_id":    str(row.get("track_id", "")),
            "track_name":  str(row.get("track_name", "")),
            "artists":     str(row.get("artists", "")),
            "play_count":  to_decimal(row.get("play_count", 0)),
            "genre":       str(row.get("track_genre", "")),
            "date":        str(row.get("date", "")),
            "expires_at":  ttl,
        })
    log_event("top_songs_items_prepared", count=len(items))
    batch_write(table, items, table_name, pk_keys=("genre_date", "rank"))


def write_top_genres(rows: list[dict], table, table_name: str):
    """
    Item shape:
      PK = date        e.g. "2024-06-25"
      SK = rank        e.g. "1"
      genre, listen_count
      expires_at
    """
    ttl   = ttl_one_year()
    items = []
    for row in rows:
        items.append({
            "date":         str(row.get("date", "")),
            "rank":         str(int(row.get("rank", 0))),
            "genre":        str(row.get("track_genre", "")),
            "listen_count": to_decimal(row.get("listen_count", 0)),
            "expires_at":   ttl,
        })
    log_event("top_genres_items_prepared", count=len(items))
    batch_write(table, items, table_name, pk_keys=("date", "rank"))


# ── Main ───────────────────────────────────────────────────

def get_args() -> dict:
    args = getResolvedOptions(
        sys.argv,
        [
            "raw_bucket", "execution_id",
            "genre_kpis_table", "top_songs_table", "top_genres_table",
        ]
    )
    log_event("args_parsed", **args)
    return args


def main():
    args     = get_args()
    dynamodb = boto3.resource("dynamodb")
    bucket   = args["raw_bucket"]
    exec_id  = args["execution_id"]
    prefix   = f"transformed/{exec_id}"

    log_event("ingest_started", bucket=bucket, execution_id=exec_id)

    # Read the three Parquet outputs from the transform stage
    genre_kpis_rows = read_parquet_from_s3(bucket, f"{prefix}/genre_kpis")
    top_songs_rows  = read_parquet_from_s3(bucket, f"{prefix}/top_songs")
    top_genres_rows = read_parquet_from_s3(bucket, f"{prefix}/top_genres")

    # Write each dataset to its dedicated DynamoDB table
    write_genre_kpis(
        genre_kpis_rows,
        dynamodb.Table(args["genre_kpis_table"]),
        args["genre_kpis_table"],
    )
    write_top_songs(
        top_songs_rows,
        dynamodb.Table(args["top_songs_table"]),
        args["top_songs_table"],
    )
    write_top_genres(
        top_genres_rows,
        dynamodb.Table(args["top_genres_table"]),
        args["top_genres_table"],
    )

    log_event("ingest_completed", execution_id=exec_id)
    logger.info("SUCCESS – all KPIs written to DynamoDB for execution %s", exec_id)


if __name__ == "__main__":
    main()
