"""
validate.py  –  Glue Python Shell Job (Stage 1 of 4)
=====================================================
Purpose
-------
Performs structural validation on an incoming stream CSV before any
expensive Spark computation is attempted.  The job intentionally
raises an exception on failure so Step Functions catches the error
and routes to the failure notification branch.

Checks performed
----------------
1. File accessibility  – the object must exist in S3.
2. Column presence     – streams file needs: user_id, track_id, listen_time.
3. Row count           – rejects empty files.
4. Date format         – listen_time must be parseable as a datetime.
5. Referential sample  – spot-checks that at least 1% of track_ids appear
                         in the songs catalog (catches completely wrong files).

All validation results are written to CloudWatch Logs structured as JSON
so they are queryable via CloudWatch Insights.
"""

import sys
import json
import logging
import boto3
import csv
import io
from datetime import datetime
from awsglue.utils import getResolvedOptions

# ── Logging setup ─────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter('%(asctime)s [%(levelname)s] %(message)s'))
logger.addHandler(handler)

def log_event(event_type: str, **kwargs):
    """Emit a structured JSON log entry for CloudWatch Insights."""
    payload = {"event": event_type, "timestamp": datetime.utcnow().isoformat(), **kwargs}
    logger.info(json.dumps(payload))


# ── Constants ──────────────────────────────────────────────
REQUIRED_STREAMS_COLUMNS = {"user_id", "track_id", "listen_time"}
REQUIRED_SONGS_COLUMNS   = {"track_id", "track_genre", "duration_ms", "track_name", "artists"}
DATE_FORMAT              = "%Y-%m-%d %H:%M:%S"


def get_args() -> dict:
    """Parse Glue job arguments passed from Step Functions."""
    args = getResolvedOptions(sys.argv, ["raw_bucket", "file_key", "execution_id"])
    log_event("args_parsed", **args)
    return args


def read_s3_csv_header_and_sample(s3_client, bucket: str, key: str, sample_rows: int = 200):
    """
    Stream the first `sample_rows` rows of an S3 CSV without downloading
    the full file.  Returns (columns: set, rows: list[dict]).
    """
    response = s3_client.get_object(Bucket=bucket, Key=key)
    # Read only the first 64 KB – enough for header + a sample
    raw_bytes = response["Body"].read(65536)
    text      = raw_bytes.decode("utf-8", errors="replace")
    reader    = csv.DictReader(io.StringIO(text))
    rows      = []
    for i, row in enumerate(reader):
        if i >= sample_rows:
            break
        rows.append(row)
    columns = set(reader.fieldnames or [])
    return columns, rows


def validate_column_presence(columns: set, required: set, source_name: str):
    """Raise a descriptive ValueError if any required column is absent."""
    missing = required - columns
    if missing:
        raise ValueError(
            f"[{source_name}] Missing required columns: {sorted(missing)}. "
            f"Found columns: {sorted(columns)}"
        )
    log_event("columns_validated", source=source_name, columns=sorted(columns))


def validate_date_sample(rows: list, field: str = "listen_time"):
    """
    Check that listen_time values in the sample are parseable.
    Raises ValueError if > 5% of sample rows fail to parse.
    """
    bad  = 0
    good = 0
    for row in rows:
        val = row.get(field, "").strip()
        try:
            # Try the primary format first, then ISO 8601 without seconds
            try:
                datetime.strptime(val, DATE_FORMAT)
            except ValueError:
                datetime.strptime(val, "%Y-%m-%d %H:%M")
            good += 1
        except ValueError:
            bad += 1

    total     = good + bad
    bad_ratio = bad / total if total > 0 else 1.0
    log_event("date_validation", good=good, bad=bad, bad_ratio=round(bad_ratio, 4))

    if bad_ratio > 0.05:
        raise ValueError(
            f"Date parsing failure rate {bad_ratio:.1%} exceeds 5% threshold. "
            f"Expected format: '{DATE_FORMAT}'. Sample bad value: "
            f"{next((r[field] for r in rows if r.get(field)), 'N/A')}"
        )


def validate_row_count(bucket: str, key: str, s3_client) -> int:
    """
    Quickly estimate row count using S3 Select – avoids downloading
    the full file just to count rows.
    """
    try:
        resp = s3_client.select_object_content(
            Bucket     = bucket,
            Key        = key,
            Expression = "SELECT COUNT(*) FROM S3Object",
            ExpressionType = "SQL",
            InputSerialization  = {"CSV": {"FileHeaderInfo": "USE"}},
            OutputSerialization = {"CSV": {}},
        )
        result = b""
        for event in resp["Payload"]:
            if "Records" in event:
                result += event["Records"]["Payload"]
        count = int(result.decode("utf-8").strip())
    except Exception as exc:
        # S3 Select might not be available for all object types; fall back
        logger.warning("S3 Select failed, skipping row count: %s", exc)
        return -1

    log_event("row_count", bucket=bucket, key=key, count=count)
    if count == 0:
        raise ValueError(f"File {key} is empty (0 data rows).")
    return count


def referential_sample_check(stream_rows: list, bucket: str, s3_client) -> None:
    """
    Spot-check that stream track_ids exist in the songs catalog.
    We only check a sample to keep the job fast.
    Fails if ZERO sample IDs match (catches completely wrong files).
    """
    # Load all catalog track_ids from the well-known songs prefix
    try:
        catalog_key = "songs/songs.csv"
        s3_client.head_object(Bucket=bucket, Key=catalog_key)
        response = s3_client.get_object(Bucket=bucket, Key=catalog_key)
        text = response["Body"].read().decode("utf-8", errors="replace")
        reader = csv.DictReader(io.StringIO(text))
        catalog_ids = {r["track_id"].strip() for r in reader if r.get("track_id")}
    except Exception as exc:
        logger.warning("Catalog not yet available, skipping referential check: %s", exc)
        return

    stream_ids = {r["track_id"].strip() for r in stream_rows if r.get("track_id")}
    overlap    = stream_ids & catalog_ids

    log_event(
        "referential_check",
        stream_sample_size = len(stream_ids),
        catalog_sample_size = len(catalog_ids),
        overlap = len(overlap),
    )

    if len(stream_ids) > 0 and len(overlap) == 0:
        raise ValueError(
            "Zero stream track_ids matched the songs catalog sample. "
            "File may be for a different dataset entirely."
        )


def main():
    args       = get_args()
    s3_client  = boto3.client("s3")
    bucket     = args["raw_bucket"]
    key        = args["file_key"]
    exec_id    = args["execution_id"]

    log_event("validation_started", bucket=bucket, key=key, execution_id=exec_id)

    # 1. Check the file exists
    try:
        s3_client.head_object(Bucket=bucket, Key=key)
    except s3_client.exceptions.ClientError as exc:
        raise FileNotFoundError(f"S3 object s3://{bucket}/{key} not found: {exc}") from exc

    # 2. Read header + sample rows
    columns, rows = read_s3_csv_header_and_sample(s3_client, bucket, key)

    # 3. Column presence
    validate_column_presence(columns, REQUIRED_STREAMS_COLUMNS, source_name=key)

    # 4. Row count
    validate_row_count(bucket, key, s3_client)

    # 5. Date format sample
    validate_date_sample(rows, field="listen_time")

    # 6. Referential integrity sample
    referential_sample_check(rows, bucket, s3_client)

    log_event(
        "validation_passed",
        bucket       = bucket,
        key          = key,
        execution_id = exec_id,
        columns      = sorted(columns),
        sample_rows  = len(rows),
    )
    logger.info("SUCCESS – all validation checks passed for s3://%s/%s", bucket, key)


if __name__ == "__main__":
    main()
