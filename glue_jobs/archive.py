"""
archive.py  –  Glue Python Shell Job (Stage 4 of 4)
====================================================
Purpose
-------
After the pipeline has successfully validated, transformed, and loaded
data into DynamoDB, this job moves the source CSV from the raw/ prefix
to archive/.  This serves two purposes:

  1. Idempotency guard  – the EventBridge S3-trigger rule fires every
     time a new object appears under raw/streams/.  If a file is never
     archived after processing, it would trigger the pipeline again on
     the next deployment cycle.

  2. Audit trail  – archived files are retained under archive/streams/
     for 12 months (Glacier lifecycle), giving engineers a way to
     replay any day's pipeline if a bug is found later.

Copy-then-delete strategy
--------------------------
S3 does not have a true "move" operation.  We copy the object to the
archive bucket first, verify the copy integrity via ETag comparison,
and only then delete the source.  This prevents silent data loss if
the copy is interrupted mid-way.

The job does NOT raise on archive failure because:
  - The KPI data is already safely in DynamoDB.
  - Step Functions catches the error, sends a WARNING alert,
    and transitions to PipelineSucceeded anyway.
  - The raw file can be manually cleaned up without re-running the
    full pipeline.
"""

import sys
import json
import logging
import boto3
from datetime import datetime
from awsglue.utils import getResolvedOptions

# ── Logging ───────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

def log_event(event_type: str, **kwargs):
    logger.info(json.dumps({"event": event_type, "ts": datetime.utcnow().isoformat(), **kwargs}))


def get_args() -> dict:
    args = getResolvedOptions(
        sys.argv,
        ["raw_bucket", "archive_bucket", "file_key", "execution_id"]
    )
    log_event("args_parsed", **args)
    return args


def compute_archive_key(file_key: str, execution_id: str) -> str:
    """
    Construct the destination key inside the archive bucket.
    We include the execution_id in the path so that if the same
    logical filename is re-used across days, the archive does not
    silently overwrite a previous run.

    Example:
      raw:     streams/streams1.csv
      archive: streams/2024-06-25/exec-abc123/streams1.csv
    """
    date_str  = datetime.utcnow().strftime("%Y-%m-%d")
    filename  = file_key.split("/")[-1]
    prefix    = "/".join(file_key.split("/")[:-1])  # e.g. "streams"
    return f"{prefix}/{date_str}/{execution_id}/{filename}"


def copy_object(s3_client, source_bucket: str, source_key: str,
                dest_bucket: str, dest_key: str) -> str:
    """
    Copy an S3 object and return the destination ETag for verification.
    Uses server-side copy (no data travels through this host), so it
    is fast and free regardless of file size.
    """
    copy_source = {"Bucket": source_bucket, "Key": source_key}
    response = s3_client.copy_object(
        CopySource              = copy_source,
        Bucket                  = dest_bucket,
        Key                     = dest_key,
        ServerSideEncryption    = "AES256",
        MetadataDirective       = "COPY",
        TaggingDirective        = "COPY",
    )
    dest_etag = response.get("CopyObjectResult", {}).get("ETag", "")
    log_event(
        "object_copied",
        source     = f"s3://{source_bucket}/{source_key}",
        destination = f"s3://{dest_bucket}/{dest_key}",
        dest_etag  = dest_etag,
    )
    return dest_etag


def verify_copy(s3_client, source_bucket: str, source_key: str,
                dest_bucket: str, dest_key: str) -> bool:
    """
    Compare ETags of source and destination to confirm the copy is
    byte-for-byte identical.  Returns False if verification fails so
    the caller can decide whether to abort or continue.

    Note: ETags are only guaranteed to be comparable for objects smaller
    than 5 GB that were NOT uploaded as multipart.  For larger files you
    would switch to an MD5 or SHA-256 comparison via S3 metadata.
    """
    src_head  = s3_client.head_object(Bucket=source_bucket, Key=source_key)
    dest_head = s3_client.head_object(Bucket=dest_bucket,   Key=dest_key)
    src_etag  = src_head.get("ETag", "").strip('"')
    dest_etag = dest_head.get("ETag", "").strip('"')
    match     = src_etag == dest_etag
    log_event(
        "copy_verified",
        src_etag  = src_etag,
        dest_etag = dest_etag,
        match     = match,
    )
    return match


def delete_source(s3_client, bucket: str, key: str):
    """Delete the original raw file after successful copy verification."""
    s3_client.delete_object(Bucket=bucket, Key=key)
    log_event("source_deleted", bucket=bucket, key=key)


def also_clean_parquet(s3_client, bucket: str, execution_id: str):
    """
    Optionally remove the intermediate Parquet files produced by the
    transform job.  This keeps the raw bucket tidy and avoids paying
    for repeated storage of intermediate results.
    """
    prefix   = f"transformed/{execution_id}/"
    paginator = s3_client.get_paginator("list_objects_v2")
    deleted  = 0
    for page in paginator.paginate(Bucket=bucket, Prefix=prefix):
        for obj in page.get("Contents", []):
            s3_client.delete_object(Bucket=bucket, Key=obj["Key"])
            deleted += 1
    log_event("parquet_cleaned", bucket=bucket, prefix=prefix, deleted=deleted)


def main():
    args     = get_args()
    s3       = boto3.client("s3")
    raw_bkt  = args["raw_bucket"]
    arc_bkt  = args["archive_bucket"]
    file_key = args["file_key"]
    exec_id  = args["execution_id"]

    log_event("archive_started", raw_bucket=raw_bkt, archive_bucket=arc_bkt,
              file_key=file_key, execution_id=exec_id)

    archive_key = compute_archive_key(file_key, exec_id)

    # Step 1: Copy source → archive
    copy_object(s3, raw_bkt, file_key, arc_bkt, archive_key)

    # Step 2: Verify integrity before deleting source
    if not verify_copy(s3, raw_bkt, file_key, arc_bkt, archive_key):
        raise RuntimeError(
            f"ETag mismatch after copy.  Source not deleted.  "
            f"src=s3://{raw_bkt}/{file_key}  "
            f"dest=s3://{arc_bkt}/{archive_key}"
        )

    # Step 3: Delete the source raw file
    delete_source(s3, raw_bkt, file_key)

    # Step 4: Clean up intermediate Parquet files
    also_clean_parquet(s3, raw_bkt, exec_id)

    log_event("archive_completed", archive_path=f"s3://{arc_bkt}/{archive_key}")
    logger.info("SUCCESS – file archived to s3://%s/%s", arc_bkt, archive_key)


if __name__ == "__main__":
    main()
