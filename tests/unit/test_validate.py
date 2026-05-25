"""
Unit tests for glue_jobs/validate.py.

All AWS calls are either mocked via moto or via unittest.mock so no
real AWS credentials are needed.
"""

import io
import csv
import pytest
from unittest.mock import MagicMock, patch
from moto import mock_aws
import boto3

from validate import (
    validate_column_presence,
    validate_date_sample,
    read_s3_csv_header_and_sample,
    referential_sample_check,
    REQUIRED_STREAMS_COLUMNS,
)

BUCKET = "test-raw-bucket"
KEY    = "streams/test.csv"


# ── validate_column_presence ──────────────────────────────────

def test_column_presence_all_required_present():
    cols = {"user_id", "track_id", "listen_time", "extra_col"}
    validate_column_presence(cols, REQUIRED_STREAMS_COLUMNS, "test.csv")


def test_column_presence_missing_one_raises():
    cols = {"user_id", "listen_time"}  # track_id missing
    with pytest.raises(ValueError, match="track_id"):
        validate_column_presence(cols, REQUIRED_STREAMS_COLUMNS, "test.csv")


def test_column_presence_empty_set_raises():
    with pytest.raises(ValueError):
        validate_column_presence(set(), REQUIRED_STREAMS_COLUMNS, "test.csv")


def test_column_presence_extra_columns_are_allowed():
    cols = REQUIRED_STREAMS_COLUMNS | {"bonus_col", "another_col"}
    validate_column_presence(cols, REQUIRED_STREAMS_COLUMNS, "test.csv")


# ── validate_date_sample ──────────────────────────────────────

def _make_rows(timestamps):
    return [{"listen_time": ts} for ts in timestamps]


def test_date_sample_all_valid_primary_format():
    rows = _make_rows(["2024-06-25 10:00:00", "2024-06-25 11:30:45"])
    validate_date_sample(rows)


def test_date_sample_short_format_accepted():
    rows = _make_rows(["2024-06-25 10:00", "2024-06-25 11:30"])
    validate_date_sample(rows)


def test_date_sample_mixed_formats_both_accepted():
    rows = _make_rows(["2024-06-25 10:00:00", "2024-06-25 11:30"])
    validate_date_sample(rows)


def test_date_sample_exactly_5_percent_bad_passes():
    # 5 bad out of 100 == 5% == at the threshold (not exceeding)
    good = _make_rows(["2024-06-25 10:00:00"] * 95)
    bad  = _make_rows(["not-a-date"] * 5)
    validate_date_sample(good + bad)


def test_date_sample_over_5_percent_bad_raises():
    good = _make_rows(["2024-06-25 10:00:00"] * 94)
    bad  = _make_rows(["not-a-date"] * 6)  # 6% bad
    with pytest.raises(ValueError, match="5%"):
        validate_date_sample(good + bad)


def test_date_sample_all_bad_raises():
    rows = _make_rows(["garbage", "also-garbage"])
    with pytest.raises(ValueError):
        validate_date_sample(rows)


def test_date_sample_empty_rows_raises():
    with pytest.raises(ValueError):
        validate_date_sample([])


# ── read_s3_csv_header_and_sample ─────────────────────────────

@mock_aws
def test_read_s3_csv_returns_columns_and_rows():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    csv_content = "user_id,track_id,listen_time\n1,abc,2024-06-25 10:00:00\n2,def,2024-06-25 11:00:00\n"
    s3.put_object(Bucket=BUCKET, Key=KEY, Body=csv_content.encode())

    cols, rows = read_s3_csv_header_and_sample(s3, BUCKET, KEY, sample_rows=10)
    assert cols == {"user_id", "track_id", "listen_time"}
    assert len(rows) == 2
    assert rows[0]["user_id"] == "1"


@mock_aws
def test_read_s3_csv_respects_sample_rows_limit():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    lines = ["user_id,track_id,listen_time"] + [f"{i},track_{i},2024-06-25 10:00:00" for i in range(50)]
    s3.put_object(Bucket=BUCKET, Key=KEY, Body="\n".join(lines).encode())

    _, rows = read_s3_csv_header_and_sample(s3, BUCKET, KEY, sample_rows=10)
    assert len(rows) == 10


# ── referential_sample_check ──────────────────────────────────

@mock_aws
def test_referential_check_passes_when_overlap_exists():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    catalog_csv = "id,track_id,track_genre,duration_ms,track_name,artists\n1,track_abc,pop,200000,Song,Artist\n"
    s3.put_object(Bucket=BUCKET, Key="songs/songs.csv", Body=catalog_csv.encode())

    stream_rows = [{"track_id": "track_abc"}, {"track_id": "track_xyz"}]
    referential_sample_check(stream_rows, BUCKET, s3)  # should not raise


@mock_aws
def test_referential_check_raises_when_zero_overlap():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    catalog_csv = "id,track_id,track_genre,duration_ms,track_name,artists\n1,catalog_only_id,pop,200000,Song,Artist\n"
    s3.put_object(Bucket=BUCKET, Key="songs/songs.csv", Body=catalog_csv.encode())

    stream_rows = [{"track_id": "completely_different_id"}]
    with pytest.raises(ValueError, match="Zero stream track_ids"):
        referential_sample_check(stream_rows, BUCKET, s3)


@mock_aws
def test_referential_check_skipped_when_catalog_missing():
    s3 = boto3.client("s3", region_name="us-east-1")
    s3.create_bucket(Bucket=BUCKET)
    # No catalog file uploaded – the function should log a warning and return
    stream_rows = [{"track_id": "any_id"}]
    referential_sample_check(stream_rows, BUCKET, s3)  # should not raise


def test_referential_check_skipped_for_empty_stream():
    s3 = MagicMock()
    referential_sample_check([], "bucket", s3)  # empty stream rows → no raise
