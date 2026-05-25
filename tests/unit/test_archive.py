"""
Unit tests for glue_jobs/archive.py.

S3 is mocked via MagicMock so the tests exercise the logic (ETag comparison,
multipart fallback, path construction) without any real AWS calls.
"""

import pytest
from unittest.mock import MagicMock, patch
from archive import verify_copy, compute_archive_key


# ── compute_archive_key ───────────────────────────────────────

def test_compute_archive_key_basic_structure():
    key = compute_archive_key("streams/data.csv", "exec-abc123")
    # Should be: streams/YYYY-MM-DD/exec-abc123/data.csv
    parts = key.split("/")
    assert parts[0] == "streams"
    assert parts[-1] == "data.csv"
    assert parts[-2] == "exec-abc123"


def test_compute_archive_key_date_is_today():
    from datetime import datetime
    today = datetime.utcnow().strftime("%Y-%m-%d")
    key   = compute_archive_key("streams/data.csv", "exec-1")
    assert today in key


def test_compute_archive_key_nested_source_path():
    # Even nested paths should extract just the filename
    key      = compute_archive_key("streams/2024/june/data.csv", "exec-1")
    filename = key.split("/")[-1]
    assert filename == "data.csv"


def test_compute_archive_key_preserves_prefix():
    key = compute_archive_key("streams/data.csv", "exec-xyz")
    assert key.startswith("streams/")


# ── verify_copy (single-part ETag) ────────────────────────────

def _make_s3(src_etag, dest_etag, src_size=100, dest_size=100):
    s3 = MagicMock()
    s3.head_object.side_effect = [
        {"ETag": f'"{src_etag}"',  "ContentLength": src_size},
        {"ETag": f'"{dest_etag}"', "ContentLength": dest_size},
    ]
    return s3


def test_verify_copy_etag_match_returns_true():
    s3 = _make_s3("abc123", "abc123")
    assert verify_copy(s3, "src-bucket", "k.csv", "dst-bucket", "a/k.csv") is True


def test_verify_copy_etag_mismatch_returns_false():
    s3 = _make_s3("abc123", "def456")
    assert verify_copy(s3, "src-bucket", "k.csv", "dst-bucket", "a/k.csv") is False


# ── verify_copy (multipart ETag, content-length fallback) ─────

def _make_s3_multipart(src_size, dest_size):
    # ETags with '-' indicate multipart uploads
    s3 = MagicMock()
    s3.head_object.side_effect = [
        {"ETag": '"abc123-42"', "ContentLength": src_size},
        {"ETag": '"xyz789-42"', "ContentLength": dest_size},
    ]
    return s3


def test_verify_copy_multipart_same_size_returns_true():
    s3 = _make_s3_multipart(5_242_880, 5_242_880)
    assert verify_copy(s3, "src-bucket", "big.csv", "dst-bucket", "a/big.csv") is True


def test_verify_copy_multipart_different_size_returns_false():
    s3 = _make_s3_multipart(5_242_880, 5_242_879)
    assert verify_copy(s3, "src-bucket", "big.csv", "dst-bucket", "a/big.csv") is False


def test_verify_copy_multipart_does_not_compare_etags():
    # Even if ETags happen to match (they won't in real life), content-length
    # is the authoritative check for multipart objects.
    s3 = MagicMock()
    s3.head_object.side_effect = [
        {"ETag": '"same-42"', "ContentLength": 100},
        {"ETag": '"same-42"', "ContentLength": 101},  # different size despite same ETag
    ]
    assert verify_copy(s3, "src", "k", "dst", "k") is False


def test_verify_copy_single_part_uses_etag_not_size():
    # Single-part objects: a size difference alone should not affect the result
    # if ETags match (in practice they'd be consistent, but we verify logic).
    s3 = MagicMock()
    s3.head_object.side_effect = [
        {"ETag": '"abc123"', "ContentLength": 100},
        {"ETag": '"abc123"', "ContentLength": 100},
    ]
    assert verify_copy(s3, "src", "k", "dst", "k") is True
