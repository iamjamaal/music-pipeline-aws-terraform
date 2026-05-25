"""
Unit tests for glue_jobs/catalog_ingest.py.
"""

import io
import csv
import time
import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch
from moto import mock_aws
import boto3

from catalog_ingest import (
    safe_decimal,
    build_ddb_item,
    table_already_loaded,
    batch_write_songs,
    IDEMPOTENCY_THRESHOLD,
)

BUCKET = "test-raw-bucket"


# ── safe_decimal ──────────────────────────────────────────────

def test_safe_decimal_float():
    result = safe_decimal("3.14")
    assert result == Decimal("3.14")


def test_safe_decimal_empty_string_returns_zero():
    assert safe_decimal("") == Decimal("0")


def test_safe_decimal_none_returns_zero():
    assert safe_decimal(None) == Decimal("0")


def test_safe_decimal_non_numeric_returns_zero():
    assert safe_decimal("not-a-number") == Decimal("0")


def test_safe_decimal_integer_string():
    assert safe_decimal("200000") == Decimal("200000")


# ── build_ddb_item ────────────────────────────────────────────

def test_build_ddb_item_missing_track_id_returns_none():
    row = {"track_name": "Song", "track_genre": "pop"}
    assert build_ddb_item(row, ttl=9999999) is None


def test_build_ddb_item_blank_track_id_returns_none():
    row = {"track_id": "  ", "track_name": "Song"}
    assert build_ddb_item(row, ttl=9999999) is None


def test_build_ddb_item_has_required_keys():
    row = {
        "track_id": "abc123", "track_name": "Song A", "artists": "Artist X",
        "album_name": "Album", "track_genre": "pop", "duration_ms": "200000",
        "popularity": "75", "danceability": "0.8", "energy": "0.9",
        "tempo": "120.0", "explicit": "False",
    }
    item = build_ddb_item(row, ttl=9999999)
    assert item is not None
    for key in ("track_id", "track_name", "artists", "album_name", "track_genre",
                "duration_ms", "popularity", "expires_at"):
        assert key in item, f"Key '{key}' missing"


def test_build_ddb_item_numeric_fields_are_decimal():
    row = {
        "track_id": "abc", "track_name": "S", "artists": "A",
        "album_name": "Al", "track_genre": "pop", "duration_ms": "200000",
        "popularity": "80", "danceability": "0.7", "energy": "0.8",
        "tempo": "120.0", "explicit": "False",
    }
    item = build_ddb_item(row, ttl=9999999)
    assert isinstance(item["duration_ms"], Decimal)
    assert isinstance(item["popularity"], Decimal)


def test_build_ddb_item_ttl_preserved():
    row = {"track_id": "abc", "track_name": "S", "artists": "A",
           "album_name": "Al", "track_genre": "pop"}
    item = build_ddb_item(row, ttl=1234567890)
    assert item["expires_at"] == 1234567890


# ── table_already_loaded ──────────────────────────────────────

def test_table_already_loaded_returns_true_when_has_items():
    table = MagicMock()
    table.scan.return_value = {"Count": 1}
    assert table_already_loaded(table) is True


def test_table_already_loaded_returns_true_for_large_table():
    table = MagicMock()
    table.scan.return_value = {"Count": IDEMPOTENCY_THRESHOLD}
    assert table_already_loaded(table) is True


def test_table_already_loaded_empty_table():
    table = MagicMock()
    table.scan.return_value = {"Count": 0}
    assert table_already_loaded(table) is False


# ── batch_write_songs ─────────────────────────────────────────

def _mock_table():
    table  = MagicMock()
    writer = MagicMock()
    table.batch_writer.return_value.__enter__ = MagicMock(return_value=writer)
    table.batch_writer.return_value.__exit__  = MagicMock(return_value=False)
    return table, writer


def test_batch_write_songs_skips_none_items():
    table, writer = _mock_table()
    items   = [None, {"track_id": "abc", "track_genre": "pop"}, None]
    written = batch_write_songs(table, items, "test-table")
    assert written == 1
    assert writer.put_item.call_count == 1


def test_batch_write_songs_writes_all_valid_items():
    table, writer = _mock_table()
    items   = [{"track_id": f"t{i}", "track_genre": "pop"} for i in range(5)]
    written = batch_write_songs(table, items, "test-table")
    assert written == 5
    assert writer.put_item.call_count == 5


def test_batch_write_songs_all_none_writes_zero():
    table, writer = _mock_table()
    written = batch_write_songs(table, [None, None], "test-table")
    assert written == 0
    assert writer.put_item.call_count == 0
