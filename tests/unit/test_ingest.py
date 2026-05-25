"""
Unit tests for glue_jobs/ingest.py.

DynamoDB tables are mocked via MagicMock so no real AWS calls are made
and the batch_writer context manager is easily inspectable.
"""

import time
import pytest
from decimal import Decimal
from unittest.mock import MagicMock, patch, call

from ingest import (
    to_decimal,
    ttl_one_year,
    write_genre_kpis,
    write_top_songs,
    write_top_genres,
    batch_write,
)


# ── to_decimal ────────────────────────────────────────────────

def test_to_decimal_converts_float():
    assert to_decimal(3.14159) == Decimal("3.1416")


def test_to_decimal_rounds_to_4_places():
    assert to_decimal(1.123456789) == Decimal("1.1235")


def test_to_decimal_none_returns_zero():
    assert to_decimal(None) == Decimal("0")


def test_to_decimal_zero():
    assert to_decimal(0) == Decimal("0")


def test_to_decimal_integer():
    assert to_decimal(42) == Decimal("42.0")


def test_to_decimal_string_number():
    assert to_decimal("9.99") == Decimal("9.99")


# ── ttl_one_year ──────────────────────────────────────────────

def test_ttl_one_year_is_in_the_future():
    ttl = ttl_one_year()
    assert ttl > int(time.time())


def test_ttl_one_year_is_roughly_365_days():
    ttl = ttl_one_year()
    now = int(time.time())
    days_ahead = (ttl - now) / 86400
    assert 364 < days_ahead < 366


# ── batch_write (deduplication) ───────────────────────────────

def _mock_table():
    table  = MagicMock()
    writer = MagicMock()
    table.batch_writer.return_value.__enter__ = MagicMock(return_value=writer)
    table.batch_writer.return_value.__exit__  = MagicMock(return_value=False)
    return table, writer


def test_batch_write_deduplicates_by_pk():
    table, writer = _mock_table()
    items = [
        {"genre_date": "pop#2024-06-25", "metric": "listen_count", "value": Decimal("100")},
        {"genre_date": "pop#2024-06-25", "metric": "listen_count", "value": Decimal("200")},  # dup
    ]
    batch_write(table, items, "test-table", pk_keys=("genre_date", "metric"))
    assert writer.put_item.call_count == 1
    # Last-write-wins: the second item should survive
    written = writer.put_item.call_args[1]["Item"]
    assert written["value"] == Decimal("200")


def test_batch_write_all_unique_items_written():
    table, writer = _mock_table()
    items = [
        {"genre_date": "pop#2024-06-25",  "metric": "listen_count"},
        {"genre_date": "rock#2024-06-25", "metric": "listen_count"},
    ]
    batch_write(table, items, "test-table", pk_keys=("genre_date", "metric"))
    assert writer.put_item.call_count == 2


# ── write_genre_kpis ──────────────────────────────────────────

def test_write_genre_kpis_produces_4_items_per_row():
    table, writer = _mock_table()
    rows = [{
        "genre_date":                    "pop#2024-06-25",
        "track_genre":                   "pop",
        "date":                          "2024-06-25",
        "listen_count":                  1000,
        "unique_listeners":              200,
        "total_listening_time_ms":       200000000,
        "avg_listening_time_per_user_ms": 1000000,
    }]
    write_genre_kpis(rows, table, "genre-kpis-test")
    assert writer.put_item.call_count == 4


def test_write_genre_kpis_item_has_correct_sk_values():
    table, writer = _mock_table()
    rows = [{
        "genre_date":                    "pop#2024-06-25",
        "track_genre":                   "pop",
        "date":                          "2024-06-25",
        "listen_count":                  100,
        "unique_listeners":              10,
        "total_listening_time_ms":       1000000,
        "avg_listening_time_per_user_ms": 100000,
    }]
    write_genre_kpis(rows, table, "genre-kpis-test")
    written_metrics = {c[1]["Item"]["metric"] for c in writer.put_item.call_args_list}
    assert written_metrics == {
        "listen_count", "unique_listeners",
        "total_listening_time_ms", "avg_listening_time_per_user_ms",
    }


def test_write_genre_kpis_sets_expires_at():
    table, writer = _mock_table()
    rows = [{
        "genre_date": "pop#2024-06-25", "track_genre": "pop", "date": "2024-06-25",
        "listen_count": 1, "unique_listeners": 1,
        "total_listening_time_ms": 1000, "avg_listening_time_per_user_ms": 1000,
    }]
    write_genre_kpis(rows, table, "genre-kpis-test")
    item = writer.put_item.call_args_list[0][1]["Item"]
    assert item["expires_at"] > int(time.time())


# ── write_top_songs ───────────────────────────────────────────

def test_write_top_songs_rank_stored_as_string():
    table, writer = _mock_table()
    rows = [{
        "genre_date": "pop#2024-06-25", "track_genre": "pop", "date": "2024-06-25",
        "rank": 1, "track_id": "abc", "track_name": "Song A",
        "artists": "Artist X", "play_count": 42,
    }]
    write_top_songs(rows, table, "top-songs-test")
    item = writer.put_item.call_args[1]["Item"]
    assert item["rank"] == "1"
    assert isinstance(item["rank"], str)


def test_write_top_songs_item_has_expected_keys():
    table, writer = _mock_table()
    rows = [{
        "genre_date": "pop#2024-06-25", "track_genre": "pop", "date": "2024-06-25",
        "rank": 2, "track_id": "xyz", "track_name": "Song B",
        "artists": "Band Y", "play_count": 20,
    }]
    write_top_songs(rows, table, "top-songs-test")
    item = writer.put_item.call_args[1]["Item"]
    for key in ("genre_date", "rank", "track_id", "track_name", "artists", "play_count", "expires_at"):
        assert key in item, f"Key '{key}' missing from top_songs item"


# ── write_top_genres ──────────────────────────────────────────

def test_write_top_genres_rank_stored_as_string():
    table, writer = _mock_table()
    rows = [{
        "date": "2024-06-25", "track_genre": "pop",
        "listen_count": 500, "rank": 1,
    }]
    write_top_genres(rows, table, "top-genres-test")
    item = writer.put_item.call_args[1]["Item"]
    assert item["rank"] == "1"
    assert isinstance(item["rank"], str)


def test_write_top_genres_pk_is_date():
    table, writer = _mock_table()
    rows = [{"date": "2024-06-25", "track_genre": "rock", "listen_count": 100, "rank": 3}]
    write_top_genres(rows, table, "top-genres-test")
    item = writer.put_item.call_args[1]["Item"]
    assert item["date"] == "2024-06-25"
