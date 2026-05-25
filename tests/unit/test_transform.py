"""
Unit tests for glue_jobs/transform.py.

Uses a local SparkSession (session-scoped fixture from conftest.py).
All tests operate on in-memory DataFrames – no AWS calls are made.
"""

import pytest
from datetime import datetime, date
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    IntegerType, StringType, LongType, TimestampType, DateType,
)

from transform import (
    compute_genre_kpis,
    compute_top_songs_per_genre,
    compute_top_genres,
    enrich_streams,
    STREAMS_SCHEMA,
    SONGS_SCHEMA,
)


# ── Fixtures ──────────────────────────────────────────────────

ENRICHED_SCHEMA = StructType([
    StructField("user_id",    IntegerType(), True),
    StructField("track_id",   StringType(),  True),
    StructField("listen_time",TimestampType(),True),
    StructField("date",       DateType(),    True),
    StructField("track_genre",StringType(),  True),
    StructField("duration_ms",LongType(),    True),
    StructField("track_name", StringType(),  True),
    StructField("artists",    StringType(),  True),
])

_D1 = date(2024, 6, 25)
_TS1 = datetime(2024, 6, 25, 10, 0, 0)


def _enriched(spark, rows):
    return spark.createDataFrame(rows, schema=ENRICHED_SCHEMA)


# ── compute_genre_kpis ────────────────────────────────────────

def test_genre_kpis_listen_count(spark):
    rows = [
        (1, "t1", _TS1, _D1, "pop",  200000, "Song A", "Artist X"),
        (2, "t2", _TS1, _D1, "pop",  180000, "Song B", "Artist Y"),
        (3, "t3", _TS1, _D1, "rock", 210000, "Song C", "Artist Z"),
    ]
    df    = _enriched(spark, rows)
    kpis  = compute_genre_kpis(df).collect()
    pop   = next(r for r in kpis if r["track_genre"] == "pop")
    rock  = next(r for r in kpis if r["track_genre"] == "rock")
    assert pop["listen_count"]  == 2
    assert rock["listen_count"] == 1


def test_genre_kpis_unique_listeners(spark):
    rows = [
        (1, "t1", _TS1, _D1, "pop", 200000, "Song A", "Artist X"),
        (1, "t2", _TS1, _D1, "pop", 180000, "Song B", "Artist X"),  # same user
        (2, "t3", _TS1, _D1, "pop", 200000, "Song A", "Artist X"),
    ]
    df   = _enriched(spark, rows)
    kpis = compute_genre_kpis(df).collect()
    pop  = kpis[0]
    assert pop["unique_listeners"] == 2


def test_genre_kpis_total_listening_time(spark):
    rows = [
        (1, "t1", _TS1, _D1, "pop", 100000, "Song A", "Artist X"),
        (2, "t2", _TS1, _D1, "pop", 200000, "Song B", "Artist Y"),
    ]
    df   = _enriched(spark, rows)
    kpis = compute_genre_kpis(df).collect()
    pop  = kpis[0]
    assert pop["total_listening_time_ms"] == 300000


def test_genre_kpis_avg_listening_time(spark):
    rows = [
        (1, "t1", _TS1, _D1, "pop", 200000, "Song A", "Artist X"),
        (2, "t2", _TS1, _D1, "pop", 200000, "Song B", "Artist Y"),
    ]
    df   = _enriched(spark, rows)
    kpis = compute_genre_kpis(df).collect()
    pop  = kpis[0]
    # 400000 total / 2 unique = 200000.0
    assert pop["avg_listening_time_per_user_ms"] == 200000.0


def test_genre_kpis_composite_key_format(spark):
    rows = [(1, "t1", _TS1, _D1, "jazz", 200000, "Song A", "Artist X")]
    df   = _enriched(spark, rows)
    kpis = compute_genre_kpis(df).collect()
    assert kpis[0]["genre_date"] == "jazz#2024-06-25"


def test_genre_kpis_empty_input_returns_empty(spark):
    df   = spark.createDataFrame([], schema=ENRICHED_SCHEMA)
    kpis = compute_genre_kpis(df)
    assert kpis.count() == 0


# ── compute_top_songs_per_genre ───────────────────────────────

def test_top_songs_only_returns_top_3(spark):
    # Genre "pop" has 5 distinct songs; only top 3 should come back
    rows = []
    for i, count in enumerate([50, 40, 30, 20, 10]):
        for _ in range(count):
            rows.append((i + 1, f"t{i}", _TS1, _D1, "pop", 200000, f"Song{i}", "Artist"))
    df        = _enriched(spark, rows)
    top_songs = compute_top_songs_per_genre(df, top_n=3)
    ranks     = sorted([r["rank"] for r in top_songs.collect()])
    assert max(ranks) == 3
    assert min(ranks) == 1


def test_top_songs_ordering_by_play_count(spark):
    rows = [
        (1, "popular",   _TS1, _D1, "pop", 200000, "Hit",   "Star"),
        (2, "popular",   _TS1, _D1, "pop", 200000, "Hit",   "Star"),
        (3, "popular",   _TS1, _D1, "pop", 200000, "Hit",   "Star"),
        (1, "mid_tier",  _TS1, _D1, "pop", 200000, "Mid",   "Artist"),
        (2, "mid_tier",  _TS1, _D1, "pop", 200000, "Mid",   "Artist"),
        (1, "underplay", _TS1, _D1, "pop", 200000, "Under", "Niche"),
    ]
    df        = _enriched(spark, rows)
    top_songs = compute_top_songs_per_genre(df, top_n=3)
    results   = top_songs.orderBy("rank").collect()
    assert results[0]["track_id"] == "popular"
    assert results[0]["rank"]     == 1


def test_top_songs_genre_date_key_present(spark):
    rows = [(1, "t1", _TS1, _D1, "electronic", 200000, "Song", "DJ")]
    df        = _enriched(spark, rows)
    top_songs = compute_top_songs_per_genre(df, top_n=3).collect()
    assert top_songs[0]["genre_date"] == "electronic#2024-06-25"


# ── compute_top_genres ────────────────────────────────────────

def test_top_genres_only_returns_top_5(spark):
    # 7 genres; only top 5 should appear
    rows = []
    for i, genre in enumerate(["pop", "rock", "jazz", "classical", "edm", "folk", "blues"]):
        for _ in range(100 - i * 10):  # descending play counts
            rows.append((i + 1, f"t{i}", _TS1, _D1, genre, 200000, f"Song{i}", "Artist"))
    df        = _enriched(spark, rows)
    kpis      = compute_genre_kpis(df)
    top_gen   = compute_top_genres(kpis, top_n=5)
    assert top_gen.count() == 5


def test_top_genres_rank_1_has_highest_listen_count(spark):
    rows = (
        [(1, "t1", _TS1, _D1, "pop",  200000, "Song", "Artist")] * 50 +
        [(2, "t2", _TS1, _D1, "rock", 200000, "Song", "Artist")] * 10
    )
    df      = _enriched(spark, rows)
    kpis    = compute_genre_kpis(df)
    top_gen = compute_top_genres(kpis, top_n=5).collect()
    rank1   = next(r for r in top_gen if r["rank"] == 1)
    assert rank1["track_genre"] == "pop"


# ── enrich_streams ────────────────────────────────────────────

def test_enrich_streams_inner_join_excludes_unmatched_tracks(spark):
    streams_data = [
        (1, "known_track",   _TS1),
        (2, "unknown_track", _TS1),
    ]
    streams_df = spark.createDataFrame(streams_data, schema=STREAMS_SCHEMA)

    songs_data = [(0, "known_track", "Artist", "Album", "Known Song", 80, 200000,
                   "False", 0.5, 0.8, 5, -5.0, 1, 0.05, 0.1, 0.0, 0.1, 0.6, 120.0, 4, "pop")]
    songs_df   = spark.createDataFrame(songs_data, schema=SONGS_SCHEMA)

    enriched = enrich_streams(streams_df, songs_df)
    assert enriched.count() == 1
    assert enriched.collect()[0]["track_id"] == "known_track"


def test_enrich_streams_adds_date_column(spark):
    streams_data = [(1, "known_track", _TS1)]
    streams_df   = spark.createDataFrame(streams_data, schema=STREAMS_SCHEMA)

    songs_data = [(0, "known_track", "Artist", "Album", "Song", 80, 200000,
                   "False", 0.5, 0.8, 5, -5.0, 1, 0.05, 0.1, 0.0, 0.1, 0.6, 120.0, 4, "pop")]
    songs_df   = spark.createDataFrame(songs_data, schema=SONGS_SCHEMA)

    enriched = enrich_streams(streams_df, songs_df)
    row      = enriched.collect()[0]
    assert row["date"] == _D1
