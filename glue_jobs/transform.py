"""
transform.py  –  Glue PySpark Job (Stage 2 of 4)
=================================================
Purpose
-------
This is the computational heart of the pipeline.  It:
  1. Reads the incoming stream CSV file (user_id, track_id, listen_time).
  2. Reads the songs catalog (track_id, track_genre, duration_ms, track_name, artists).
  3. Joins them to enrich each stream event with genre and duration.
  4. Computes five daily KPI sets:
       a. Genre-level aggregates: listen_count, unique_listeners,
          total_listening_time_ms, avg_listening_time_per_user_ms
       b. Top-3 songs per genre per day  (by listen count)
       c. Top-5 genres per day           (by listen count)
  5. Writes each KPI set as Parquet to s3://{raw_bucket}/transformed/{execution_id}/

Why Parquet?
-----------
The downstream ingest job reads these outputs.  Parquet gives us
columnar compression (much smaller than CSV) and typed schema, so the
ingest job can read only the columns it needs per DynamoDB table.

Spark window functions are used for the top-N rankings because they
allow a single pass over the sorted data rather than a separate sort
per group.
"""

import sys
import json
import logging
from datetime import datetime, timezone

from awsglue.utils import getResolvedOptions
from pyspark.context import SparkContext
from awsglue.context import GlueContext
from awsglue.job import Job

from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, LongType, TimestampType, DoubleType
)

# ── Logging ───────────────────────────────────────────────
logger = logging.getLogger()
logger.setLevel(logging.INFO)
handler = logging.StreamHandler(sys.stdout)
handler.setFormatter(logging.Formatter("%(asctime)s [%(levelname)s] %(message)s"))
logger.addHandler(handler)

def log_event(event_type: str, **kwargs):
    logger.info(json.dumps({"event": event_type, "ts": datetime.utcnow().isoformat(), **kwargs}))


# ── Schema definitions ─────────────────────────────────────
# Explicit schemas prevent Spark from inferring wrong types,
# especially important for track_id (always string, never numeric)
STREAMS_SCHEMA = StructType([
    StructField("user_id",     IntegerType(),   True),
    StructField("track_id",    StringType(),    True),
    StructField("listen_time", TimestampType(), True),
])

SONGS_SCHEMA = StructType([
    StructField("id",               IntegerType(), True),
    StructField("track_id",         StringType(),  True),
    StructField("artists",          StringType(),  True),
    StructField("album_name",       StringType(),  True),
    StructField("track_name",       StringType(),  True),
    StructField("popularity",       IntegerType(), True),
    StructField("duration_ms",      LongType(),    True),
    StructField("explicit",         StringType(),  True),
    StructField("danceability",     DoubleType(),  True),
    StructField("energy",           DoubleType(),  True),
    StructField("key",              IntegerType(), True),
    StructField("loudness",         DoubleType(),  True),
    StructField("mode",             IntegerType(), True),
    StructField("speechiness",      DoubleType(),  True),
    StructField("acousticness",     DoubleType(),  True),
    StructField("instrumentalness", DoubleType(),  True),
    StructField("liveness",         DoubleType(),  True),
    StructField("valence",          DoubleType(),  True),
    StructField("tempo",            DoubleType(),  True),
    StructField("time_signature",   IntegerType(), True),
    StructField("track_genre",      StringType(),  True),
])


def get_args() -> dict:
    args = getResolvedOptions(
        sys.argv,
        ["JOB_NAME", "raw_bucket", "file_key", "execution_id", "output_prefix"]
    )
    log_event("args_parsed", **{k: v for k, v in args.items() if k != "JOB_NAME"})
    return args


def read_streams(glue_ctx, bucket: str, key: str):
    """
    Read the incoming stream CSV.  We use Spark's CSV reader with the
    explicit schema (avoids an extra inference scan) and set mode=PERMISSIVE
    so a handful of malformed rows don't abort the entire job – they are
    counted and logged instead.
    """
    path = f"s3://{bucket}/{key}"
    df = (
        glue_ctx.spark_session.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .option("columnNameOfCorruptRecord", "_corrupt_record")
        .option("timestampFormat", "yyyy-MM-dd HH:mm:ss")
        .schema(STREAMS_SCHEMA)
        .csv(path)
    )
    total   = df.count()
    corrupt = df.filter(F.col("_corrupt_record").isNotNull()).count() if "_corrupt_record" in df.columns else 0
    log_event("streams_read", path=path, total_rows=total, corrupt_rows=corrupt)
    # Drop corrupt rows – they were already counted above
    return df.filter(F.col("user_id").isNotNull() & F.col("track_id").isNotNull())


def read_songs_catalog(glue_ctx, bucket: str):
    """
    The songs catalog is a reference dataset.  We read all genres but only
    keep the columns needed for the join to minimise shuffle overhead.
    The catalog may have duplicate track_ids (same song in multiple genres)
    – we keep all rows because genre diversity is intentional in the source.
    """
    path = f"s3://{bucket}/songs/songs.csv"
    df = (
        glue_ctx.spark_session.read
        .option("header", "true")
        .option("mode", "PERMISSIVE")
        .schema(SONGS_SCHEMA)
        .csv(path)
        .select("track_id", "track_genre", "duration_ms", "track_name", "artists")
        .filter(F.col("track_id").isNotNull() & F.col("track_genre").isNotNull())
    )
    log_event("songs_catalog_read", path=path, row_count=df.count())
    return df


def enrich_streams(streams_df, songs_df):
    """
    Inner-join streams with the songs catalog on track_id.
    Using inner join ensures we only compute KPIs for tracks
    we have metadata for (no orphaned stream events).

    We also extract the date component from listen_time here so
    all downstream aggregations can group by the same 'date' column.
    """
    enriched = (
        streams_df.alias("s")
        .join(songs_df.alias("g"), on="track_id", how="inner")
        .withColumn("date", F.to_date("listen_time"))
        .select(
            F.col("s.user_id"),
            F.col("s.track_id"),
            F.col("s.listen_time"),
            F.col("date"),
            F.col("g.track_genre"),
            F.col("g.duration_ms"),
            F.col("g.track_name"),
            F.col("g.artists"),
        )
    )
    matched = enriched.count()
    log_event("streams_enriched", matched_rows=matched)
    return enriched


def compute_genre_kpis(enriched_df):
    """
    Compute the four daily genre-level KPIs:
      - listen_count              : raw play count per genre per day
      - unique_listeners          : distinct user_ids (approx HyperLogLog for scale)
      - total_listening_time_ms   : sum of duration_ms across all plays
      - avg_listening_time_per_user_ms : total / unique_listeners

    avg_listening_time_per_user is derived (not a separate aggregation) to
    ensure consistency with the other two figures.
    """
    genre_kpis = (
        enriched_df
        .groupBy("date", "track_genre")
        .agg(
            F.count("*").alias("listen_count"),
            F.approx_count_distinct("user_id", rsd=0.02).alias("unique_listeners"),
            F.sum("duration_ms").alias("total_listening_time_ms"),
        )
        .withColumn(
            "avg_listening_time_per_user_ms",
            F.round(F.col("total_listening_time_ms") / F.col("unique_listeners"), 2)
        )
        # Composite key used as the DynamoDB PK
        .withColumn(
            "genre_date",
            F.concat(F.col("track_genre"), F.lit("#"), F.col("date").cast(StringType()))
        )
    )
    log_event("genre_kpis_computed", row_count=genre_kpis.count())
    return genre_kpis


def compute_top_songs_per_genre(enriched_df, top_n: int = 3):
    """
    Use a window function to rank songs within each (genre, date) partition
    by listen count, then keep only the top N.

    Window functions are more efficient than repeated groupBy + filter
    because Spark can compute all ranks in a single shuffle.
    """
    # Step 1: count plays per song per genre per day
    song_counts = (
        enriched_df
        .groupBy("date", "track_genre", "track_id", "track_name", "artists")
        .agg(F.count("*").alias("play_count"))
    )

    # Step 2: rank within each (date, genre) window, descending by play_count
    genre_date_window = Window.partitionBy("date", "track_genre").orderBy(F.desc("play_count"))
    top_songs = (
        song_counts
        .withColumn("rank", F.dense_rank().over(genre_date_window))
        .filter(F.col("rank") <= top_n)
        .withColumn(
            "genre_date",
            F.concat(F.col("track_genre"), F.lit("#"), F.col("date").cast(StringType()))
        )
    )
    log_event("top_songs_computed", top_n=top_n, row_count=top_songs.count())
    return top_songs


def compute_top_genres(genre_kpis_df, top_n: int = 5):
    """
    Rank genres per day by listen_count and keep the top N.
    We reuse the already-computed genre_kpis dataframe to avoid
    re-aggregating the raw events.
    """
    day_window = Window.partitionBy("date").orderBy(F.desc("listen_count"))
    top_genres = (
        genre_kpis_df
        .withColumn("rank", F.dense_rank().over(day_window))
        .filter(F.col("rank") <= top_n)
        .select("date", "track_genre", "listen_count", "rank")
    )
    log_event("top_genres_computed", top_n=top_n, row_count=top_genres.count())
    return top_genres


def write_parquet(df, bucket: str, prefix: str, name: str) -> str:
    """
    Write a DataFrame as Parquet and return the S3 path.
    We use a single partition to make the downstream Python Shell
    ingest job simpler (no glob needed for small daily batches).
    """
    path = f"s3://{bucket}/{prefix}/{name}/"
    df.coalesce(1).write.mode("overwrite").parquet(path)
    log_event("parquet_written", path=path)
    return path


def main():
    args         = get_args()
    sc           = SparkContext()
    glue_ctx     = GlueContext(sc)
    job          = Job(glue_ctx)
    job.init(args["JOB_NAME"], args)

    bucket       = args["raw_bucket"]
    file_key     = args["file_key"]
    exec_id      = args["execution_id"]
    output_prefix = f"{args['output_prefix']}{exec_id}"

    log_event("transform_started", bucket=bucket, file_key=file_key, execution_id=exec_id)

    # ── Read ──────────────────────────────────────────────
    streams_df = read_streams(glue_ctx, bucket, file_key)
    songs_df   = read_songs_catalog(glue_ctx, bucket)

    # ── Enrich ────────────────────────────────────────────
    enriched_df = enrich_streams(streams_df, songs_df)

    # ── Cache: enriched_df is read three times below ──────
    enriched_df.cache()

    # ── Compute KPIs ──────────────────────────────────────
    genre_kpis_df = compute_genre_kpis(enriched_df)
    top_songs_df  = compute_top_songs_per_genre(enriched_df, top_n=3)
    top_genres_df = compute_top_genres(genre_kpis_df,        top_n=5)

    # ── Write Parquet outputs ──────────────────────────────
    write_parquet(genre_kpis_df, bucket, output_prefix, "genre_kpis")
    write_parquet(top_songs_df,  bucket, output_prefix, "top_songs")
    write_parquet(top_genres_df, bucket, output_prefix, "top_genres")

    enriched_df.unpersist()

    log_event("transform_completed", execution_id=exec_id, output_prefix=output_prefix)
    logger.info("SUCCESS – transformation complete.  Output: s3://%s/%s/", bucket, output_prefix)

    job.commit()


if __name__ == "__main__":
    main()
