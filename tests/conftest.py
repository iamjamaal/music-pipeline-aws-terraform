"""
Shared pytest configuration.

awsglue is mocked at module level here so that any test file can
import glue_jobs/*.py without needing the Glue runtime installed.
glue_jobs/ is added to sys.path so imports like `from validate import ...`
work without package qualification.
"""

import sys
import os
from pathlib import Path
from unittest.mock import MagicMock

# ── 1. Mock awsglue before any glue_jobs module is imported ──
for _mod in ["awsglue", "awsglue.utils", "awsglue.context", "awsglue.job"]:
    sys.modules[_mod] = MagicMock()

# ── 2. Add glue_jobs/ to the import path ─────────────────────
_GLUE_JOBS = Path(__file__).parent.parent / "glue_jobs"
sys.path.insert(0, str(_GLUE_JOBS))

# ── 3. Fake AWS credentials so boto3 never tries to reach AWS ─
os.environ.setdefault("AWS_ACCESS_KEY_ID",     "testing")
os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
os.environ.setdefault("AWS_SECURITY_TOKEN",    "testing")
os.environ.setdefault("AWS_SESSION_TOKEN",     "testing")
os.environ.setdefault("AWS_DEFAULT_REGION",    "us-east-1")

import pytest


@pytest.fixture(scope="session")
def spark():
    """Local SparkSession for transform unit tests.  Session-scoped to avoid JVM restart overhead."""
    try:
        from pyspark.sql import SparkSession
        session = (
            SparkSession.builder
            .master("local[1]")
            .appName("music-pipeline-unit-tests")
            .config("spark.sql.shuffle.partitions", "1")
            .config("spark.ui.enabled", "false")
            .getOrCreate()
        )
        session.sparkContext.setLogLevel("ERROR")
    except Exception as exc:
        pytest.skip(f"PySpark unavailable in this environment: {exc}")
        return
    yield session
    session.stop()
