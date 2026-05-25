# Music Streaming Data Pipeline
## Architecture, Setup, and Operations Guide

---

## Table of Contents

1. [Overview](#overview)
2. [Architecture](#architecture)
3. [Repository Structure](#repository-structure)
4. [Data Model](#data-model)
5. [Infrastructure Setup](#infrastructure-setup)
6. [CI/CD Setup](#cicd-setup)
7. [Running the Pipeline Manually](#running-the-pipeline-manually)
8. [DynamoDB Query Reference](#dynamodb-query-reference)
9. [Monitoring and Troubleshooting](#monitoring-and-troubleshooting)
10. [Cost Estimates](#cost-estimates)

---

## Overview

This pipeline ingests music streaming events that arrive in S3 as batch CSV files at irregular intervals, joins them with a songs catalog, and computes five daily KPI datasets that are stored in DynamoDB for sub-millisecond lookups by downstream applications.

The core design decision is to separate infrastructure management (Terraform) from application code deployment (GitHub Actions), so a hot-fix to a Glue script never triggers a full infrastructure re-plan, and an infrastructure change never accidentally overwrites scripts that running Glue jobs depend on.

---

## Architecture

```
S3 raw/streams/*.csv
        │  (EventBridge S3 Object Created)
        ▼
┌───────────────────────────────────────────────────┐
│            AWS Step Functions                      │
│                                                   │
│  [ValidateInput]──fail──▶[NotifyValidationFailure]│
│        │                                          │
│  [TransformData]──fail──▶[NotifyTransformFailure] │
│        │                                          │
│  [IngestToDynamoDB]─fail▶[NotifyIngestFailure]    │
│        │                                          │
│  [ArchiveFiles]───warn──▶[NotifyArchiveFailure]   │
│        │                                          │
│  [PipelineSucceeded]                              │
└───────────────────────────────────────────────────┘
        │
        ├── AWS Glue (PySpark)  →  S3 Parquet (intermediate)
        ├── AWS Glue (PyShell)  →  DynamoDB KPI tables
        └── AWS Glue (PyShell)  →  S3 archive bucket
```

**Why Step Functions + Glue rather than a single Lambda or Airflow?**

Step Functions with the `.sync:2` Glue integration means AWS manages polling — your code never blocks a thread waiting for a Spark job to finish. Retries and error routing are declared in JSON rather than written as try/except logic, making the failure handling auditable by non-engineers. And Glue auto-scales the Spark cluster, so you never need to size an EMR cluster for peak load.

---

## Repository Structure

```
music-pipeline/
├── terraform/                  # All AWS infrastructure as code
│   ├── main.tf                 # Root: wires all modules together
│   ├── variables.tf            # Environment, region inputs
│   ├── outputs.tf              # Bucket names, table names, SFN ARN
│   └── modules/
│       ├── s3/                 # raw, archive, scripts buckets
│       ├── dynamodb/           # 4 KPI tables with TTL + PITR
│       ├── iam/                # Glue and SFN least-privilege roles
│       ├── glue/               # 4 Glue job definitions
│       └── step_functions/     # State machine + EventBridge trigger
├── glue_jobs/
│   ├── validate.py             # Stage 1: column/date/row checks
│   ├── transform.py            # Stage 2: PySpark KPI computation
│   ├── ingest.py               # Stage 3: DynamoDB batch writes
│   └── archive.py              # Stage 4: S3 copy-then-delete
├── step_functions/
│   └── state_machine.json      # ASL definition (all states + retries)
├── scripts/
│   ├── poll_execution.py       # CI helper: waits for SFN to finish
│   └── assert_dynamo.py        # CI helper: confirms DDB items written
├── .github/workflows/
│   ├── terraform.yml           # Plan on PR, Apply on merge to main
│   ├── deploy_scripts.yml      # Upload glue_jobs/ to S3 on change
│   └── integration_test.yml    # End-to-end smoke test after deploy
└── docs/
    └── README.md               # This file
```

---

## Data Model

### Source Data

The pipeline joins two source datasets on `track_id`:

**Streams** (`streams*.csv`): user play events with 3 columns — `user_id` (int), `track_id` (string), `listen_time` (timestamp). Each row is one song play by one user.

**Songs Catalog** (`songs.csv`): 89,741 tracks with 21 columns including `track_id`, `track_genre`, `duration_ms`, `track_name`, and `artists`. This is the reference dataset — it does not change with every batch.

### DynamoDB Tables

All tables use `PAY_PER_REQUEST` billing because stream files arrive at unpredictable intervals. Provisioned throughput would waste money during quiet periods and throttle during spikes. All tables have a 1-year TTL and Point-In-Time Recovery enabled.

**`music-pipeline-{env}-genre-kpis`**

Stores four metrics per genre per day as separate items. The reason for separate items (rather than four attributes on one item) is that it makes it trivially easy to add a new metric later — you simply write a new `metric` SK value without migrating existing data.

| Attribute     | Type   | Example                   |
|---------------|--------|---------------------------|
| `genre_date`  | PK (S) | `"acoustic#2024-06-25"`   |
| `metric`      | SK (S) | `"listen_count"`          |
| `value`       | N      | `4201`                    |
| `genre`       | S      | `"acoustic"`              |
| `date`        | S      | `"2024-06-25"`            |
| `expires_at`  | N      | Unix TTL timestamp        |

**`music-pipeline-{env}-top-songs`**

Stores the top 3 songs per genre per day. Rank is the sort key so you can retrieve all three with a single Query (no filtering needed).

| Attribute    | Type   | Example                   |
|--------------|--------|---------------------------|
| `genre_date` | PK (S) | `"pop#2024-06-25"`        |
| `rank`       | SK (S) | `"1"`                     |
| `track_id`   | S      | `"5SuOikwiRyPMVoIQDJUgSV"`|
| `track_name` | S      | `"Comedy"`                |
| `artists`    | S      | `"Gen Hoshino"`           |
| `play_count` | N      | `312`                     |

**`music-pipeline-{env}-top-genres`**

Stores the top 5 genres per day. Simple to query — give me the date, get back 5 items in rank order.

| Attribute      | Type   | Example          |
|----------------|--------|------------------|
| `date`         | PK (S) | `"2024-06-25"`   |
| `rank`         | SK (S) | `"1"`            |
| `genre`        | S      | `"pop"`          |
| `listen_count` | N      | `8740`           |

---

## Infrastructure Setup

### Prerequisites

You need the AWS CLI configured with credentials that have permissions to create S3, DynamoDB, IAM, Glue, Step Functions, EventBridge, and CloudWatch resources. Terraform 1.6+ must be installed locally.

### Step 1: Bootstrap the Terraform state backend

Before the first `terraform init`, you need an S3 bucket and DynamoDB table for Terraform's remote state. This is a one-time manual step because Terraform cannot manage its own state backend bootstrapping.

```bash
# Create the state bucket (must be globally unique)
aws s3api create-bucket \
  --bucket music-pipeline-tfstate \
  --region us-east-1

# Enable versioning so you can recover from a bad state file
aws s3api put-bucket-versioning \
  --bucket music-pipeline-tfstate \
  --versioning-configuration Status=Enabled

# Create the state lock table
aws dynamodb create-table \
  --table-name music-pipeline-tfstate-lock \
  --attribute-definitions AttributeName=LockID,AttributeType=S \
  --key-schema AttributeName=LockID,KeyType=HASH \
  --billing-mode PAY_PER_REQUEST \
  --region us-east-1
```

### Step 2: Initialise and apply Terraform

```bash
cd terraform
terraform init
terraform plan -var="environment=dev" -var="aws_region=us-east-1"
terraform apply -var="environment=dev" -var="aws_region=us-east-1"
```

After apply completes, note the outputs — they contain the exact bucket names (which include a random suffix) and the state machine ARN.

### Step 3: Upload the reference data (one-time)

The songs catalog and users file are reference data that do not change with each pipeline run. Upload them once to the raw bucket:

```bash
RAW_BUCKET=$(terraform output -raw raw_bucket_name)

aws s3 cp songs.csv  s3://${RAW_BUCKET}/songs/songs.csv
aws s3 cp users.csv  s3://${RAW_BUCKET}/users/users.csv
```

### Step 4: Upload Glue scripts

```bash
SCRIPTS_BUCKET=$(terraform output -raw scripts_bucket_name)

aws s3 sync glue_jobs/ s3://${SCRIPTS_BUCKET}/glue_jobs/ --sse AES256
```

In normal operations, the `deploy_scripts.yml` CI workflow handles this automatically on every merge to main.

---

## CI/CD Setup

### GitHub Secrets Required

Navigate to your repository → **Settings → Secrets and variables → Actions** and create these repository secrets:

| Secret Name             | Description                                      |
|-------------------------|--------------------------------------------------|
| `AWS_ACCESS_KEY_ID`     | Access key for the CI/CD IAM user                |
| `AWS_SECRET_ACCESS_KEY` | Corresponding secret                             |
| `AWS_REGION`            | e.g. `us-east-1`                                 |
| `AWS_ACCOUNT_ID`        | 12-digit AWS account ID (used for SNS ARN)       |

### How the three workflows interact

Think of the three workflows as a pipeline of gates. The `terraform.yml` workflow is the slow lane — it runs when infrastructure files change, takes 3-5 minutes, and requires a PR review before apply. The `deploy_scripts.yml` workflow is the fast lane — it runs in about 60 seconds and only uploads Python files to S3, so a bug fix in `validate.py` goes live almost immediately after merging. The `integration_test.yml` workflow runs automatically after a successful script deploy and acts as the quality gate — if the end-to-end smoke test fails, the team is immediately notified.

### GitHub Environment Protection (optional but recommended for prod)

Create a GitHub Environment named `prod` under Settings → Environments and configure it to require manual approval before the apply job runs. This creates a human gate between a merge to a release branch and actual production infrastructure changes.

---

## Running the Pipeline Manually

### Trigger via AWS Console

Navigate to **Step Functions → State machines → music-pipeline-dev → Start execution** and paste the following JSON as input, replacing the values with your actual resource names:

```json
{
  "raw_bucket":         "music-pipeline-raw-dev-{your-suffix}",
  "file_key":           "streams/streams1.csv",
  "archive_bucket":     "music-pipeline-archive-dev-{your-suffix}",
  "validate_job_name":  "music-pipeline-validate-dev",
  "transform_job_name": "music-pipeline-transform-dev",
  "ingest_job_name":    "music-pipeline-ingest-dev",
  "archive_job_name":   "music-pipeline-archive-dev",
  "genre_kpis_table":   "music-pipeline-dev-genre-kpis",
  "top_songs_table":    "music-pipeline-dev-top-songs",
  "top_genres_table":   "music-pipeline-dev-top-genres",
  "sns_topic_arn":      "arn:aws:sns:us-east-1:{account-id}:music-pipeline-alerts-dev"
}
```

### Trigger via AWS CLI

```bash
# First upload a stream file to the raw bucket
aws s3 cp streams1.csv s3://${RAW_BUCKET}/streams/streams1.csv

# The EventBridge rule fires automatically, or trigger manually:
aws stepfunctions start-execution \
  --state-machine-arn ${STATE_MACHINE_ARN} \
  --name "manual-run-$(date +%s)" \
  --input file://scripts/sample_input.json
```

---

## DynamoDB Query Reference

All queries below use the AWS CLI. Replace `{env}` with `dev`, `staging`, or `prod`.

### Query 1: All metrics for a specific genre on a specific day

This is the most common query — a business analyst wants to know how acoustic music performed on a particular day.

```bash
aws dynamodb query \
  --table-name "music-pipeline-{env}-genre-kpis" \
  --key-condition-expression "genre_date = :gd" \
  --expression-attribute-values '{":gd": {"S": "acoustic#2024-06-25"}}'
```

### Query 2: Single metric (e.g. unique_listeners) for a genre on a day

When you only need one KPI, add a condition on the sort key to avoid reading all four metric items:

```bash
aws dynamodb get-item \
  --table-name "music-pipeline-{env}-genre-kpis" \
  --key '{
    "genre_date": {"S": "pop#2024-06-25"},
    "metric":     {"S": "unique_listeners"}
  }'
```

### Query 3: Top 3 songs for a genre on a day

```bash
aws dynamodb query \
  --table-name "music-pipeline-{env}-top-songs" \
  --key-condition-expression "genre_date = :gd" \
  --expression-attribute-values '{":gd": {"S": "hip-hop#2024-06-25"}}'
```

Because rank is the sort key, DynamoDB returns the items in rank order (1, 2, 3) without any client-side sorting.

### Query 4: Top 5 genres for a specific day

```bash
aws dynamodb query \
  --table-name "music-pipeline-{env}-top-genres" \
  --key-condition-expression "#d = :date" \
  --expression-attribute-names  '{"#d": "date"}' \
  --expression-attribute-values '{":date": {"S": "2024-06-25"}}'
```

Note that `date` is a reserved word in DynamoDB expression syntax, so we use an expression attribute name `#d` as an alias. This is a common gotcha — if a query returns an error about reserved words, wrap the attribute name in `#alias`.

### Query 5: Compare listen counts across all genres for a day

This requires a scan (no direct support for "all genres on day X" without a GSI), so use it sparingly. For production workloads, consider adding a GSI on `date` to avoid full table scans.

```bash
aws dynamodb scan \
  --table-name "music-pipeline-{env}-genre-kpis" \
  --filter-expression "#d = :date AND metric = :m" \
  --expression-attribute-names  '{"#d": "date"}' \
  --expression-attribute-values '{":date": {"S": "2024-06-25"}, ":m": {"S": "listen_count"}}' \
  --projection-expression "genre, #d, #val" \
  --expression-attribute-names '{"#d": "date", "#val": "value"}'
```

---

## Monitoring and Troubleshooting

### CloudWatch Log Groups

Every component writes structured JSON logs (emitted via the `log_event()` helper in each script) to these log groups:

| Component            | Log Group                                             |
|----------------------|-------------------------------------------------------|
| Validate job         | `/aws-glue/jobs/music-pipeline-validate-{env}`        |
| Transform job        | `/aws-glue/jobs/music-pipeline-transform-{env}`       |
| Ingest job           | `/aws-glue/jobs/music-pipeline-ingest-{env}`          |
| Archive job          | `/aws-glue/jobs/music-pipeline-archive-{env}`         |
| Step Functions       | `/aws/states/music-pipeline-{env}`                    |

### CloudWatch Insights Query: find all failures in the last 24h

```
fields @timestamp, event, bucket, key, @message
| filter event like "failed" or @message like "ERROR"
| sort @timestamp desc
| limit 50
```

### Common failure modes and remedies

**Validation fails with "Missing required columns"** — The incoming CSV has a different schema than expected. Check if the upstream system changed its export format. The validate job logs the exact columns it found, so you can compare them against `REQUIRED_STREAMS_COLUMNS` in `validate.py`.

**Transform fails with "Path does not exist"** — The songs catalog has not been uploaded to `s3://{raw_bucket}/songs/songs.csv`. Follow the reference data upload step in the setup guide.

**Ingest fails with "ProvisionedThroughputExceededException"** — This should not happen because all tables use `PAY_PER_REQUEST`, but if a table was manually switched to provisioned mode, restore it to on-demand or increase the provisioned capacity.

**Archive fails with "ETag mismatch after copy"** — An extremely rare condition where S3 returned a different ETag for the destination object than the source. This could indicate S3 replication lag or a bug in the copy logic. The raw file is NOT deleted in this case, so the data is safe. Manually re-run just the archive job with the same execution_id to retry.

### Re-running a single stage

Because each Glue job is independently invocable, you can re-run any stage without re-running the whole pipeline. This is useful when the transform succeeded but the ingest failed due to a transient DynamoDB issue:

```bash
aws glue start-job-run \
  --job-name "music-pipeline-ingest-dev" \
  --arguments '{
    "--raw_bucket":        "music-pipeline-raw-dev-{suffix}",
    "--execution_id":      "my-original-exec-id",
    "--genre_kpis_table":  "music-pipeline-dev-genre-kpis",
    "--top_songs_table":   "music-pipeline-dev-top-songs",
    "--top_genres_table":  "music-pipeline-dev-top-genres"
  }'
```

---

## Cost Estimates

The following estimates assume 30 stream files per month (~11,000 rows each), which matches the three provided sample files used daily.

| Service          | Usage pattern                        | Est. monthly cost |
|------------------|--------------------------------------|-------------------|
| Glue (PySpark)   | 30 runs × 2 G.1X workers × ~5 min   | ~$4.80            |
| Glue (PyShell)   | 90 runs × 0.0625 DPU × ~1 min each  | ~$0.07            |
| Step Functions   | 30 executions × ~8 state transitions | ~$0.01            |
| DynamoDB (writes)| ~3,000 WCUs/month across 3 tables    | ~$0.04            |
| DynamoDB (reads) | Varies by application load           | ~$0.01–$1.00      |
| S3 (storage)     | ~500 MB raw + archive                | ~$0.01            |
| **Total**        |                                      | **~$5–$6/month**  |

The dominant cost is the PySpark transform job. If cost is a concern, consider replacing it with a Python Shell job using Pandas for small datasets (under ~500,000 rows/run), which would reduce the Glue cost to under $0.10/month.
