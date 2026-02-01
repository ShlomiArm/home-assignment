# NOAA PRECIP_15 → Apache Iceberg Pipeline

This project implements a **batch data pipeline** that ingests historical precipitation data from the **NOAA PRECIP_15 dataset**, validates and normalizes it using **PySpark**, and writes the results directly into **Apache Iceberg tables** stored on **S3-compatible storage (MinIO)**.

The pipeline is designed to be **idempotent**, **schema-aware**, and runnable via `spark-submit`.

---

## High-Level Architecture

```
NOAA API
   |
   v
NOAAClient (HTTP + retries)
   |
   v
PySpark Batch Pipeline
   |
   |-- Validation & Normalization
   |-- Bad-record isolation
   |-- Incremental aggregation
   v
Apache Iceberg (MinIO / S3)
```

---

## Data Model & Tables

All tables are created automatically during `init()`.

### 1. Good Records Table
**`<catalog>.<db>.good`**

Stores validated and normalized precipitation observations.

Columns:
- `eventtime` (TIMESTAMP)
- `datatype` (STRING)
- `station` (STRING)
- `attributes_arr` (ARRAY<STRING>)
- `value` (INT)
- `date_epoch_ms` (BIGINT)
- `ingestion_ts` (BIGINT)

Partitioning:
- `days(eventtime)`

---

### 2. Bad Records Table
**`<catalog>.<db>.bad`**

Stores rows that failed validation, along with the failure reason.

Columns:
- `date` (STRING)
- `datatype` (STRING)
- `station` (STRING)
- `attributes` (STRING)
- `value` (INT)
- `bad_reason` (STRING)
- `ingestion_ts` (BIGINT)

---

### 3. Missing Metrics Table
**`<catalog>.<db>.missing_metrics`**

Incremental per-station quality metrics.

Columns:
- `station`
- `total_observations`
- `missing_observations`
- `missing_pct`
- `updated_at_ms`

Missing values are defined as `value = 99999`.

---

### 4. Pipeline State Table
**`<catalog>.<db>.pipeline_state`**

Tracks ingestion watermark to ensure incremental transforms.

Columns:
- `pipeline_name`
- `last_ingestion_ts_ms`
- `updated_at_ms`

---

## Validation & Normalization Rules

Applied during ingestion:

1. Parse `date` into a proper timestamp (`eventtime`)
2. Convert timestamps to **epoch milliseconds**
3. Split comma-separated `attributes` into arrays
4. Add ingestion timestamp
5. Validate:
   - Required fields present
   - Timestamp parseable
   - `value >= 0`

Valid rows → **good table**
Invalid rows → **bad table**

---

## Configuration

### Environment Variable
```bash
  NOAA_TOKEN: ${NOAA_TOKEN}
  PIPELINE_START_DAY: ${PIPELINE_START_DAY:-2010-05-01}
  PIPELINE_END_DAY: ${PIPELINE_END_DAY:-2010-05-31}
```

- **`NOAA_TOKEN`** *(required)*
  NOAA API authentication token.
  This variable has **no default** and must be provided by the user.

- **`PIPELINE_START_DAY`** *(optional, default: `2010-05-01`)*
  Start date for ingestion, in `YYYY-MM-DD` format.

- **`PIPELINE_END_DAY`** *(optional, default: `30`)*
  End date for ingestion, in `YYYY-MM-DD` format.

### Key Config Files
- `config.py`
  - `NOAAConfig`: API settings, token, retries
  - `PipelineConfig`: table names, chunk size, retention
- `client.py`: NOAA API client with retries & pagination
- `pipeline.py`: core pipeline logic
- `utils.py`: date range utilities

---

## How to Run

### 1. Start Required Services
Make sure the following are running:
- MinIO (S3-compatible storage)
- Iceberg REST Catalog

### 2. Run the Pipeline
```bash
docker-compose build

docker-compose -f datalake/trino/docker-compose.yaml up

PIPELINE_START_DAY = <OPTIONAL. Default is `2010-05-01`> -e PIPELINE_END_DAY = <OPTIONAL. Default is `2010-05-31`> docker-compose up
```

Default behavior in `main.py`:
- Initializes Iceberg schema
- Ingests **30 days starting from 2010-05-01**
- Runs transformation logic

---

## Pipeline Stages

### init()
Creates schemas and Iceberg tables if they do not exist.

### ingest(startdate, totaldays)
- Fetches NOAA data in **non-overlapping date chunks**
- Validates & normalizes records
- Writes:
  - good data → overwrite partitions
  - bad data → append

### transform()
- Reads new records since last watermark
- Updates missing-value metrics incrementally
- Advances watermark

### maintain() (optional)
- Compacts small Iceberg files
- Expires old snapshots

---

## Design Notes

- Uses **overwritePartitions()** to guarantee idempotent writes
- Chunked ingestion prevents memory pressure
- Retry-enabled HTTP client for NOAA API limits
- Iceberg used for ACID guarantees and schema evolution


## ToDo (did not had the time...)
 - Sanity checks:
    - check for missing days
    - compare num of rows per day to Noaa num of rows
    - check for value range
    - check that the pipeline state last update is up to date
    - check no records in the bad table
