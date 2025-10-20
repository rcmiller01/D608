# Data Pipelines with Airflow (Udacity DEND) — Redshift + S3

This repository contains an Airflow DAG and custom operators to build a star-schema pipeline on Amazon Redshift using data staged from S3.

## Project Structure

```
cd12380-data-pipelines-with-airflow-main/
├─ dags/
│  └─ final_project.py
├─ plugins/
│  ├─ helpers/
│  │  └─ sql_queries.py
│  └─ operators/
│     ├─ stage_redshift.py
│     ├─ load_fact.py
│     ├─ load_dimension.py
│     └─ data_quality.py
└─ create_tables.sql
```

## What the DAG Does

1. **Stage** raw JSON from S3 to Redshift staging tables (events, songs) using a parametrized COPY.
2. **Load** the **fact** table (`songplays`) via a SQL transform.
3. **Load** **dimension** tables (`users`, `songs`, `artists`, `time`) with an append or truncate-insert mode.
4. **Check** data quality using parametrized tests.
5. Runs **hourly** and does not depend on past, with retries configured per rubric.

The task dependency graph is:

```
Begin_execution
  ├─> Stage_events
  └─> Stage_songs
Stage_events & Stage_songs
  └─> Load_songplays_fact_table
Load_songplays_fact_table
  ├─> Load_user_dim_table
  ├─> Load_song_dim_table
  ├─> Load_artist_dim_table
  └─> Load_time_dim_table
All dimension loads
  └─> Run_data_quality_checks
Run_data_quality_checks
  └─> Stop_execution
```

## Airflow Setup

### Connections (Admin → Connections)

- **redshift**: `Postgres` connection to your Redshift endpoint (host, db, user, password, port 5439).
- **aws_credentials**: `Amazon Web Services` connection with Access Key and Secret (or use IAM role on workers).

### Variables (Admin → Variables)

- `s3_bucket` — your bucket, e.g. `my-d608-bucket`
- `s3_log_prefix` — e.g. `log-data`
- `s3_song_prefix` — e.g. `song-data`
- `log_json_path` — e.g. `s3://my-d608-bucket/log_json_path.json`
- (optional) `region` — AWS region string

## S3 Data (copy once)

```bash
# Create your bucket (unique name)
aws s3 mb s3://<your-unique-bucket>

# Copy Udacity data into your account
aws s3 cp s3://udacity-dend/log-data/  ~/log-data/  --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
aws s3 cp s3://udacity-dend/log_json_path.json ~/

# Move to your bucket (ideally same region as your Redshift workgroup)
aws s3 cp ~/log-data/  s3://<your-unique-bucket>/log-data/  --recursive
aws s3 cp ~/song-data/ s3://<your-unique-bucket>/song-data/ --recursive
aws s3 cp ~/log_json_path.json s3://<your-unique-bucket>/
```

## Running Locally with Airflow

- Put this repository under your Airflow `dags/` and `plugins/` directories (or use docker-compose if provided).
- Start Airflow, open the UI, enable the `final_project` DAG.
- Create **Connections** and **Variables** as above.
- Trigger the DAG.

## Operator Notes (Rubric Alignment)

- **StageToRedshiftOperator** (`plugins/operators/stage_redshift.py`)
  - Uses **template_fields** for `s3_key` and builds a dynamic **COPY** with params.
  - Uses **PostgresHook** (Redshift) and **AwsHook** (credentials).
  - Logs before and after COPY.

- **LoadFactOperator** (`plugins/operators/load_fact.py`)
  - Appends into fact table using provided SQL (from `helpers/sql_queries.py`).

- **LoadDimensionOperator** (`plugins/operators/load_dimension.py`)
  - Supports `mode="append"` or `mode="truncate-insert"` (default).
  - On `truncate-insert`, deletes from target before insert.

- **DataQualityOperator** (`plugins/operators/data_quality.py`)
  - Accepts tests: list of `{sql, expected, cmp}` and raises on mismatch.

## DAG Configuration (final_project.py)

- `default_args` include: `owner`, `depends_on_past=False`, `start_date`, `retries=3`, `retry_delay=5min`, `catchup=False`, `email_on_retry=False`.
- **Schedule**: hourly (`0 * * * *`).

## Verifying Against the Rubric

- UI imports without errors and the graph matches the diagram above.
- Logs show staging COPY operations with the expected S3 locations.
- Fact and dimension loads complete; switch dimension mode via operator param.
- Data quality tests run and the DAG fails if a test fails.

## Troubleshooting

- **Permissions**: If COPY fails, verify the `aws_credentials` connection/IAM role permissions (`s3:GetObject`) and Redshift IAM role.
- **Paths**: Ensure Airflow Variables match your bucket/prefixes exactly.
- **Region**: Keep data bucket and Redshift in the same region to avoid cross-region headaches.
