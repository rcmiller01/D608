Build & Grade Guide for Sparkify Airflow Project
Purpose

This file instructs AI coding assistants (e.g., GitHub Copilot Chat) how to complete and validate the Data Pipelines with Airflow (Sparkify) project end‑to‑end. It encodes the rubric into concrete tasks, paths, commands, and checks.

Project Brief


Begin_execution
    ├─> Stage_events
    └─> Stage_songs
           \           /
            > Load_songplays_fact_table
                       ├─> Load_user_dim_table
                       ├─> Load_song_dim_table
                       ├─> Load_artist_dim_table
                       └─> Load_time_dim_table
                                    └─> Run_data_quality_checks

Goal: COPY JSON from S3 → Redshift staging tables using parameters and hooks.

Requirements

Build COPY SQL dynamically from params:

table, s3_bucket, s3_key (templated), region, copy_json_option, iam_role_arn

Use PostgresHook(postgres_conn_id=redshift_conn_id) to run SQL.

template_fields = ("s3_key",) to support backfills by execution date ({{ ds }} etc.).

Log start, SQL, and completion.

Optional: truncate=True to TRUNCATE before load.

Example Param Usage in DAG

s3_bucket="{{ var.value.sparkify_bucket }}"

copy_json_option="auto" or path to JSONPath file in your bucket

2) Fact Operator (load_fact.py)

Goal: Run provided insert SQL to populate the songplays fact.

Log before/after.

3) Dimension Operator (load_dimensions.py)

Goal: Populate users, songs, artists, time with insert‑delete (truncate‑insert) or append.

Requirements

Params: redshift_conn_id, table, sql, mode="truncate-insert" | "append"

If truncate-insert, run TRUNCATE <table> first.

Execute via PostgresHook.

Log actions and row effects (if feasible).

4) Data Quality Operator (data_quality.py)

Goal: Run parametrized test list; raise on failure.

Requirements

Input: tests=[{"sql": "...", "expected": <value>, "cmp": "eq|gt|ge|ne"}]

For each test:

Execute SQL via PostgresHook.get_first

Compare first cell to expected using cmp

If mismatch ⇒ raise ValueError(...) (triggers Airflow retries/failed run)

Log each test, observed result, expectation, and pass/fail.

Suggested Baseline Tests

SELECT COUNT(*) FROM songplays; is > 0

SELECT COUNT(*) FROM users WHERE userid IS NULL; is 0

SELECT COUNT(*) FROM songs WHERE songid IS NULL; is 0

SELECT COUNT(*) FROM artists WHERE artistid IS NULL; is 0

SELECT COUNT(*) FROM time; is > 0

DAG Implementation Steps (Copilot Tasks)

Wire default_args, schedule, and catchup

Set keys exactly as in the DAG contract.

Instantiate operators

Stage_events → target table staging_events

Stage_songs → target table staging_songs

Load_songplays_fact_table with SqlQueries.songplay_table_insert

Four dimensions with corresponding SqlQueries.*_table_insert

Set dependencies to match the graph contract.

Parameterize all operators (no hard‑coded S3 paths or SQL inside the DAG).

Logging in all operators at start, during SQL build/exec, and on completion.

Commands & Smoke Tests

Start/Verify Services (workspace)

pwd                                    # expect /home/workspace
/opt/airflow/start-services.sh          # Postgres & Cassandra
/opt/airflow/start.sh                   # Airflow webserver + scheduler
ps aux | grep airflow                   # verify daemons
airflow users create --email s@example.com --firstname a --lastname b --password admin --role Admin --username admin
airflow dags list


Run DAG once (UI or CLI)

airflow dags trigger sparkify_etl


Redshift sanity queries (Query Editor v2)

SELECT COUNT(*) FROM staging_events;
SELECT COUNT(*) FROM staging_songs;
SELECT COUNT(*) FROM songplays;


Expected

Staging tables populated

songplays rowcount > 0

Data Quality task passes

Acceptance Criteria (Definition of Done)

DAG imports cleanly; no import errors.

DAG visible in UI and scheduled @hourly; catchup=False.

Dependencies exactly match the provided graph.

Stage operator builds dynamic COPY SQL using params, uses hooks, logs steps.

Fact/Dimension operators use params; dimension supports append & truncate‑insert.

Data quality operator accepts param list of tests and raises on failure; DAG retries on failure.

All of the above align with the formal rubric.

Guardrails & Constraints

Region alignment: S3 bucket must be in the same region as the Redshift workgroup.

Prefer IAM role on Redshift Serverless over embedding AWS keys.

Avoid hard‑coding secrets, ARNs, or bucket names; consume from Connections/Variables.

Keep operators generic & reusable (no Sparkify‑specific table names inside operators).

Common Failure Modes & Fix Hints

COPY access denied
Check Redshift workgroup’s IAM role includes s3:GetObject on s3://<bucket>/* and s3:ListBucket.

Scheduler warning in UI (“scheduler not running”)
Run airflow scheduler in the terminal.

No data in fact table
Inspect join conditions in SqlQueries.songplay_table_insert; verify staging tables loaded and event logs filtered correctly.

DAG stuck
Inspect Task Instance logs. The operators must log the rendered S3 path and the exact COPY statement (minus secrets).

Copilot Chat Prompts (use inside the repo)

Implement Stage Operator

Implement a StageToRedshiftOperator in plugins/final_project_operators/stage_redshift.py that uses PostgresHook to run a Redshift COPY built from params: table, s3_bucket, s3_key (templated), copy_json_option, region, iam_role_arn. Add template_fields = ("s3_key",), log all steps, and support an optional truncate flag.

Implement Fact & Dimension Operators

In load_fact.py and load_dimensions.py, create operators that execute provided SQL via PostgresHook. Dimension operator must support mode="truncate-insert" and mode="append". Add logging.

Implement Data Quality Operator

In data_quality.py, create an operator that takes tests=[{"sql": str, "expected": Any, "cmp": "eq|gt|ge|ne"}], executes each SQL, compares the first cell to the expected value using cmp, logs, and raises ValueError on mismatch.

Wire DAG

In dags/.../final_project.py, set default_args, @hourly, catchup=False. Instantiate operators with Airflow Variables (sparkify_bucket, redshift_role_arn) and set dependencies to match the graph. No hard‑coded secrets.

Add Minimal DQ Tests

Add DQ tests that ensure non‑zero songplays rows and zero NULL primary keys in dimensions.

Code Style & Commits

Keep operators parameterized, SQL in helper, values injected via DAG.

Commit messages:

feat(operator): add templated StageToRedshiftOperator with COPY params

feat(dag): wire hourly schedule and full dependency graph

feat(quality): parametrized data quality checks + failure raising

chore: add Airflow Variables & connection notes to README

Extension Ideas (optional, nice‑to‑have)

Add S3 existence checks before COPY (proactive monitoring).

Track task runtimes and alert on anomalies.

Support JSONPath selector for events vs songs as a parameter.

Final Submission Pack

airflow/dags/.../final_project.py

airflow/plugins/final_project_operators/*.py

This agents.md

A short README.md with:

How to set Connections/Variables

How to trigger the DAG

Expected DQ tests