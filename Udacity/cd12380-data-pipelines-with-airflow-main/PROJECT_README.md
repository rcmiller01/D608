# Data Pipelines with Airflow - Udacity Project

## Overview

This project implements an ETL pipeline using Apache Airflow to process Sparkify music streaming data. The pipeline extracts data from S3, stages it in Amazon Redshift, transforms it into a star schema, and runs data quality checks.

## Project Structure

```
├── dags/
│   └── final_project.py          # Main DAG file
├── plugins/
│   ├── operators/
│   │   ├── stage_redshift.py     # Staging operator
│   │   ├── load_fact.py          # Fact table loading operator
│   │   ├── load_dimension.py     # Dimension table loading operator
│   │   └── data_quality.py       # Data quality checking operator
│   └── helpers/
│       └── sql_queries.py        # SQL query definitions
├── create_tables.sql             # DDL for Redshift tables
└── PROJECT_README.md             # This file
```

## Rubric Compliance Checklist

### ✅ General Requirements
- [x] **Imports clean / DAG browsable**: All imports are properly organized
- [x] **Correct task dependencies**: Begin_execution → Stage_* → Load_songplays → Load_*dims → DataQuality → Stop_execution
- [x] **Start and end tasks present**: DummyOperator tasks for Begin_execution and Stop_execution

### ✅ DAG Configuration
- [x] **default_args**: Contains owner, start_date, depends_on_past, retries, retry_delay, catchup, email_on_retry
- [x] **Hourly schedule**: Set to '0 * * * *' (hourly at minute 0)

### ✅ Staging
- [x] **Custom Redshift stage operator**: StageToRedshiftOperator implemented
- [x] **Dynamic COPY with params**: Uses templated s3_key field
- [x] **Hooks/connections + logging**: PostgresHook and comprehensive logging

### ✅ Fact & Dimensions
- [x] **Fact loaded via LoadFactOperator**: songplays table using custom operator
- [x] **Dimensions via LoadDimensionOperator**: users, songs, artists, time tables
- [x] **Append vs truncate-insert switch**: mode="truncate-insert" parameter
- [x] **Parameterized, no static SQL**: SQL passed via helpers.SqlQueries

### ✅ Data Quality
- [x] **DataQualityOperator**: Accepts parameterized test cases
- [x] **Raises on mismatch**: Throws ValueError on test failures
- [x] **Multiple test cases**: Includes null checks and row count validations

## Setup Instructions

### 1. Airflow Variables

Configure these variables in Airflow UI (Admin → Variables):

```
s3_bucket: your-unique-bucket-name
s3_log_prefix: log-data
s3_song_prefix: song-data
log_json_path: s3://your-bucket/log_json_path.json
redshift_iam_role: arn:aws:iam::your-account:role/RedshiftRole
```

### 2. Airflow Connections

Configure these connections in Airflow UI (Admin → Connections):

**aws_credentials**
- Connection Type: Amazon Web Services
- Login: Your AWS Access Key ID
- Password: Your AWS Secret Access Key

**redshift**
- Connection Type: Postgres
- Host: your-redshift-cluster.region.redshift.amazonaws.com
- Database: your-database-name
- Login: your-username
- Password: your-password
- Port: 5439

### 3. S3 Data Setup

Copy the Udacity datasets to your S3 bucket:

```bash
# Create your bucket (must be globally unique)
aws s3 mb s3://your-unique-bucket-name

# Copy from Udacity to your CloudShell home
aws s3 cp s3://udacity-dend/log-data/ ~/log-data/ --recursive
aws s3 cp s3://udacity-dend/song-data/ ~/song-data/ --recursive
aws s3 cp s3://udacity-dend/log_json_path.json ~/

# Copy from home to your bucket
aws s3 cp ~/log-data/ s3://your-unique-bucket-name/log-data/ --recursive
aws s3 cp ~/song-data/ s3://your-unique-bucket-name/song-data/ --recursive
aws s3 cp ~/log_json_path.json s3://your-unique-bucket-name/

# Verify the copy
aws s3 ls s3://your-unique-bucket-name/log-data/ | head
aws s3 ls s3://your-unique-bucket-name/song-data/ | head
aws s3 ls s3://your-unique-bucket-name/log_json_path.json
```

### 4. Redshift Setup

1. Create a Redshift cluster
2. Run the SQL commands in `create_tables.sql` to create the required tables
3. Create an IAM role for Redshift with S3 read access
4. Attach the role to your Redshift cluster

## DAG Workflow

1. **Begin_execution**: DummyOperator start task
2. **Stage_events & Stage_songs**: Parallel staging from S3 to Redshift
3. **Load_songplays_fact_table**: Load fact table from staged data
4. **Load dimension tables**: Parallel loading of users, songs, artists, time dimensions
5. **Run_data_quality_checks**: Validate data integrity with multiple tests
6. **Stop_execution**: DummyOperator end task

## Data Quality Tests

The pipeline includes comprehensive data quality checks:

1. **Row Count Validation**: Ensures fact and dimension tables have data
2. **Null Key Validation**: Checks for NULL values in primary key columns
3. **Configurable Tests**: Easy to add new quality tests via the tests parameter

## Operators

### StageToRedshiftOperator
- Loads JSON data from S3 to Redshift staging tables
- Supports IAM role-based authentication
- Uses templated S3 keys for dynamic file paths
- Includes proper error handling and logging

### LoadFactOperator
- Loads data into fact tables using INSERT statements
- Supports append-only mode for fact tables
- Uses parameterized SQL from helpers module

### LoadDimensionOperator
- Loads data into dimension tables
- Supports both truncate-insert and append modes
- Configurable via mode parameter

### DataQualityOperator
- Runs configurable data quality tests
- Supports multiple comparison operators (eq, gt, ge, lt, le, ne)
- Fails the DAG if any test fails
- Provides detailed logging for troubleshooting

## Running the DAG

1. Ensure all connections and variables are configured
2. Place the DAG file in your Airflow dags folder
3. Place the plugins in your Airflow plugins folder
4. Enable the DAG in the Airflow UI
5. Trigger a manual run or wait for the hourly schedule

## Monitoring

- Monitor DAG runs in the Airflow UI
- Check logs for each task to debug issues
- Use the Graph View to visualize task dependencies
- Monitor data quality test results in task logs

## Troubleshooting

**Common Issues:**
1. **S3 Access Denied**: Check IAM role permissions and S3 bucket policies
2. **Redshift Connection Failed**: Verify connection parameters and security groups
3. **Data Quality Failures**: Check the specific test that failed in the logs
4. **Import Errors**: Ensure plugins are in the correct directory structure

**Log Locations:**
- Task logs available in Airflow UI
- Detailed SQL execution logs in each operator
- Connection and authentication logs in Airflow scheduler logs