
from datetime import timedelta
from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from operators.stage_redshift import StageToRedshiftOperator
from operators.load_fact import LoadFactOperator
from operators.load_dimension import LoadDimensionOperator
from operators.data_quality import DataQualityOperator
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}

with DAG(
    'final_project',
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
) as dag:

    start_operator = DummyOperator(task_id='Begin_execution')

    s3_bucket = Variable.get('s3_bucket')
    s3_log_prefix = Variable.get('s3_log_prefix')
    s3_song_prefix = Variable.get('s3_song_prefix')
    log_json_path = Variable.get('log_json_path', default_var='auto')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table='staging_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket=s3_bucket,
        s3_key=s3_log_prefix,
        json_path=log_json_path
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table='staging_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        s3_bucket=s3_bucket,
        s3_key=s3_song_prefix,
        json_path='auto'
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert,
        mode='truncate-insert'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert,
        mode='truncate-insert'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert,
        mode='truncate-insert'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert,
        mode='truncate-insert'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tests=[
            {"sql": "SELECT COUNT(*) FROM songplays", "expected": 0, "cmp": "gt"},
            {"sql": "SELECT COUNT(*) FROM users", "expected": 0, "cmp": "gt"},
            {"sql": "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", "expected": 0, "cmp": "eq"},
            {"sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "expected": 0, "cmp": "eq"},
            {"sql": "SELECT COUNT(*) FROM songs WHERE songid IS NULL", "expected": 0, "cmp": "eq"},
            {"sql": "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", "expected": 0, "cmp": "eq"},
            {"sql": "SELECT COUNT(*) FROM time WHERE start_time IS NULL", "expected": 0, "cmp": "eq"}
        ]
    )

    stop_operator = DummyOperator(task_id='Stop_execution')

    # Dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ]
    [
        load_user_dimension_table,
        load_song_dimension_table,
        load_artist_dimension_table,
        load_time_dimension_table
    ] >> run_quality_checks >> stop_operator
