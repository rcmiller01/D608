from datetime import timedelta
from airflow import DAG
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago
from airflow.models import Variable

from plugins.operators.stage_redshift import StageToRedshiftOperator
from plugins.operators.load_fact import LoadFactOperator
from plugins.operators.load_dimension import LoadDimensionOperator
from plugins.operators.data_quality import DataQualityOperator
from plugins.helpers.sql_queries import SqlQueries

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

    start = EmptyOperator(task_id='Begin_execution')

    s3_bucket = Variable.get('s3_bucket')                    # e.g., robert-miller
    s3_log_prefix = Variable.get('s3_log_prefix')            # e.g., log_data
    s3_song_prefix = Variable.get('s3_song_prefix')          # e.g., song_data
    log_json_path = Variable.get('log_json_path', default_var='s3://udacity-dend/log_json_path.json')
    role_arn = Variable.get('redshift_iam_role')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        table='staging_events',
        s3_bucket=s3_bucket,
        s3_key=s3_log_prefix,
        region='us-east-1',
        copy_json_option=log_json_path,   # JSONPaths for events
        iam_role_arn=role_arn,
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        table='staging_songs',
        s3_bucket=s3_bucket,
        s3_key=s3_song_prefix,
        region='us-east-1',
        copy_json_option='auto',          # songs use auto
        iam_role_arn=role_arn,
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        table='songplays',
        sql=SqlQueries.songplay_table_insert,
        append_only=False
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
            {"sql": "SELECT COUNT(*) FROM songplays WHERE playid IS NULL", "expected": 0, "cmp": "eq"},
            {"sql": "SELECT COUNT(*) FROM users WHERE userid IS NULL", "expected": 0, "cmp": "eq"},
            {"sql": "SELECT COUNT(*) FROM songplays", "expected": 0, "cmp": "gt"},
            {"sql": "SELECT COUNT(*) FROM songs WHERE songid IS NULL", "expected": 0, "cmp": "eq"},
            {"sql": "SELECT COUNT(*) FROM artists WHERE artistid IS NULL", "expected": 0, "cmp": "eq"},
            {"sql": "SELECT COUNT(*) FROM time", "expected": 0, "cmp": "gt"},
        ]
    )

    stop = EmptyOperator(task_id='Stop_execution')

    start >> [stage_events_to_redshift, stage_songs_to_redshift]
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
    ] >> run_quality_checks >> stop
