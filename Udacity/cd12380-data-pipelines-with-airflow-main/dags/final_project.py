from datetime import timedelta
import pendulum
from airflow.decorators import dag
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'start_date': pendulum.now(),
    'depends_on_past': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'email_on_retry': False
}


@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule_interval='0 * * * *',
    catchup=False
)
def final_project():

    start_operator = DummyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_events',
        s3_bucket=Variable.get('s3_bucket'),
        s3_key='log_data/{execution_date.year}/{execution_date.month}',
        region='us-east-1',
        copy_json_option='s3://udacity-dend/log_json_path.json',
        iam_role_arn=Variable.get('redshift_iam_role', default_var='')
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        aws_credentials_id='aws_credentials',
        table='staging_songs',
        s3_bucket=Variable.get('s3_bucket'),
        s3_key='song_data',
        region='us-east-1',
        copy_json_option='auto',
        iam_role_arn=Variable.get('redshift_iam_role', default_var='')
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
            {
                'sql': 'SELECT COUNT(*) FROM songplays',
                'expected': 0,
                'cmp': 'gt'
            },
            {
                'sql': 'SELECT COUNT(*) FROM users WHERE userid IS NULL',
                'expected': 0,
                'cmp': 'eq'
            },
            {
                'sql': 'SELECT COUNT(*) FROM songs WHERE songid IS NULL',
                'expected': 0,
                'cmp': 'eq'
            },
            {
                'sql': 'SELECT COUNT(*) FROM artists WHERE artistid IS NULL',
                'expected': 0,
                'cmp': 'eq'
            },
            {
                'sql': 'SELECT COUNT(*) FROM time',
                'expected': 0,
                'cmp': 'gt'
            }
        ]
    )

    stop_operator = DummyOperator(task_id='Stop_execution')

    # Task dependencies
    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]
    
    [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    
    load_songplays_table >> [
        load_user_dimension_table, load_song_dimension_table,
        load_artist_dimension_table, load_time_dimension_table
    ]
    
    [
        load_user_dimension_table, load_song_dimension_table,
        load_artist_dimension_table, load_time_dimension_table
    ] >> run_quality_checks >> stop_operator


final_project_dag = final_project()
