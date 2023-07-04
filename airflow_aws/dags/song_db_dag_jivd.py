import pendulum

from datetime import datetime, timedelta
import os
from airflow import DAG
from airflow.decorators import dag,task

from airflow.operators.empty import EmptyOperator
#from custom_operators import (StageToRedshiftOperator, LoadFactOperator,
#                                LoadDimensionOperator, DataQualityOperator)

from plugins.customOperators import StageToRedshiftOperator
from plugins.customOperators import LoadFactOperator
from plugins.customOperators import LoadDimensionOperator
from plugins.customOperators import DataQualityOperator

#from helpers import SqlQueries

default_args = {
    'description': 'A dag to process song data events with Airflow and Redshift',
    'owner': 'jivd',
    'start_date':pendulum.datetime(2018, 11, 2, 0, 0, 0, 0),
    #'end_date':pendulum.datetime(2018, 11, 3, 0, 0, 0, 0),
    'schedule_interval': timedelta(minutes=5),
    'max_active_runs': 1,
    #'provide_context' : True,
    'catchup' : False,
    'email_on_retry': False,
    'retries': 3
}

@dag(default_args= default_args)
def song_db_dag(**kwargs):
    start_operator = EmptyOperator(task_id='Begin_execution')

    # The following code will load trips data from S3 to RedShift. Use the s3_key
    # "/log-data/... or the s3_key /song-data/... -events.json" and the s3_bucket 
    # "rawsongdb"
    
    #s3_key = "log-data/2018/11/{{ds}}-events.json"
    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        table="staging_events",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="rawsongdb",
        s3_key= "log-data",
        json_path = "s3://rawsongdb/log_json_path.json",
        execution_date= "{{ds}}"
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        table="staging_songs",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        s3_bucket="rawsongdb",
        s3_key="song-data/B/",
        json_path = "",
        execution_date= "{{ds}}"
    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        table="factsongplays",
        redshift_conn_id="redshift",
        aws_credentials_id="aws_credentials",
        functionality="delete-load"
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_users_dim_table',
        table="dimusers",
        redshift_conn_id="redshift",
        functionality ="delete-load"
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_songs_dim_table',
        table="dimsongs",
        redshift_conn_id="redshift",
        functionality ="delete-load"
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artists_dim_table',
        table="dimartists",
        redshift_conn_id="redshift",
        functionality ="delete-load"
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        table="dimtime",
        redshift_conn_id="redshift",
        functionality ="delete-load"
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id="redshift",
    )

    end_operator = EmptyOperator(task_id='Stop_execution')

    start_operator >> stage_events_to_redshift
    start_operator >> stage_songs_to_redshift

    stage_events_to_redshift >> load_songplays_table
    stage_songs_to_redshift >> load_songplays_table

    load_songplays_table >> load_song_dimension_table
    load_songplays_table >> load_user_dimension_table
    load_songplays_table >> load_artist_dimension_table
    load_songplays_table >> load_time_dimension_table

    load_song_dimension_table >> run_quality_checks
    load_user_dimension_table >> run_quality_checks
    load_artist_dimension_table >> run_quality_checks
    load_time_dimension_table >> run_quality_checks

    run_quality_checks >> end_operator

song_db_dag = song_db_dag()
