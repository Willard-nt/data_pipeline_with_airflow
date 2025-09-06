from datetime import datetime, timedelta
import pendulum
import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from operators import (StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)
from helpers import SqlQueries

default_args = {
    'owner': 'udacity',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'catchup': False,
    'start_date': pendulum.now(),
}

@dag(
    default_args=default_args,
    description='Load and transform data in Redshift with Airflow',
    schedule='0 * * * *'
)
def final_project():

    start_operator = EmptyOperator(task_id='Begin_execution')

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        create_staging_events_table="""
            DROP table IF EXISTS public.staging_events;
                CREATE TABLE public.staging_events (
	            artist varchar(256),
	            auth varchar(256),
	            firstname varchar(256),
	            gender varchar(256),
	            iteminsession int4,
	            lastname varchar(256),
	            length numeric(18,0),
	            "level" varchar(256),
	            location varchar(256),
	            "method" varchar(256),
	            page varchar(256),
	            registration numeric(18,0),
	            sessionid int4,
	            song varchar(256),
	            status int4,
	            ts int8,
	            useragent varchar(256),
	            userid int4
            );""",
                create_staging_songs_table=None

    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        create_staging_events_table=None,
        create_staging_songs_table="""
            DROP table IF EXISTS public.songs;
                CREATE TABLE public.staging_songs (
	            num_songs int4,
	            artist_id varchar(256),
	            artist_name varchar(512),
	            artist_latitude numeric(18,0),
	            artist_longitude numeric(18,0),
	            artist_location varchar(512),
	            song_id varchar(256),
	            title varchar(512),
	            duration numeric(18,0),
	            "year" int4
            );"""
                

)

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
    )

    start_operator >> [stage_events_to_redshift, stage_songs_to_redshift]

final_project_dag = final_project()

