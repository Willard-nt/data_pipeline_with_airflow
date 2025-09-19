from datetime import datetime, timedelta
import os
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from operators import (CreateTablesOperator, StageToRedshiftOperator, LoadFactOperator,
                       LoadDimensionOperator, DataQualityOperator)

from operators.stage_redshift import copy_staging_events, copy_staging_songs

from helpers import SqlQueries
from helpers.create_statements import tables_list

default_args = {
    'owner': 'student',
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'depends_on_past': False,
    'email_on_retry': False,
    'catchup': False,
}

@dag(
    default_args=default_args,
    start_date= datetime(2018, 11, 1),
    end_date=datetime(2018, 11, 2),
    description='Load and transform data in Redshift with Airflow',
    schedule='@daily'
)
def final_project():

    start_operator = EmptyOperator(task_id='Begin_execution')


    create_tables_redshift = CreateTablesOperator(
        task_id='create_tables',
        redshift_conn_id='redshift',
        sql_list=tables_list
    )

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id='Stage_events',
        redshift_conn_id='redshift',
        copy_staging_events=copy_staging_events
        
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id='Stage_songs',
        redshift_conn_id='redshift',
        copy_staging_songs=copy_staging_songs

    )

    load_songplays_table = LoadFactOperator(
        task_id='Load_songplays_fact_table',
        redshift_conn_id='redshift',
        sql=SqlQueries.songplay_table_insert,
        append_only=True
        
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id='Load_user_dim_table',
        redshift_conn_id='redshift',
        table='users',
        sql=SqlQueries.user_table_insert,
        mode='overwrite'
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id='Load_song_dim_table',
        redshift_conn_id='redshift',
        table='songs',
        sql=SqlQueries.song_table_insert,
        mode='overwrite'
    )

    load_artist_dimension_table = LoadDimensionOperator(
        task_id='Load_artist_dim_table',
        redshift_conn_id='redshift',
        table='artists',
        sql=SqlQueries.artist_table_insert,
        mode='overwrite'
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id='Load_time_dim_table',
        redshift_conn_id='redshift',
        table='time',
        sql=SqlQueries.time_table_insert,
        mode='overwrite'
    )

    run_quality_checks = DataQualityOperator(
        task_id='Run_data_quality_checks',
        redshift_conn_id='redshift',
        tables=['songplays', 'users', 'songs', 'artists', 'time']
    )


    end_operator = EmptyOperator(task_id='End_execution')


    start_operator >> create_tables_redshift >> [stage_events_to_redshift, stage_songs_to_redshift] >> load_songplays_table
    load_songplays_table >> [load_user_dimension_table, load_song_dimension_table, load_artist_dimension_table, load_time_dimension_table] >> run_quality_checks
    run_quality_checks >> end_operator

final_project_dag = final_project()

