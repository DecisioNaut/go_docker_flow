# import os
# from datetime import datetime, timedelta

import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from custom_operators import DataQualityOperator  # StaticQueryOperator,
from custom_operators import (
    LoadDimensionOperator,
    LoadFactOperator,
    StageToRedshiftOperator,
)
from helpers import SqlQueries

default_args = {
    "owner": "me",
    "start_date": pendulum.datetime(2023, 1, 1),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=10),
    "catchup": False,
}


@dag(
    default_args=default_args,
    description="Load and transform data in Redshift with Airflow",
    schedule_interval=None,
    # schedule_interval='0 * * * *',
)
def sparkify_pipe():
    """
    Pipeline to update Sparkify's data warehouse from data in S3 buckets
    """

    start_operator = EmptyOperator(task_id="Begin_execution")

    stage_events_to_redshift = StageToRedshiftOperator(
        task_id="Stage_events",
        s3_folder="log-data",
        s3_json_format="s3://udacity-dend/log_json_path.json",
        redshift_table="staging_events",
    )

    stage_songs_to_redshift = StageToRedshiftOperator(
        task_id="Stage_songs",
        s3_folder="song-data/A/A",
        s3_json_format="auto",
        redshift_table="staging_songs",
    )

    staging_done_operator = EmptyOperator(task_id="Staging_completed")

    load_artist_dimension_table = LoadDimensionOperator(
        task_id="Load_artist_dim_table",
        redshift="redshift",
        table="artists",
        query=SqlQueries.artist_table_insert,
        append=False,
    )

    load_song_dimension_table = LoadDimensionOperator(
        task_id="Load_song_dim_table",
        redshift="redshift",
        table="songs",
        query=SqlQueries.song_table_insert,
        append=False,
    )

    load_time_dimension_table = LoadDimensionOperator(
        task_id="Load_time_dim_table",
        redshift="redshift",
        table="time",
        query=SqlQueries.time_table_insert,
        append=False,
    )

    load_user_dimension_table = LoadDimensionOperator(
        task_id="Load_user_dim_table",
        redshift="redshift",
        table="users",
        query=SqlQueries.user_table_insert,
        append=False,
    )

    dimensions_done_operator = EmptyOperator(task_id="Dimensions_completed")

    load_songplays_table = LoadFactOperator(
        task_id="Load_songplays_fact_table",
        redshift="redshift",
        table="songplays",
        query=SqlQueries.songplay_table_insert,
        append=False,
    )

    facts_done_operator = EmptyOperator(task_id="Facts_completed")

    run_quality_checks = DataQualityOperator(
        task_id="Run_data_quality_checks",
        test_query="""
            SELECT
                count(session_id) + count(songplay_id) AS primary_keys
            FROM
                songplays
            WHERE
                (session_id IS NULL) OR (songplay_id IS NULL)
        """,
        expected_result=0,
    )

    end_operator = EmptyOperator(task_id="End_execution")

    (
        start_operator
        >> [stage_events_to_redshift, stage_songs_to_redshift]
        >> staging_done_operator
        >> [
            load_user_dimension_table,
            load_artist_dimension_table,
            load_time_dimension_table,
        ]
        >> load_song_dimension_table  # Has a dependency on artists table
        >> dimensions_done_operator
        >> load_songplays_table
        >> facts_done_operator
        >> run_quality_checks
        >> end_operator
    )


sparkify_pipe_dag = sparkify_pipe()
