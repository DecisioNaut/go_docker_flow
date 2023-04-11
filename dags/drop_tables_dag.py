import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from custom_operators import StaticQueryOperator
from helpers import SqlQueries

default_args = {
    "owner": "me",
    "start_date": pendulum.datetime(2030, 12, 31),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=1),
    "catchup": False,
}


@dag(
    description="Process to drop the tables in Amazon Redshift if existant",
    schedule_interval=None,
    default_args=default_args,
)
def drop_tables():
    """Additional pipeline to drop tables in Redshift if existant"""

    start_operator = EmptyOperator(task_id="Begin_execution")

    drop_staging_events_table = StaticQueryOperator(
        task_id="drop_staging_events_table",
        redshift="redshift",
        query=SqlQueries.drop_staging_events_table,
    )

    drop_staging_songs_table = StaticQueryOperator(
        task_id="drop_staging_songs_table",
        redshift="redshift",
        query=SqlQueries.drop_staging_songs_table,
    )

    drop_songplays_table = StaticQueryOperator(
        task_id="drop_songplays_table",
        redshift="redshift",
        query=SqlQueries.drop_songplays_table,
    )

    drop_artists_table = StaticQueryOperator(
        task_id="drop_artists_table",
        redshift="redshift",
        query=SqlQueries.drop_artists_table,
    )

    drop_songs_table = StaticQueryOperator(
        task_id="drop_songs_table",
        redshift="redshift",
        query=SqlQueries.drop_songs_table,
    )

    drop_time_table = StaticQueryOperator(
        task_id="drop_time_table",
        redshift="redshift",
        query=SqlQueries.drop_time_table,
    )

    drop_users_table = StaticQueryOperator(
        task_id="drop_users_table",
        redshift="redshift",
        query=SqlQueries.drop_users_table,
    )

    end_operator = EmptyOperator(task_id="End_execution")

    (
        start_operator
        >> [
            drop_staging_events_table,
            drop_staging_songs_table,
            drop_artists_table,
            drop_songs_table,
            drop_time_table,
            drop_users_table,
        ]
        >> drop_songplays_table
        >> end_operator
    )


drop_tables_dag = drop_tables()
