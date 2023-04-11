import pendulum
from airflow.decorators import dag
from airflow.operators.empty import EmptyOperator
from custom_operators import StaticQueryOperator
from helpers import SqlQueries

default_args = {
    "owner": "me",
    "start_date": pendulum.datetime(2023, 4, 11),
    "depends_on_past": False,
    "retries": 3,
    "retry_delay": pendulum.duration(minutes=1),
    "catchup": False,
}


@dag(
    description="Process to create the tables necessary in Amazon Redshift if not existant",
    schedule_interval="@once",
    default_args=default_args,
)
def create_tables():
    """Additional pipeline to create tables necessary in Redshift if not existant"""

    start_operator = EmptyOperator(task_id="Begin_execution")

    create_staging_events_table = StaticQueryOperator(
        task_id="create_staging_events_table",
        redshift="redshift",
        query=SqlQueries.create_staging_events_table,
    )

    create_staging_songs_table = StaticQueryOperator(
        task_id="create_staging_songs_table",
        redshift="redshift",
        query=SqlQueries.create_staging_songs_table,
    )

    create_songplays_table = StaticQueryOperator(
        task_id="create_songplays_table",
        redshift="redshift",
        query=SqlQueries.create_songplays_table,
    )

    create_artists_table = StaticQueryOperator(
        task_id="create_artists_table",
        redshift="redshift",
        query=SqlQueries.create_artists_table,
    )

    create_songs_table = StaticQueryOperator(
        task_id="create_songs_table",
        redshift="redshift",
        query=SqlQueries.create_songs_table,
    )

    create_time_table = StaticQueryOperator(
        task_id="create_time_table",
        redshift="redshift",
        query=SqlQueries.create_time_table,
    )

    create_users_table = StaticQueryOperator(
        task_id="create_users_table",
        redshift="redshift",
        query=SqlQueries.create_users_table,
    )

    end_operator = EmptyOperator(task_id="End_execution")

    (
        start_operator
        >> [
            create_staging_events_table,
            create_staging_songs_table,
            create_artists_table,
            create_time_table,
            create_users_table,
        ]
        >> create_songs_table
        >> create_songplays_table
        >> end_operator
    )


create_tables_dag = create_tables()
