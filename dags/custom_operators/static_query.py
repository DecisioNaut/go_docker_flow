import logging

from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class StaticQueryOperator(BaseOperator):
    """Operator to run pre-defined Queries"""

    @apply_defaults
    def __init__(self, redshift, query, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.redshift = redshift
        self.query = query

    def execute(self, context):
        redshift_connection = PostgresHook(postgres_conn_id=self.redshift)
        response = redshift_connection.run(self.query)
        logging.info(response)
