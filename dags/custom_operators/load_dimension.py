from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    insert_sql = """
    INSERT INTO {}
    {}
    """

    @apply_defaults
    def __init__(self, table, query, redshift="redshift", *args, **kwargs):
        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        self.table = table
        self.query = query
        self.redshift = redshift

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift_connection = PostgresHook(postgres_conn_id=self.redshift)

        self.log.info(f"Clearing data from Redshift table {self.table}")
        redshift_connection.run(f"DELETE FROM {self.table}")

        self.log.info(f"Insert data to Redshift table {self.table}")

        query = LoadDimensionOperator.insert_sql.format(self.table, self.query)

        redshift_connection.run(query)

        self.log.info(f"Completed inserting data to Redshift table {self.table}")
