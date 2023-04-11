from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class LoadDimensionOperator(BaseOperator):
    ui_color = "#80BD9E"

    @apply_defaults
    def __init__(
        self, table, query, redshift="redshift", append=False, *args, **kwargs
    ):
        super().__init__(*args, **kwargs)
        self.table = table
        self.query = query
        self.redshift = redshift
        self.append = append

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift_connection = PostgresHook(postgres_conn_id=self.redshift)

        if self.append:
            self.log.info(f"Appending to Redshift table {self.table}")
            query = f"""
                CREATE TEMP TABLE temp_{self.table} (LIKE {self.table});

                INSERT INTO temp_{self.table}
                {self.query};

                DELETE FROM {self.table}
                USING temp_{self.table};

                INSERT INTO {self.table}
                SELECT * FROM temp_{self.table};
            """
            redshift_connection.run(query)
            self.log.info(f"Completed appending to Redshift table {self.table}")

        else:
            self.log.info(f"Clearing data from Redshift table {self.table}")
            redshift_connection.run(f"DELETE FROM {self.table}")

            self.log.info(f"Insert data to Redshift table {self.table}")
            query = f"""
                INSERT INTO {self.table}
                {self.query}
            """
            redshift_connection.run(query)
            self.log.info(f"Completed inserting data to Redshift table {self.table}")
