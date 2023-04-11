from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        test_query,
        expected_result,
        redshift="redshift",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.test_query = test_query
        self.expected_result = expected_result
        self.redshift = redshift

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift_connection = PostgresHook(postgres_conn_id=self.redshift)

        self.log.info("Start testing")
        result = redshift_connection.get_records(self.test_query)
        if result[0][0] != self.expected_result:
            self.log.info("Data quality error occured!")
            raise ValueError(
                f"""
                Data quality error occured!
                {result[0][0]} != {self.expected_result}
            """
            )
        else:
            self.log.info("Data quality check passed!")
