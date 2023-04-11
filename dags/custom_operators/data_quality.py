from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults


class DataQualityOperator(BaseOperator):
    ui_color = "#89DA59"

    @apply_defaults
    def __init__(
        self,
        checks,
        redshift="redshift",
        *args,
        **kwargs,
    ):
        super().__init__(*args, **kwargs)
        self.checks = checks
        self.redshift = redshift

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift_connection = PostgresHook(postgres_conn_id=self.redshift)

        for num, check in enumerate(self.checks):
            self.log.info(f"Start test {num}")
            result = redshift_connection.get_records(check["test_query"])
            if result[0][0] != check["expected_result"]:
                self.log.info(f"Data quality error occured with check {num}!")
                raise ValueError(
                    f"""
                    Data quality error occured with check {num}!
                    {result[0][0]} != {check["expected_result"]}
                """
                )
            else:
                self.log.info(f"Data quality check {num} passed!")
