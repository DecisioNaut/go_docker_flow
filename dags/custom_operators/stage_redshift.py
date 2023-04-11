from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults


class StageToRedshiftOperator(BaseOperator):
    """Operator to copy data from s3 to staging area in Amazon Redshift (serverless)"""

    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        REGION AS '{}'
        FORMAT AS JSON '{}'
    """

    @apply_defaults
    def __init__(
        self,
        s3_folder,
        redshift_table,
        s3_bucket="udacity-dend",
        s3_region="us-west-2",
        s3_json_format="",
        aws_credentials="aws_credentials",
        redshift="redshift",
        *args,
        **kwargs,
    ):
        # super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        super().__init__(*args, **kwargs)

        metastore_backend = MetastoreBackend()

        self.aws_credentials = metastore_backend.get_connection(aws_credentials)

        self.s3_path = f"s3://{s3_bucket}/{s3_folder}/"
        self.s3_region = s3_region
        self.s3_json_format = s3_json_format

        self.redshift = redshift
        self.redshift_table = redshift_table

    def execute(self, context):
        self.log.info("Connecting to Redshift")
        redshift_connection = PostgresHook(postgres_conn_id=self.redshift)

        self.log.info(f"Clearing data from Redshift table {self.redshift_table}")
        redshift_connection.run(f"DELETE FROM {self.redshift_table}")

        self.log.info(f"Copying data from S3 to Redshift table {self.redshift_table}")

        query = StageToRedshiftOperator.copy_sql.format(
            self.redshift_table,
            self.s3_path,
            self.aws_credentials.login,
            self.aws_credentials.password,
            self.s3_region,
            self.s3_json_format,
        )

        redshift_connection.run(query)
