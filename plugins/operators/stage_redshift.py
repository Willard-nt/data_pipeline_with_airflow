from airflow.models import BaseOperator
from airflow.hooks.base import BaseHook
from airflow.providers.postgres.hooks.postgres import PostgresHook

class StageToRedshiftOperator(BaseOperator):
    template_field = ('s3_key')

    def __init__(
        self,
        *,
        redshift_conn_id: str,
        aws_conn_id: str,
        table: str,
        s3_bucket: str,
        s3_key: str,
        json_format: str = 'auto',
        **kwargs,
    ):
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.aws_conn_id = aws_conn_id
        self.table = table
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_format = json_format

    def execute(self, context):
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        aws_conn = BaseHook.get_connection(self.aws_conn_id)
        access_key = aws_conn.login
        secret_key = aws_conn.password

        rendered_key = self.s3_key.format(**context)
        s3_path = f's3://{self.s3_bucket}/{rendered_key}'

        self.log.info(f'Copying data from {s3_path} to Redshift table {self.table}')

    
        if self.json_format.lower() == 'auto':
            format_option = "FORMAT AS JSON 'auto'"
        else:
            format_option = f"FORMAT AS JSON '{self.json_format}'"

        copy_sql = f"""
            COPY {self.table}
            FROM '{s3_path}'
            ACCESS_KEY_ID '{access_key}'
            SECRET_ACCESS_KEY '{secret_key}'
            {format_option};
        """

        redshift.run(copy_sql)
        self.log.info(f'Successfully copied data to {self.table}')
