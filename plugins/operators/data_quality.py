from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException
class DataQualityOperator(BaseOperator):

    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str,
                 tables: '',
                 **kwargs):

        super(DataQualityOperator, self).__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tables = tables or []

    def execute(self, context):
        self.log.info('Connecting to Redshift database')
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for table in self.tables:
            self.log.info(f"Running data quality check on table: {table}")
            records = redshift.get_records(f"SELECT COUNT(*) FROM {table};")

            if not records or records[0][0] < 1:
                raise AirflowException(f"Data quality check failed: {table} returned no rows.")
            else:
                self.log.info(f"Data quality check passed for {table}: {records[0][0]} records found.")
