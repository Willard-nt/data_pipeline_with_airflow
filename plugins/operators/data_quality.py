from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.exceptions import AirflowException

class DataQualityOperator(BaseOperator):
    ui_color = '#89DA59'

    def __init__(self,
                 redshift_conn_id: str,
                 tests: list,
                 **kwargs):
        
        super().__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.tests = tests or []

    def execute(self, context):
        if not self.tests:
            raise AirflowException("No data quality tests provided.")

        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        for test in self.tests:
            sql = test.get('sql')
            expected = test.get('expected')

            if not sql or expected is None:
                raise AirflowException("Each test must include 'sql' and 'expected'.")

            self.log.info(f"Running data quality test: {sql}")
            result = redshift.get_records(sql)

            if not result or result[0][0] < expected:
                raise AirflowException(
                    f"Data quality test failed.\nSQL: {sql}\nExpected at least: {expected}\nGot: {result[0][0]}"
                )
            self.log.info(f"Test passed. SQL: {sql} returned {result[0][0]}")
