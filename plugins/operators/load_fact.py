from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class LoadFactOperator(BaseOperator):
    ui_color = '#F98866'

    def __init__(self,
                redshift_conn_id: str,
                sql: str,
                append_only:bool = True,
                 **kwargs):

        super(LoadFactOperator, self).__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql = sql
        self.append_only = append_only

    def execute(self, context):
        
        self.log.info("Connecting to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.append_only:
            self.log.info("Appending data to fact table")
            redshift.run(self.sql)

        else:
            self.log.info("Overwriting data in songplays table")
            redshift.run("DELETE FROM songplays;")
            redshift.run(self.sql)

        self.log.info("Insert data into songplays table is successful")