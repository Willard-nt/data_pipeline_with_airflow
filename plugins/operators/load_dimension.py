from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'

    def __init__(self,
                 redshift_conn_id: str,
                 sql: str,
                 table:str,
                 mode: str = 'overwrite',
                 **kwargs):

        super(LoadDimensionOperator, self).__init__(**kwargs)
        self.redshift_conn_id=redshift_conn_id
        self.sql = sql
        self.table = table
        self.mode = mode.lower()

    def execute(self, context):
        if self.mode not in ['overwrite', 'append']:
            raise ValueError(f"Invalid mode '{self.mode}'. Use 'overwrite' or 'append'.")


        self.log.info("Connecting to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        
        if self.mode == 'overwrite':
            self.log.info(f"Truncating {self.table} table; overwriting data")
            redshift.run(f"TRUNCATE TABLE {self.table};")
            redshift.run(self.sql)

        else:
            self.log.info(f"Appending data to {self.table} table")
            redshift.run(self.sql)

        self.log.info("Loaded data to dimension tables succesfully")