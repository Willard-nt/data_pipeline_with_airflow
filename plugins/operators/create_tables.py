from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from typing import List

class CreateTablesOperator(BaseOperator):
    
    def __init__(
                self,
                redshift_conn_id: str,
                sql_list: List[str],
                 **kwargs
                 ):

        super(CreateTablesOperator, self).__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.sql_list = sql_list

    def execute(self, context):
        
        self.log.info("Connecting to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.sql_list:
            self.log.info(f"Executing {len(self.sql_list)} SQL statements")
            for i, query in enumerate(self.sql_list, start=1):
                self.log.info(f"Running query {i}: {query[:100]}...")
                redshift.run(query)


        self.log.info("All tables created successfully")