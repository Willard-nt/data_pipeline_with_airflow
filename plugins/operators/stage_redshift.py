from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'

    def __init__(
                self,
                redshift_conn_id: str,
                create_staging_events_table: str,
                create_staging_songs_table: str,
                 **kwargs
                 ):

        super(StageToRedshiftOperator, self).__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.create_staging_events_table = create_staging_events_table
        self.create_staging_songs_table = create_staging_songs_table


    def execute(self, context):
        self.log.info("Connecting to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.create_staging_events_table:
            self.log.info("Creating staging events table")
            redshift.run(self.create_staging_events_table)

        if self.create_staging_songs_table:
            self.log.info("Creating staging songs table")
            redshift.run(self.create_staging_songs_table)

        self.log.info("Both tables created successfully")






