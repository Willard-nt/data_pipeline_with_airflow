from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator

copy_staging_events = ("""
        COPY staging_events
        FROM 's3://ghostface-cowboy/log_data/2018/11/{{ ds }}-events.json'
        IAM_ROLE 'arn:aws:iam::616717107464:role/my-redshift-service-role'
        FORMAT AS JSON 's3://ghostface-cowboy/log_json_path.json'
        TIMEFORMAT 'auto'
        ACCEPTANYDATE
        MAXERROR 100
    """)

copy_staging_songs = ("""
        COPY staging_songs
        FROM 's3://ghostface-cowboy/song-data/A/A/A/'
        IAM_ROLE 'arn:aws:iam::616717107464:role/my-redshift-service-role'
        FORMAT AS JSON 'auto'
    """)


class StageToRedshiftOperator(BaseOperator):
    template_fields = ['copy_staging_events']
    ui_color = '#358140'

    def __init__(
                self,
                redshift_conn_id: str,
                copy_staging_events: str = None,
                copy_staging_songs: str = None,
                 **kwargs
                 ):

        super(StageToRedshiftOperator, self).__init__(**kwargs)
        self.redshift_conn_id = redshift_conn_id
        self.copy_staging_events = copy_staging_events
        self.copy_staging_songs = copy_staging_songs
       


    def execute(self, context):
        
        self.log.info("Connecting to Redshift database")
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)

        if self.copy_staging_events:
            self.log.info("Copying event data from s3")
            redshift.run(self.copy_staging_events)

        if self.copy_staging_songs:
            self.log.info("Copying song data from s3")
            redshift.run(self.copy_staging_songs)

        self.log.info("Event and song data have been copied successfully")



__all__ = ['StageToRedshiftOperator', 'copy_staging_events', 'copy_staging_songs']

