import datetime
from plugins.helpers import sql_statements_song_db
from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults

#https://airflow.apache.org/docs/apache-airflow/stable/_api/airflow/models/baseoperator/index.html

class StageToRedshiftOperator(BaseOperator):
    ui_color = '#358140'
    template_fields = ('s3_key', 'execution_date')
    copy_sql = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json 'auto';
    """
    copy_sql_json = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{}'
        SECRET_ACCESS_KEY '{}'
        json '{}';
    """

    #https://airflow.apache.org/docs/apache-airflow/1.10.3/_modules/airflow/utils/decorators.html
    @apply_defaults
    def __init__(self,
                 redshift_conn_id = "",
                 aws_credentials_id = "",
                 table = "",
                 s3_bucket = "",
                 s3_key = "",
                 json_path = "",
                 execution_date = "",
                 *args, **kwargs):

        super(StageToRedshiftOperator, self).__init__(*args, **kwargs)
        self.aws_credentials_id = aws_credentials_id
        self.table = table
        self.redshift_conn_id = redshift_conn_id
        self.s3_bucket = s3_bucket
        self.s3_key = s3_key
        self.json_path = json_path
        self.execution_date = execution_date
        
    def execute(self, context):

        self.log.info("Setting up Redshift connection...") 
        metastoreBackend = MetastoreBackend()
        aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        if self.table == 'staging_events':
            self.log.info("Creating staging_events table if not exists into Redshift")
            redshift.run(sql_statements_song_db.CREATE_STAGING_EVENTS_TABLE_SQL)

            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info("Copying data from S3 to Redshift")
            exec_date_rendered = self.execution_date.format(**context)
            exec_date_obj = datetime.datetime.strptime( exec_date_rendered, \
                                                    '%Y-%m-%d')
            exec_date_obj_str = str(exec_date_obj)
            rendered_key = self.s3_key.format(**context)
            rendered_key = rendered_key + '/' + str(exec_date_obj.year) + '/' + str(exec_date_obj.month) + '/' + exec_date_obj_str[0:10] + '-events.json'
            
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            formatted_sql = StageToRedshiftOperator.copy_sql_json \
                .format(self.table,
                        s3_path,
                        aws_connection.login,
                        aws_connection.password,
                        self.json_path
                        )
            redshift.run(formatted_sql)
        else:
            self.log.info("Creating staging_songs table if not exists into Redshift")
            redshift.run(sql_statements_song_db.CREATE_STAGING_SONGS_TABLE_SQL)

            self.log.info("Clearing data from destination Redshift table")
            redshift.run("DELETE FROM {}".format(self.table))

            self.log.info("Copying data from S3 to Redshift")        
            rendered_key = self.s3_key.format(**context)
            
            s3_path = "s3://{}/{}".format(self.s3_bucket, rendered_key)
            formatted_sql = StageToRedshiftOperator.copy_sql \
                .format(self.table,
                        s3_path,
                        aws_connection.login,
                        aws_connection.password
                        )
            redshift.run(formatted_sql)

    





