from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults
from plugins.helpers import sql_statements_song_db

class LoadFactOperator(BaseOperator):

    ui_color = '#F98866'
    
    sql_template = """
        INSERT INTO {}
        {};
    """
    
    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 aws_credentials_id = "",
                 functionality="",
                 *args, **kwargs):

        super(LoadFactOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.functionality = functionality

    def execute(self, context):
        # Set AWS Redshift connections
        self.log.info("Setting up Redshift connection...") 
        metastoreBackend = MetastoreBackend()
        #aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        #Creating table if not exits:
        self.log.info("Creating staging_events table if not exists into Redshift")
        redshift.run(sql_statements_song_db.CREATE_FACT_SONGPLAYS_TABLE)

        #Delete contents if delete-load mode.
        if self.functionality == "delete-load":
            self.log.info("functionality = delete-load. Clearing data from Redshift destination table: {} ..."\
                          .format(self.table))
            redshift.run("DELETE FROM {}".format(self.table))
        else:
            # It means is "append-only"
            self.log.info("functionality = append-only. No Delete, just inserting new data on top of old one in Redshift destination table: {} ..."\
                          .format(self.table))
        
        # Prepare SQL query
        self.log.info("Preparing SQL query for {} table".format(self.table))
        
        if self.functionality == "append-only":
            query_name = sql_statements_song_db.FACT_SONGPLAYS_TABLE_INSERT_ONLY
            formatted_sql = LoadFactOperator.sql_template.format(
            self.table)

        else:
            query_name = sql_statements_song_db.FACT_SONGPLAYS_TABLE_DELET_LOAD_INSERT
            formatted_sql = LoadFactOperator.sql_template.format(
            self.table,
            query_name
            )
        
        # Executing Load operation
        self.log.info("Executing Redshift load operation to {}..."\
                        .format(self.table))
        self.log.info("SQL query: {}".format(formatted_sql))
        redshift.run(formatted_sql)
        self.log.info("Redshift load operation DONE.")