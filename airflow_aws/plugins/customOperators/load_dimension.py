from airflow.providers.postgres.hooks.postgres import PostgresHook
from airflow.models import BaseOperator
from airflow.secrets.metastore import MetastoreBackend
from airflow.utils.decorators import apply_defaults
from plugins.helpers import sql_statements_song_db

class LoadDimensionOperator(BaseOperator):

    ui_color = '#80BD9E'
    sql_template = """
        INSERT INTO {}
        {};
    """
    create_query_name = 'CREATE_{}_TABLE'
    insert_query_name = '{}_TABLE_INSERT_TO'

    @apply_defaults
    def __init__(self,
                 # Define your operators params (with defaults) here
                 redshift_conn_id="",
                 table="",
                 aws_credentials_id = "",
                 functionality="",
                 *args, **kwargs):

        super(LoadDimensionOperator, self).__init__(*args, **kwargs)
        # Map params here
        self.redshift_conn_id = redshift_conn_id
        self.table = table
        self.aws_credentials_id = aws_credentials_id
        self.functionality = functionality

    def execute(self, context):
        # Set AWS Redshift connections
        self.log.info("Setting up Redshift connection...") 
        #metastoreBackend = MetastoreBackend()
        #aws_connection=metastoreBackend.get_connection(self.aws_credentials_id)
        redshift = PostgresHook(postgres_conn_id=self.redshift_conn_id)
        self.log.info("Redshift connection created.")

        #Creating table if not exits:
        self.log.info("Creating one of the dimension tables if not exists into \
                      Redshift")
        specific_create_query = LoadDimensionOperator.create_query_name.format(
            self.table.upper())
        

        redshift.run(getattr(sql_statements_song_db, specific_create_query))
        #if self.table == 'dimusers':
        #    redshift.run(sql_statements_song_db.CREATE_DIM_USERS_TABLE)
        #elif self.table == 'dimsongs':
        #    redshift.run(sql_statements_song_db.CREATE_DIM_SONGS_TABLE)
        #elif self.table == 'dimartists':
        #    redshift.run(sql_statements_song_db.CREATE_DIM_ARTISTS_TABLE)
        #else: #self.table == 'dimtime':
        #    redshift.run(sql_statements_song_db.CREATE_DIM_TIME_TABLE)

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
            specific_insert_query = LoadDimensionOperator \
                .insert_query_name.format(self.table.upper())
        
            query_name = getattr(sql_statements_song_db, \
                                 specific_insert_query)
            
            formatted_sql = LoadDimensionOperator \
                .sql_template.format(self.table,
                                     query_name. \
                                        format(self.table, self.table)
        )

        else:
            specific_insert_query = LoadDimensionOperator \
                .insert_query_name.format(self.table.upper())
        
            query_name = getattr(sql_statements_song_db, \
                                 specific_insert_query)
            
            formatted_sql = LoadDimensionOperator \
                .sql_template.format(self.table,
                                     query_name. \
                                        format(self.table, self.table)
        )

        # Executing Load operation
        self.log.info("Executing Redshift load operation to {}..."\
                        .format(self.table))
        self.log.info("SQL query: {}".format(formatted_sql))
        redshift.run(formatted_sql)
        self.log.info("Redshift load operation DONE.") 