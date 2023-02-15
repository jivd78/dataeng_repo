import configparser
import psycopg2
from sql_queries import create_table_queries, \
                        create_staging_table_queries, \
                        create_star_table_queries, \
                        drop_table_queries, \
                        drop_staging_table_queries, \
                        drop_star_table_queries    
import os

def drop_tables(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for dropping tables from a 
    list if exist.
    '''
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()

def drop_staging_tables(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for dropping staging tables 
    from a list if exist.
    '''
    for query in drop_staging_table_queries:
        cur.execute(query)
        conn.commit()

def drop_star_tables(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for dropping star schema 
    tables from a list if exist.
    '''
    for query in drop_star_table_queries:
        cur.execute(query)
        conn.commit()

def create_tables(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for creating tables from a 
    list if exist.
    '''
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()

def create_staging_tables(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for creating staging tables 
    from a list if exist.
    '''
    for query in create_staging_table_queries:
        cur.execute(query)
        conn.commit()

def create_star_tables(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for creating star-schema 
    tables from a list if exist.
    '''
    for query in create_star_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}" \
                   .format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    #all tables block
    drop_tables(cur, conn)
    create_tables(cur, conn)

    #staging tables block
    #drop_staging_tables(cur, conn)
    #create_staging_tables(cur, conn)

    #star tables block
    #drop_star_tables(cur, conn)
    #create_star_tables(cur, conn)
    
    conn.close()


if __name__ == "__main__":
    main()