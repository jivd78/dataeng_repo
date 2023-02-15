import configparser
import psycopg2
from sql_queries import copy_table_queries, \
                        insert_table_queries, \
                        counting_register_queries, \
                        exploring_queries_list    
#import os

def load_staging_tables(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for loading staging tables 
    from a list if exist.
    '''
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for inserting data into star
    schema tables from staging tables.
    '''
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()

def couting_queries(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for analytic purposes, such
    as counting registers of databases.
    '''
    for query in counting_register_queries:
        cur.execute(query)
        result = cur.fetchall()
        for row in result:
            print(query)
            print(row)
        conn.commit()
        
def exploring_queries(cur, conn):
    '''
    With a Cursor and a Connection instances from a postgresql database
    executes and commits a series of queries for analytic purposes.
    '''
    for query in exploring_queries_list:
        cur.execute(query)
        print(query)
        result = cur.fetchall()
        for row in result:
            print(row)
        conn.commit()
 
def main():
    
    # CONFIG
    config = configparser.ConfigParser()

    config.read_file(open('dwh.cfg'))

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}" \
                   .format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)
    couting_queries(cur, conn)
    exploring_queries(cur, conn)

    conn.close()

if __name__ == "__main__":
    main()