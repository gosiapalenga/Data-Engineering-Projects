import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Load data from S3 buckets to staging tables.
    Staging tables: staging_log_data, staging_songs_data.
    """
    for query in copy_table_queries:
        try:
            cur.execute(query)
            print("------------------------------")
            print(query + "completed successfully")
        except psycopg2.Error as e:
            print("Error executing: " + query)
            print(e)


def insert_tables(cur, conn):
    """
    Insert data from staging tables to fact and dimensional tables.
    """
    for query in insert_table_queries:
        try:
            cur.execute(query)
            print("------------------------------")
            print(query + "completed successfully")
        except psycopg2.Error as e:
            print("Error executing: " + query)
            print(e)


def main():
    """
    Connect to Redshift cluster database.
    Get cursor.
    Load data from S3 buckets to staging tables.
    Insert data from staging tables to fact and dimensional tables.    
    """    
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                                .format(*config['CLUSTER']
                                .values()))
    
    cur = conn.cursor()
        
                
    conn.set_session(autocommit = True)
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()