import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drop tables in spakify_database if they already exist.
    Use cursor (cur) to connect to database and perform perations on db. 
    """
    for query in drop_table_queries:
        try:
            cur.execute(query)
            print("------------------------------")
            print(query + "completed successfully")
        except psycopg2.Error as e:
            print("Error executing: " + query)
            print(e)


def create_tables(cur, conn):
    """
    Create tables in spakify_database.
    Tables: songplays, users, artists, songs, time
    """
    for query in create_table_queries:
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
    Drop tables if they exist.
    Create new tables in the sparkify_database.
    Close connection.    
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                                .format(*config['CLUSTER']
                                .values()))
    print("Successfully connected to database")

    cur = conn.cursor()
    print("Cursor created successfully")
         
                
    conn.set_session(autocommit = True)

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()