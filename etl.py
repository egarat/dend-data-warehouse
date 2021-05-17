import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    Loads the staging tables 'staging_events' and 'staging_songs' using the COPY statement as defined in the list 'copy_table_queries'
    
        Parameters:
            cur (object): Cursor of the databae connection
            conn (object): Database connection object as returned from the connection() method
    """
    
    print("Loading staging tables...")
    
    for query in copy_table_queries:
        
        print("Query: ")
        print(query)
        print("-------------------")
        
        cur.execute(query)
        conn.commit()

    print("Staging tables loaded!")

def insert_tables(cur, conn):
    """
    Transforms and loads data from the staging tables to the analysis tables using INSERT statements as defined in the list 'insert_table_queries'.
    
        Parameters:
            cur (object): Cursor of the databae connection
            conn (object): Database connection object as returned from the connection() method
    """
    
    print("Loading OLAP tables...")
    
    for query in insert_table_queries:
        
        print("Query: ")
        print(query)
        print("-------------------")
        
        cur.execute(query)
        conn.commit()

    print("OLAP tables loaded!")


def main():
    """
    Entry function that will trigger the ETL process. The process consists of the following steps:
        - Extract JSON source files from S3 and load them into staging tables
        - Transform the data from staging tables
        - Load transformed data into analysis tables
    """
    config = configparser.ConfigParser()
    config.read("dwh.cfg")

    HOST        = config.get("CLUSTER", "HOST")
    DB_NAME     = config.get("CLUSTER", "DB_NAME")
    DB_USER     = config.get("CLUSTER", "DB_USER")
    DB_PASSWORD = config.get("CLUSTER", "DB_PASSWORD")
    DB_PORT     = config.get("CLUSTER", "DB_PORT")

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(HOST, DB_NAME, DB_USER, DB_PASSWORD, DB_PORT))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()