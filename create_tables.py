import configparser
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def drop_tables(cur, conn):
    """
    Drops the staging and analysis tables using the query as defined in the list 'drop_table_queries'.
    
        Parameters:
            cur (object): Cursor of the databae connection
            conn (object): Database connection object as returned from the connection() method
    """
    
    print("Dropping tables...")
    
    for query in drop_table_queries:
        
        print("Query: ")
        print(query)
        print("-------------------")
        
        cur.execute(query)
        conn.commit()


def create_tables(cur, conn):
    """
    Creates the staging and analysis tables using the query as defined in the list 'create_table_queries'.
    
        Parameters:
            cur (object): Cursor of the databae connection
            conn (object): Database connection object as returned from the connection() method
    """
    
    print("Creating tables...")
    
    for query in create_table_queries:
        
        print("Query: ")
        print(query)
        print("-------------------")
        
        cur.execute(query)
        conn.commit()


def main():
    """
    Entry function that drops and creates the staging and analysis tables in Redshift. In order to do that, this function will
    establish a connection to the Redshift cluster using configuration as defined in 'dwh.cfg'. This function will invoke drop_tables()
    and create_tables functions passing the database connection and its cursor to drop and create the tables respectively. Finally,
    this function will close the connection.
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

    drop_tables(cur, conn)
    create_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()