import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    """
    This function loads data into staging tables from S3 by running all queries in the copy_table_queries list. 
    The DB connection string and cursor are passed in as arguments to the function
    """
    for query in copy_table_queries:
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    """
    This function performs ETL from staging tables to fact/dimension tables by running all queries in the 
    insert_table_queries list. 
    The DB connection string and cursor are passed in as arguments to the function
    """
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    """
    Driver Function
    """
    config = configparser.ConfigParser()
    config.read('dwh.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}".format(*config['CLUSTER'].values()))
    cur = conn.cursor()
    
    load_staging_tables(cur, conn)
    insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()