# import configparser
from config import *
import argparse
import psycopg2
from sql_queries import create_table_queries, drop_table_queries


def create_schema(cur, conn):
    """
    This function creates the schema if not exists.
    :param cur: cursor
    :param conn: connection to postgres
    :return: None
    """
    cur.execute('CREATE SCHEMA IF NOT EXISTS {}'.format(DWH_SCHEMA))
    conn.commit()
    cur.execute('SET search_path to {}'.format(DWH_SCHEMA))
    conn.commit()
    return None


def drop_tables(cur, conn):
    """
    This function drops the tables if they already exists.
    :param cur: cursor
    :param conn: connection to postgres
    :return: None
    """
    cur.execute('SET search_path to {}'.format(DWH_SCHEMA))
    conn.commit()
    
    for query in drop_table_queries:
        cur.execute(query)
        conn.commit()
    return None


def create_tables(cur, conn):
    """
    This function creates the tables.
    :param cur: cursor
    :param conn: connection to the postgres
    :return: None
    """
    cur.execute('SET search_path to {}'.format(DWH_SCHEMA))
    conn.commit()
    
    for query in create_table_queries:
        cur.execute(query)
        conn.commit()
        
    return None


def main():
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, help='Type the endpoint')
    args = parser.parse_args()
    DWH_ENDPOINT = args.host
    
    # create the postgres connection
    print('Connecting to the Postgres.')
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(
                    DWH_DB_USER,
                    DWH_DB_PASSWORD,
                    DWH_ENDPOINT,
                    DWH_PORT,
                    DWH_DB)

    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    print('Postgres connection has been established successfully!')
    
    # create schema
    create_schema(cur, conn)
    
    # drop tables
    drop_tables(cur, conn)
    
    # create tables
    create_tables(cur, conn)

    # close the connection
    conn.close()


if __name__ == "__main__":
    main()