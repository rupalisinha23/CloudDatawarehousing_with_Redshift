# import configparser
from config import *
import argparse
import psycopg2
from sql_queries import copy_log_command, copy_song_command, insert_table_queries


def load_staging_tables(cur, conn, iam_role):
    """
    This function extracts he data from the s3 buckets and loads into the staging tables.
    :param cur: cursor
    :param conn: connection to the postgres
    :return: None
    """
    
    # copy to log staging table
    copy_to_log = copy_log_command.format(DWH_SCHEMA,
                                      DWH_LOG_STAGING_TABLE,
                                      S3_BUCKET_LOG_JSON_PATH,
                                      iam_role,
                                      LOG_JSON_FORMAT)
    cur.execute(copy_to_log)
    conn.commit()

    # copy data to song staging table
    copy_to_song = copy_song_command.format(DWH_SCHEMA,
                                      DWH_SONG_STAGING_TABLE,
                                      S3_BUCKET_SONG_JSON_PATH,
                                      iam_role,
                                      LOG_JSON_FORMAT)
    cur.execute(copy_to_song)
    conn.commit()
    return None


def insert_tables(cur, conn):
    """
    This function inserts the data from the staging tables to the fact and dimenion tables.
    :param cur: cursor
    :param conn: connection to the postgres
    :return: None
    """
    cur.execute("SET search_path to {}".format(DWH_SCHEMA))
    conn.commit()
    
    # insert data to tables
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()
    return None


def main():
    # parse arguments
    parser = argparse.ArgumentParser()
    parser.add_argument('--host', type=str, help='enter your redshift endpoint')
    parser.add_argument('--iamrole', type=str, help='enter your iam role credentials')
    args = parser.parse_args()
    DWH_ENDPOINT = args.host
    iam_role = args.iamrole
    
    # create the postgres connection
    conn_string = "postgresql://{}:{}@{}:{}/{}".format(
        DWH_DB_USER,
        DWH_DB_PASSWORD,
        DWH_ENDPOINT,
        DWH_PORT,
        DWH_DB
    )
    conn = psycopg2.connect(conn_string)
    cur = conn.cursor()
    print('Postgres connection is now established.')
    
    # collect data from s3 bucket and put in the staging tables
    load_staging_tables(cur, conn, iam_role)
    print('Data has been loaded to the staging tables.')
    
    # collect data from the staging tables and load into the facts and dimension tables
    insert_tables(cur, conn)
    print('Data has been inserted to the fact and dimension tables.')
    
    conn.close()
    print('ETL is complete. Cursor is now closed!')


if __name__ == "__main__":
    main()