import configparser
import psycopg2
from sql_queries import copy_table_queries, insert_table_queries


def load_staging_tables(cur, conn):
    for query in copy_table_queries:
        print(query)
        cur.execute(query)
        conn.commit()


def insert_tables(cur, conn):
    for query in insert_table_queries:
        cur.execute(query)
        conn.commit()


def main():
    config = configparser.ConfigParser()
    config.read('dwh.cfg')
    
    conn_str = "host={} dbname={} user={} password={} port={}"
    conn_str = conn_str.format(*config['CLUSTER'].values())
    conn = psycopg2.connect(conn_str)
    cur = conn.cursor()
    
    #trouble_shooting(cur, conn)
    load_staging_tables(cur, conn)
    #insert_tables(cur, conn)

    conn.close()


if __name__ == "__main__":
    main()