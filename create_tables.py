import configparser
import psycopg2
import sqlparse
import json

from sql_queries import (
    create_table_queries, 
    drop_table_queries,
    copy_table_queries,
    insert_table_queries,
)


def drop_tables(cur):
    """ Delete all the tables used by the data warehouse

        Args:
        * cur: the cursor to the db connection
    """
    print("=== Dropping Tables...")
    for query in drop_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(e)


def create_tables(cur):
    """ Create all the tables needed for the data warehouse

    Args:
        * cur: the cursor to the db connection
    """
    print("=== Creating Tables...")
    for query in create_table_queries:
        try:
            cur.execute(query)
        except Exception as e:
            print(e)


def create_jsonpath():
    """ Create manifest file for reading json files from S3.
        Needed because some of the keys have capitalization which
        the `auto` setting doesn't handle properly.
    """

    data = {
        "jsonpaths": [
            "$['artist']",
            "$['auth']",
            "$['firstName']",
            "$['gender']",
            "$['itemInSession']",
            "$['lastName']",
            "$['length']",
            "$['level']",
            "$['location']",
            "$['method']",
            "$['page']",
            "$['registration']",
            "$['sessionId']",
            "$['song']",
            "$['status']",
            "$['ts']",
            "$['userAgent']",
            "$['userId']",
        ]
    }

    json.dump(data, open("events.jsonpaths", 'w+'))


def setup_db_connection():
    """ Setup the connection to the Redshift dB
    """

    print ("=== Setup dB Connection")
    config = configparser.ConfigParser()
    config_redshift = configparser.ConfigParser()

    config.read('dwh.cfg')
    config_redshift.read('cluster.cfg')

    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(config_redshift.get("REDSHIFT", "dwh_endpoint"),
                                    *config['REDSHIFT'].values())
                        )
    print("Connected!")
    conn.set_session(autocommit=True)
    return conn

def main():
    """ Main entrypoint for the script
    """

    conn = setup_db_connection()
    cur = conn.cursor()

    drop_tables(cur)
    create_tables(cur)

    conn.close()
    print ("Done!")

if __name__ == "__main__":
    main()