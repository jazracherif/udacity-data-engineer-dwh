import configparser
import psycopg2
import pandas as pd

from sql_queries import (
    copy_table_queries, 
    insert_table_queries, 
    tests_queries
)


def load_staging_tables(cur):
    print("=== Loading S3 files into staging tables...")
    for query in copy_table_queries:
        try:
            print(query)
            cur.execute(query)
            print ("Success!")

        except Exception as e:
            print(e)


def insert_tables(cur):
    print("=== Inserting staging data into main tables...")
    for query in insert_table_queries:
        try:
            print(query)
            cur.execute(query)
            print ("Success!")

        except Exception as e:
            print(e)

def run_tests(cur):
    print("=== Runs tests...")
    for query in tests_queries:
        try:
            print(query)
            cur.execute(query)
            rows = cur.fetchall()
            print(pd.DataFrame(rows))

        except Exception as e:
            print(e)

def check_tables(cur):
    tables = (
                ("staging_events", 
                    ("artist", "auth", "firstName", "gender" ,
                     "itemInSession","lastName","length", "level", 
                     "location", "method", "page", "registration",
                     "sessionId" ,"song", "status" ,"ts","userAgent", "userId")
                ),
                ("staging_songs", 
                    ("num_songs", "artist_id", "artist_latitude", "artist_longitude", "artist_location", 
                     "artist_name", "song_id",  "title", "duration", "year")
                ),
                ("users", ("user_id", "first_name", "last_name", "gender", "level")
                ),
                ("songs", ("song_id", "title", "artist_id", "year", "duration")
                ),
                ("artists", ("artist_id", "name", "location", "latitude", "longitude")
                ),
                ("time", ("start_time", "hour", "day", "week", "month", "year", "weekday")
                ),
                ("songplay",
                    ("songplay_id","start_time","user_id", "level", "song_id",
                    "artist_id", "session_id", "location", "user_agent" )
                )
            )
        
    for table, cols in tables:
        query = """
            SELECT * 
              FROM {}
             ORDER BY random()
             LIMIT {};
        """.format(table, 10)

        print(f"\n===== {table} ===== ")

        try:
            cur.execute(query)

            rows = cur.fetchall()
            print(pd.DataFrame(rows, columns=cols))

        except Exception as e:
            print(e)


def setup_db_connection():
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
    conn = setup_db_connection()
    cur = conn.cursor()
    
    # 1. Load Data from S3 to Staging tables
    load_staging_tables(cur)

    # 2. Ingest staging tables into main tables
    insert_tables(cur)

    # 3. Print a sample of data for sanitation
    check_tables(cur)

    # 4. run tests
    run_tests(cur)

    conn.close()

    print("Done!")

if __name__ == "__main__":
    main()