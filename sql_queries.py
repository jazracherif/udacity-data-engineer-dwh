import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('cluster.cfg')
ROLE_ARN = config.get("REDSHIFT", "dwh_role_arn")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs"
songplay_table_drop = "DROP TABLE IF EXISTS songplay"
user_table_drop = "DROP TABLE IF EXISTS users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE staging_events (
        artist          VARCHAR(1024),
        auth            VARCHAR(1024),
        firstName       VARCHAR(1024),
        gender          VARCHAR(1024),
        itemInSession   INTEGER,
        lastName        VARCHAR(1024),
        length          REAL,
        level           VARCHAR(1024),
        location        VARCHAR(1024),
        method          VARCHAR(1024),
        page            VARCHAR(1024) sortkey,
        registration    DOUBLE PRECISION,
        sessionId       INTEGER,
        song            VARCHAR(1024) distkey,
        status          INTEGER,
        ts              BIGINT,
        userAgent       VARCHAR(65535),
        userId          VARCHAR(1024)
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE staging_songs (
        num_songs           INTEGER,
        artist_id           VARCHAR(1024),
        artist_latitude     REAL,
        artist_longitude    REAL,
        artist_location     VARCHAR(1024),
        artist_name         VARCHAR(1024),
        song_id             VARCHAR(1024),
        title               VARCHAR(1024) distkey,
        duration            REAL,
        year                VARCHAR(1024)
    );
""")


songplay_table_create = ("""
    CREATE TABLE songplay (
        songplay_id     BIGINT IDENTITY(0, 1) PRIMARY KEY, 
        start_time      BIGINT NOT NULL, 
        user_id         TEXT NOT NULL DISTKEY, 
        level           TEXT,
        song_id         TEXT  NOT NULL, 
        artist_id       TEXT  NOT NULL, 
        session_id      INTEGER, 
        location        TEXT sortkey, 
        user_agent      TEXT
    );
""")

user_table_create = ("""
    CREATE TABLE users (
        user_id         TEXT PRIMARY KEY DISTKEY, 
        first_name      TEXT, 
        last_name       TEXT,
        gender          TEXT,
        level           TEXT
    );

""")

song_table_create = ("""
    CREATE TABLE songs (
        song_id         TEXT PRIMARY KEY  DISTKEY, 
        title           TEXT, 
        artist_id       TEXT, 
        year            INTEGER,
        duration        FLOAT
    );
    """
)

artist_table_create = ("""
    CREATE TABLE artists (
        artist_id       TEXT PRIMARY KEY, 
        name            TEXT, 
        location        TEXT, 
        latitude        FLOAT, 
        longitude       FLOAT
    )
    diststyle all;
""")

time_table_create = ("""
    CREATE TABLE time (
        start_time      BIGINT PRIMARY KEY, 
        hour            INTEGER, 
        day             INTEGER, 
        week            INTEGER, 
        month           INTEGER, 
        year            INTEGER, 
        weekday         INTEGER
    )
    diststyle all;
""")

# STAGING TABLES


staging_events_copy = ("""
    COPY staging_events FROM 's3://udacity-dend/log_data' 
    credentials 'aws_iam_role={}'
    json 's3://jazra.udacity.dataengineer/events.jsonpaths'
    region 'us-west-2';
""").format(ROLE_ARN)

staging_songs_copy = ("""
    COPY staging_songs FROM 's3://udacity-dend/song_data' 
    credentials 'aws_iam_role={}'
    json 'auto'
    region 'us-west-2';
""").format(ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplay (start_time, user_id, level, song_id, 
                artist_id, session_id, location, user_agent)
    SELECT ts AS start_time, 
           e.userId AS user_id, 
           e.level AS level, 
           s.song_id AS song_id, 
           s.artist_id AS artist_id, 
           e.sessionId AS session_id, 
           e.location AS location, 
           e.userAgent AS user_agent
    FROM staging_events e
    LEFT JOIN staging_songs s ON e.song = s.title
            AND e.artist = s.artist_name
            AND e.length = s.duration
    WHERE e.page = 'NextSong'
      AND s.song_id IS NOT NULL
      AND s.artist_id IS NOT NULL

""")

user_table_insert = ("""
    INSERT INTO users
    WITH numbered_levels AS (
      SELECT ROW_NUMBER() over (PARTITION by userId ORDER BY ts DESC) AS row_num,
             userId AS user_id,
             firstName, 
             lastName, 
             gender, 
             level
        FROM staging_events
    )
    SELECT DISTINCT user_id, firstName, lastName, gender, level
      FROM numbered_levels
     WHERE row_num = 1
""")

song_table_insert = ("""
    INSERT INTO songs 
    SELECT distinct song_id,
           title,
           artist_id,
           CAST(year AS INTEGER),
           duration
      FROM staging_songs
""")

artist_table_insert = ("""
    INSERT INTO artists
    SELECT distinct artist_id,
           artist_name,
           artist_location,
           artist_latitude,
           artist_longitude
      FROM staging_songs
""")

time_table_insert = ("""
    INSERT INTO time 
    SELECT DISTINCT ts AS start_time,
           EXTRACT(hour FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS hour,
           EXTRACT(day FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS day,
           EXTRACT(week FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS week,
           EXTRACT(month FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS monty,
           EXTRACT(year FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS year,
           EXTRACT(weekday FROM timestamp 'epoch' + ts/1000 * interval '1 second') AS weekday
      FROM staging_events
     WHERE page = 'NextSong'
""")


test1 = (
"""
WITH top_users AS (
    SELECT user_id, COUNT(*) AS cnt
    FROM songplay
    GROUP BY user_id
    ORDER BY cnt DESC
    LIMIT 5
)
SELECT users.first_name, 
       users.last_name, 
       top_users.cnt
  FROM top_users
 INNER JOIN users
       ON users.user_id = top_users.user_id
 ORDER BY cnt DESC
"""
)

test2 = (
"""
SELECT location, 
       count(*) AS cnt 
  FROM songplay
 GROUP BY location 
 ORDER BY cnt DESC 
 LIMIT 5
"""
)


# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
tests_queries = [test1, test2]
