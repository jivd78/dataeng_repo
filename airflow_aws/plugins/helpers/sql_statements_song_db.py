#CREATE STAGING TABLEA queries:

CREATE_STAGING_EVENTS_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS staging_events (
        artist varchar(256),
        auth varchar(256),
        firstname varchar(256),
        gender varchar(256),
        iteminsession int4,
        lastname varchar(256),
        length numeric(18,0),
        "level" varchar(256),
        location varchar(256),
        "method" varchar(256),
        page varchar(256),
        registration numeric(18,0),
        sessionid int4,
        song varchar(256),
        status int4,
        ts int8,
        useragent varchar(256),
        userid int4
        );
    """)

CREATE_STAGING_SONGS_TABLE_SQL = ("""
    CREATE TABLE IF NOT EXISTS staging_songs (
        num_songs int4,
        artist_id varchar,
        artist_latitude numeric(18,0),
        artist_longitude numeric(18,0),
        artist_location varchar(256),
        artist_name varchar(500),
        song_id varchar(256),
        title varchar(256),
        duration numeric(18,0),
        "year" int4
    );
""")

#CREATING STAR TABLES QUERIES: ######################################
CREATE_FACT_SONGPLAYS_TABLE = ("""
    CREATE TABLE IF NOT EXISTS factsongplays (
        songplay_id VARCHAR NOT NULL PRIMARY KEY,
        start_time  TIMESTAMP NOT NULL,
        user_id     VARCHAR NOT NULL,
        "level"     VARCHAR,
        song_id     VARCHAR(256),
        artist_id   VARCHAR,
        session_id  INTEGER,
        location    VARCHAR,
        user_agent  VARCHAR
    );
""")

CREATE_DIMUSERS_TABLE = ("""
    CREATE TABLE IF NOT EXISTS dimusers (
        user_id    INTEGER NOT NULL PRIMARY KEY,
        first_name VARCHAR,
        last_name  VARCHAR,
        gender     VARCHAR,
        "level"      VARCHAR 
    )
""")
CREATE_DIMTIME_TABLE = ("""
    CREATE TABLE IF NOT EXISTS dimtime (
        startime TIMESTAMP NOT NULL PRIMARY KEY,
        hour     INTEGER,
        day      INTEGER,
        week     INTEGER,
        month    INTEGER,
        year     INTEGER,
        weekday  INTEGER
    )
""")
CREATE_DIMSONGS_TABLE = ("""
    CREATE TABLE IF NOT EXISTS dimsongs (
        song_id   VARCHAR NOT NULL PRIMARY KEY,
        title     VARCHAR,
        artist_id VARCHAR NOT NULL,
        year      INTEGER,
        duration  DOUBLE PRECISION
        )
""")
CREATE_DIMARTISTS_TABLE = ("""
    CREATE TABLE IF NOT EXISTS dimartists (
        artist_id VARCHAR NOT NULL PRIMARY KEY,
        name       VARCHAR(500),
        location   VARCHAR,
        latitude   DOUBLE PRECISION,
        longitude  DOUBLE PRECISION 
    )
    """)

#COPY queries: ##########################################

COPY_SQL = """
    COPY {}
    FROM '{}'
    ACCESS_KEY_ID '{{}}'
    SECRET_ACCESS_KEY '{{}}'
    IGNOREHEADER 1
    DELIMITER ','
"""

COPY_SQL_JSON = """
        COPY {}
        FROM '{}'
        ACCESS_KEY_ID '{{}}'
        SECRET_ACCESS_KEY '{{}}'
        json 'auto';
    """

COPY_MONTHLY_EVENTS_SQL = COPY_SQL_JSON.format(
    #copy {}
    "staging_events",
    #from{}
    "s3://airflow-pipelines-jivd/data-pipelines/log-data/{year}/{month}/{year}-{month}-{day}-events.json"
)


#INSERT INTO queries#############################################

#INSERT INTO factsongplays comes in the operator

#AND se.ts IS NOT NULL
#AND se.userid IS NOT NULL
#AND ss.song_id IS NOT NULL
#AND ss.artist_id IS NOT NULL 
FACT_SONGPLAYS_TABLE_DELET_LOAD_INSERT = ("""
        (
        songplay_id,  
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
        )
    SELECT DISTINCT 
        md5(se.sessionid || TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second',
        se.userid :: INTEGER,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionid,
        se.location,
        se.useragent

    FROM (SELECT * FROM staging_events 
            WHERE page = 'NextSong' ) se
    
    JOIN staging_songs ss 
        ON (se.song = ss.title 
            AND se.artist = ss.artist_name)
    
    
""")
#AND se.ts IS NOT NULL
#        AND se.userid IS NOT NULL
#        AND ss.song_id IS NOT NULL
#        AND ss.artist_id IS NOT NULL

#INSERT INTO factsongplays comes in the operator
FACT_SONGPLAYS_TABLE_INSERT_ONLY = ("""
        (
        songplay_id,  
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
        )
    SELECT DISTINCT 
        md5(se.sessionid || TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second'),
        TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second',
        se.userid :: INTEGER,
        se.level,
        ss.song_id,
        ss.artist_id,
        se.sessionid,
        se.location,
        se.useragent

    FROM staging_events se 
    JOIN staging_songs ss ON (se.song = ss.title 
                             AND se.artist = ss.artist_name
                             AND se.length = ss.duration)
    
    WHERE se.page = 'NextSong' 
""")
#AND se.ts IS NOT NULL
#        AND se.userid IS NOT NULL
#        AND ss.song_id IS NOT NULL
#        AND ss.artist_id IS NOT NULL

DIMUSERS_TABLE_INSERT_TO = ("""
    (
        user_id,
        first_name,
        last_name,
        gender,
        level 
    )
    SELECT DISTINCT 
        se.userid :: INTEGER,
        se.firstname,
        se.lastname,
        se.gender,
        se.level

    FROM staging_events se

    WHERE se.page = 'NextSong' 
        AND se.userid IS NOT NULL
""")

DIMSONGS_TABLE_INSERT_TO = ("""
    (
        song_id,
        title,
        artist_id,
        year,
        duration 
    )
    SELECT DISTINCT 
        ss.song_id,
        ss.title,
        ss.artist_id,
        ss.year,
        ss.duration

    FROM staging_Songs ss

    WHERE ss.song_id IS NOT NULL 
        AND artist_id IS NOT NULL
""")

DIMARTISTS_TABLE_INSERT_TO = ("""
    (
        artist_id,
        name,
        location,
        latitude,
        longitude 
    )
    SELECT DISTINCT 
        ss.artist_id,
        ss.artist_name,
        ss.artist_location,
        ss.artist_latitude,
        ss. artist_longitude

    FROM staging_Songs ss

    WHERE ss.artist_id IS NOT NULL
""")

# https://www.postgresqltutorial.com/postgresql-date-functions/postgresql-extract/
DIMTIME_TABLE_INSERT_TO = ("""
    (
        startime,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT DISTINCT
        f.start_time,
        EXTRACT( HOUR FROM f.start_time ),
        EXTRACT( DAY FROM f.start_time ),
        EXTRACT( WEEK FROM f.start_time ),
        EXTRACT( MONTH FROM f.start_time ),
        EXTRACT( YEAR FROM f.start_time ),
        EXTRACT( DOW FROM f.start_time )

    FROM factSongPlays f

    WHERE f.start_time IS NOT NULL

""")

#Quality Check Queries #########################################################

factsongplays_check_nulls = ("""
        SELECT COUNT(*)
        FROM factsongplays
        WHERE   songplay_id IS NULL OR
                start_time IS NULL OR
                user_id IS NULL;
    """)

dimusers_check_nulls = ("""
        SELECT COUNT(*)
        FROM dimusers
        WHERE user_id IS NULL;
    """)

dimsongs_check_nulls = ("""
        SELECT COUNT(*)
        FROM dimsongs
        WHERE song_id IS NULL;
    """)

dimartists_check_nulls = ("""
        SELECT COUNT(*)
        FROM dimartists
        WHERE artist_id IS NULL;
    """)

dimtime_check_nulls = ("""
        SELECT COUNT(*)
        FROM dimtime
        WHERE startime IS NULL;
    """)

    # Data quality check queries:
factsongplays_check_count = ("""
        SELECT COUNT(*)
        FROM factsongplays;
    """)

dimusers_check_count = ("""
        SELECT COUNT(*)
        FROM dimusers;
    """)

dimsongs_check_count = ("""
        SELECT COUNT(*)
        FROM dimsongs;
    """)

dimartists_check_count = ("""
        SELECT COUNT(*)
        FROM dimartists;
    """)

dimtime_check_count = ("""
        SELECT COUNT(*)
        FROM dimtime;
    """)