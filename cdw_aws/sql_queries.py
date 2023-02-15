import configparser

# CONFIG
config = configparser.ConfigParser()
config.read_file(open('dwh.cfg'))

LOG_DATA = config.get('S3', 'LOG_DATA')
LOG_JSON_PATH = config.get('S3', 'LOG_JSON_PATH')
SONG_DATA = config.get('S3', 'SONG_DATA')
ARN = config.get('IAM_ROLE', 'ARN')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS stagingevents"
staging_songs_table_drop = "DROP TABLE IF EXISTS stagingsongs"
songplay_table_drop = "DROP TABLE IF EXISTS factsongplays"
user_table_drop = "DROP TABLE IF EXISTS dimusers"
song_table_drop = "DROP TABLE IF EXISTS dimsongs"
artist_table_drop = "DROP TABLE IF EXISTS dimartists"
time_table_drop = "DROP TABLE IF EXISTS dimtime"

# CREATE TABLES
#https://docs.aws.amazon.com/redshift/latest/dg/federated-data-types.html

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS stagingevents (
        artist        VARCHAR,
        auth          VARCHAR,
        firstname     VARCHAR,
        gender        VARCHAR,
        iteminsession INTEGER,
        lastname      VARCHAR,
        length        VARCHAR,
        level         VARCHAR,
        location      VARCHAR,
        method        VARCHAR,
        page          VARCHAR,
        registration  DOUBLE PRECISION,
        sessionid     INTEGER,
        song          VARCHAR,
        status        INTEGER,
        ts            BIGINT,
        useragent     VARCHAR,
        userid        VARCHAR
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS stagingsongs (
        num_songs          INTEGER,
        artist_id          VARCHAR,
        artist_latitude    DOUBLE PRECISION, 
        artist_longitude   DOUBLE PRECISION, 
        artist_location    VARCHAR,
        artist_name        VARCHAR,
        song_id            VARCHAR,
        title              VARCHAR,
        duration           DOUBLE PRECISION,
        year               INTEGER           
    );
""")

songplay_table_create = ("""
    CREATE TABLE IF NOT EXISTS factsongplays (
        songplay_id INTEGER IDENTITY(0,1),
        start_time  TIMESTAMP,
        user_id     VARCHAR,
        level       VARCHAR,
        song_id     VARCHAR,
        artist_id   VARCHAR,
        session_id  INTEGER,
        location    VARCHAR,
        user_agent  VARCHAR   
    )
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimusers (
        user_id    INTEGER,
        first_name VARCHAR,
        last_name  VARCHAR,
        gender     VARCHAR,
        level      VARCHAR 
    )
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimsongs (
        song_id   VARCHAR,
        title     VARCHAR,
        artist_id VARCHAR,
        year      INTEGER,
        duration  DOUBLE PRECISION 
    )
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimartists (
        artists_id VARCHAR,
        name       VARCHAR,
        location   VARCHAR,
        latitude   DOUBLE PRECISION,
        longitude  DOUBLE PRECISION 
    )
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS dimtime (
        startime TIMESTAMP,
        hour     INTEGER,
        day      INTEGER,
        week     INTEGER,
        month    INTEGER,
        year     INTEGER,
        weekday  INTEGER
    )
""")

# STAGING TABLES
# https://docs.aws.amazon.com/redshift/latest/dg/t_loading-tables-from-s3.html 
# https://docs.aws.amazon.com/redshift/latest/dg/r_COPY_command_examples.html#copy-from-json-examples-using-jsonpaths

staging_events_copy = ("""
    COPY {} FROM '{}'
    IAM_ROLE '{}'
    json '{}';
    """).format('stagingevents', 
                LOG_DATA, 
                ARN,
                LOG_JSON_PATH
            )

staging_songs_copy = ("""
    COPY {} FROM '{}'
    IAM_ROLE '{}'
    json 'auto';
    """).format('stagingsongs', 
                SONG_DATA,  
                ARN,
            )

# FINAL TABLES

# Casting BIGINT into TIMESTAMP has its hustle. Unfortunately is not possible
# to cast any integer into timestamp since there are certain rules such as the
# begginning of counting from 01/01/1970 ('epoch'), etc.
# if you encounter a "year is out of range" error the timestamp
# may be in milliseconds, try `ts /= 1000` in that case.
# https://www.postgresql.org/docs/current/functions-datetime.html
# https://www.postgresqltutorial.com/postgresql-tutorial/postgresql-cast/
# https://stackoverflow.com/questions/3682748/converting-unix-timestamp-string-to-readable-date
songplay_table_insert = ("""
    INSERT INTO factsongplays (
        start_time,
        user_id,
        level,
        song_id,
        artist_id,
        session_id,
        location,
        user_agent
                              )
    SELECT TIMESTAMP 'epoch' + se.ts/1000 * INTERVAL '1 second',
           se.userid :: INTEGER,
           se.level,
           ss.song_id,
           ss.artist_id,
           se.sessionid,
           se.location,
           se.useragent

    FROM stagingevents se 
    JOIN stagingsongs ss ON (se.song = ss.title 
                             AND se.artist = ss.artist_name)
    
    WHERE se.page = 'NextSong'
""")

user_table_insert = ("""
    INSERT INTO DimUsers(
        user_id,
        first_name,
        last_name,
        gender,
        level 
    )
    SELECT se.userid :: INTEGER,
           se.firstname,
           se.lastname,
           se.gender,
           se.level

    FROM stagingevents se

    WHERE se.page = 'NextSong'
""")

song_table_insert = ("""
    INSERT INTO DimSongs(
        song_id,
        title,
        artist_id,
        year,
        duration 
    )
    SELECT ss.song_id,
           ss.title,
           ss.artist_id,
           ss.year,
           ss.duration

    FROM stagingSongs ss
""")

artist_table_insert = ("""
    INSERT INTO DimArtists(
        artists_id,
        name,
        location,
        latitude,
        longitude 
    )
    SELECT ss.artist_id,
           ss.artist_name,
           ss.artist_location,
           ss.artist_latitude,
           ss. artist_longitude

    FROM stagingSongs ss
""")

# https://www.postgresqltutorial.com/postgresql-date-functions/postgresql-extract/
time_table_insert = ("""
    INSERT INTO DimTime(
        startime,
        hour,
        day,
        week,
        month,
        year,
        weekday
    )
    SELECT
        f.start_time,
        EXTRACT( HOUR FROM f.start_time ),
        EXTRACT( DAY FROM f.start_time ),
        EXTRACT( WEEK FROM f.start_time ),
        EXTRACT( MONTH FROM f.start_time ),
        EXTRACT( YEAR FROM f.start_time ),
        EXTRACT( DOW FROM f.start_time )

    FROM factSongPlays f

""")

# TESTING QUERIES

# COUNTING REGISTERS

counting_registers_se = ("""
    SELECT COUNT(*) FROM stagingEvents
    """) 

counting_registers_ss = (""" 
    SELECT COUNT(*) FROM stagingSongs
    """)

counting_registers_factSongPlays = (""" 
    SELECT COUNT(*) FROM factSongPlays
    """)

counting_registers_DimUsers = (""" 
    SELECT COUNT(*) FROM DimUsers 
    """)

counting_registers_DimSongs = (""" 
    SELECT COUNT(*) FROM DimSongs 
    """)

counting_registers_DimArtists = (""" 
    SELECT COUNT(*) FROM DimArtists 
    """)

counting_registers_DimTime = (""" 
    SELECT COUNT(*) FROM DimTime 
    """)

# EXPLORING QUERIES
# 1 - What is the TOP 10 most played songS?
# 2 - When is the highest usage time of day by hour?
# 3 - When is the highest usage time by weekday?
# 3 - When is the highest usage time of day by hour for song?
# 4 - When is the highest usage time of day by weekday for song?

most_played_song = (""" 
    SELECT song,
           COUNT(song) as songplay_count
    FROM stagingEvents
    GROUP BY song
    ORDER BY COUNT(song) DESC
    LIMIT 10;
    """)

highest_usage_hour = (""" 
    SELECT hour, 
           COUNT(hour) as highest_usage_hour
    FROM DimTime
    GROUP BY hour
    ORDER BY COUNT(hour) DESC;
    """)

highest_usage_weekday = (""" 
    SELECT weekday, 
           COUNT(weekday) as highest_usage_weekday
    FROM DimTime
    GROUP BY weekday
    ORDER BY COUNT(weekday) DESC;
    """)

highest_usage_per_song_hour = (""" 
    SELECT ds.title,
           dt.hour, 
           COUNT(ds.title) as times_played
    FROM factSongPlays fs 
    JOIN DimTime dt ON (dt.startime = fs.start_time) 
    JOIN DimSongs ds ON (ds.song_id = fs.song_id)
    GROUP BY ds.title, dt.hour
    ORDER BY times_played DESC
    LIMIT 10;
    """)

highest_usage_per_song_dow = (""" 
    SELECT ds.title,
           dt.weekday, 
           COUNT(ds.title) as times_played
    FROM factSongPlays fs 
    JOIN DimTime dt ON (dt.startime = fs.start_time) 
    JOIN DimSongs ds ON (ds.song_id = fs.song_id)
    GROUP BY ds.title, dt.wekday
    ORDER BY times_played DESC
    LIMIT 10;
    """)

# QUERY LISTS

create_table_queries = [staging_events_table_create, 
                        staging_songs_table_create, 
                        songplay_table_create, 
                        user_table_create, 
                        song_table_create, 
                        artist_table_create, 
                        time_table_create]

create_staging_table_queries = [staging_events_table_create, 
                                staging_songs_table_create]

create_star_table_queries = [songplay_table_create, 
                             user_table_create, 
                             song_table_create, 
                             artist_table_create, 
                             time_table_create]

drop_table_queries = [staging_events_table_drop, 
                      staging_songs_table_drop, 
                      songplay_table_drop, 
                      user_table_drop, 
                      song_table_drop, 
                      artist_table_drop, 
                      time_table_drop]

drop_staging_table_queries = [staging_events_table_drop, 
                              staging_songs_table_drop]

drop_star_table_queries = [songplay_table_drop, 
                           user_table_drop, 
                           song_table_drop, 
                           artist_table_drop, 
                           time_table_drop]

copy_table_queries = [staging_events_copy, 
                      staging_songs_copy]

insert_table_queries = [songplay_table_insert, 
                       user_table_insert, 
                       song_table_insert, 
                       artist_table_insert, 
                       time_table_insert]

counting_register_queries = [counting_registers_se,
                             counting_registers_ss,
                             counting_registers_factSongPlays,
                             counting_registers_DimUsers,
                             counting_registers_DimSongs,
                             counting_registers_DimArtists,
                             counting_registers_DimTime]

exploring_queries_list = [most_played_song,
                          highest_usage_hour,
                          highest_usage_weekday,
                          highest_usage_per_song_hour,
                          highest_usage_per_song_dow]
