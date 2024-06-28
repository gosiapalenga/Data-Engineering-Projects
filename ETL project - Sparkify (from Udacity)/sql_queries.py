import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


LOG_DATA           = config.get('S3', 'LOG_DATA')              
LOG_JSONPATH       = config.get('S3', 'LOG_JSONPATH')         
SONG_DATA          = config.get('S3', 'SONG_DATA')             
S3_ACCESS_ROLE_ARN = config.get('IAM','S3_ACCESS_ROLE_ARN')                             




# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_log_data;"
staging_songs_table_drop  = "DROP TABLE IF EXISTS staging_songs_data;"
songplay_table_drop       = "DROP TABLE IF EXISTS songplays;"
user_table_drop           = "DROP TABLE IF EXISTS users;"
song_table_drop           = "DROP TABLE IF EXISTS songs;"
artist_table_drop         = "DROP TABLE IF EXISTS artists;"
time_table_drop           = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS public.staging_log_data (
                event_id            BIGINT IDENTITY(0,1) NOT NULL,
                artist              VARCHAR,
                auth                VARCHAR,
                firstName           VARCHAR,
                gender              VARCHAR,
                itemInSession       VARCHAR,
                lastName            VARCHAR,
                length              VARCHAR,
                level               VARCHAR,
                location            VARCHAR,
                method              VARCHAR,
                page                VARCHAR,
                registration        NUMERIC(14,1),
                sessionId           INTEGER              NOT NULL sortkey distkey,
                song                VARCHAR,
                status              INTEGER,
                ts                  BIGINT               NOT NULL,
                userAgent           VARCHAR,
                userId              INTEGER
    );
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS public.staging_songs_data (
                num_songs           INTEGER,         
                artist_id           VARCHAR              NOT NULL sortkey distkey,
                artist_latitude     VARCHAR,     
                artist_longitude    VARCHAR,     
                artist_location     VARCHAR(500),
                artist_name         VARCHAR(500),
                song_id             VARCHAR,     
                title               VARCHAR(500),
                duration            DECIMAL(9),  
                year                INTEGER
    );
""")

songplay_table_create = ("""
       CREATE TABLE IF NOT EXISTS songplays (
                songplay_id  INTEGER IDENTITY(0,1) NOT NULL sortkey,
                start_time   timestamp,
                user_id      int                   NOT NULL distkey,
                level        varchar(10),
                song_id      varchar(20),
                artist_id    varchar(20),
                session_id   int,
                location     varchar(500),
                user_agent   varchar(500)
    );
""")

user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
                user_id      int                   NOT NULL sortkey,
                first_name   varchar(100),
                last_name    varchar(100),
                gender       varchar(1),
                level        varchar(10)
    );
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
                song_id      varchar(20)           NOT NULL sortkey,
                title        varchar(200),
                artist_id    varchar(100),
                year         int,
                duration     decimal(9)
    );
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
                artist_id    varchar(20)           NOT NULL sortkey,
                name         varchar(100),
                location     varchar(200),
                latitude     decimal(9),
                longitude    decimal(9)
    );
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
                start_time   timestamp             NOT NULL sortkey,
                hour         smallint,
                day          smallint,
                week         smallint,
                month        smallint,
                year         smallint,
                weekday      smallint
    );
""")

# STAGING TABLES

staging_events_copy = ("""
    COPY staging_log_data
    FROM {}
    iam_role {}
    format as json {}
    STATUPDATE ON;
""").format(LOG_DATA, S3_ACCESS_ROLE_ARN, LOG_JSONPATH)

staging_songs_copy = ("""
    COPY staging_songs_data
    FROM {}
    iam_role {}
    format as json 'auto'
    ACCEPTINVCHARS AS '^'
    STATUPDATE ON;  
""").format(SONG_DATA, S3_ACCESS_ROLE_ARN)

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO songplays (            start_time,
                                       user_id,
                                       level,
                                       song_id,
                                       artist_id,
                                       session_id,
                                       location,
                                       user_agent)
    SELECT DISTINCT timestamp 'epoch' + ld.ts/1000 * \
                interval '1 second' AS start_time,
                ld.userid           AS user_id,
                ld.level            AS level,
                sd.song_id          AS song_id,
                sd.artist_id        AS artist_id,
                ld.sessionid        AS session_id,
                ld.location         AS location,
                ld.useragent        AS user_agent
    FROM public.staging_log_data ld
    JOIN public.staging_songs_data sd
    ON (ld.artist = sd.artist_name);
""")

user_table_insert = ("""
    INSERT INTO users (                user_id,
                                       first_name,
                                       last_name,
                                       gender,
                                       level)
    SELECT DISTINCT userId          AS user_id,
                firstName           AS first_name,
                lastName            AS last_name,
                gender              AS gender,
                level               AS level
    FROM public.staging_log_data
    WHERE userid IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO songs (              song_id,
                                     title,
                                     artist_id,
                                     year,
                                     duration)
    SELECT DISTINCT song_id       AS song_id,
                title             AS title,
                artist_id         AS artist_id,
                year              AS year,
                duration          AS duration
    FROM public.staging_songs_data;
""")

artist_table_insert = ("""
    INSERT INTO artists (            artist_id,
                                     name,
                                     location,
                                     latitude,
                                     longitude)
    SELECT DISTINCT artist_id     AS artist_id,
                artist_name       AS name,
                artist_location   AS location,
                artist_latitude   AS latitude,
                artist_longitude  AS longitude
    FROM public.staging_songs_data;
""")

time_table_insert = ("""
    INSERT INTO time (                                start_time,
                                                      hour,
                                                      day,
                                                      week,
                                                      month,
                                                      year,
                                                      weekday)
    SELECT DISTINCT timestamp 'epoch' + ts/1000 * \
                interval '1 second'                AS start_time,
                EXTRACT(hour from start_time)      AS hour,
                EXTRACT(day from start_time)       AS day,
                EXTRACT(year from start_time)      AS week,
                EXTRACT(month from start_time)     AS month,
                EXTRACT(year from start_time)      AS year,
                EXTRACT(dayofweek from start_time) AS weekday
    FROM public.staging_log_data;
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]