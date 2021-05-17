import configparser


# CONFIG
config = configparser.ConfigParser()
config.read("dwh.cfg")

IAM_ROLE_ARN       = config.get("IAM_ROLE", "IAM_ROLE_ARN")
S3_BUCKET_REGION   = config.get("S3", "BUCKET_REGION")
S3_LOG_DATA        = config.get("S3", "LOG_DATA")
S3_LOG_JSONPATH    = config.get("S3", "LOG_JSONPATH")
S3_SONG_DATA       = config.get("S3", "SONG_DATA")

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays;"
user_table_drop = "DROP TABLE IF EXISTS users;"
song_table_drop = "DROP TABLE IF EXISTS songs;"
artist_table_drop = "DROP TABLE IF EXISTS artists;"
time_table_drop = "DROP TABLE IF EXISTS time;"

# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE IF NOT EXISTS staging_events (
    artist           VARCHAR    NULL,
    auth             VARCHAR    NULL,
    firstName        VARCHAR    NULL,
    gender           VARCHAR    NULL,
    itemInSession    VARCHAR    NULL,
    lastName         VARCHAR    NULL,
    length           VARCHAR    NULL,
    level            VARCHAR    NULL,
    location         VARCHAR    NULL,
    method           VARCHAR    NULL,
    page             VARCHAR    NULL,
    registration     VARCHAR    NULL,
    sessionId        INT        NOT NULL,
    song             VARCHAR    NULL,
    status           INT        NULL,
    ts               BIGINT     NOT NULL,
    userAgent        VARCHAR    NULL,
    userId           INT        NULL
) DISTSTYLE EVEN;
""")

staging_songs_table_create = ("""
CREATE TABLE IF NOT EXISTS staging_songs (
    num_songs           INT              NULL,
    artist_id           VARCHAR          NOT NULL,
    artist_latitude     DECIMAL(10,8)    NULL,
    artist_longitude    DECIMAL(11,8)    NULL,
    artist_location     VARCHAR          NULL,
    artist_name         VARCHAR          NOT NULL,
    song_id             VARCHAR          NOT NULL,
    title               VARCHAR          NOT NULL,
    duration            DECIMAL(10,6)    NULL,
    year                INT              NULL
) DISTSTYLE EVEN;
""")

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (
    songplay_id    INTEGER IDENTITY(0,1)   NOT NULL,
    start_time     TIMESTAMP               NOT NULL,
    user_id        VARCHAR                 NOT NULL DISTKEY,
    level          VARCHAR                 NOT NULL,
    song_id        VARCHAR                 NOT NULL,
    artist_id      VARCHAR                 NOT NULL,
    session_id     VARCHAR                 NOT NULL SORTKEY,
    location       VARCHAR                 NULL,
    user_agent     VARCHAR                 NULL,
    PRIMARY KEY (songplay_id)
);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users (
    user_id       INTEGER    NOT NULL SORTKEY,
    first_name    VARCHAR    NOT NULL,
    last_name     VARCHAR    NOT NULL,
    gender        VARCHAR    NOT NULL,
    level         VARCHAR    NOT NULL,
    PRIMARY KEY (user_id)
) DISTSTYLE ALL;
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs (
    song_id     VARCHAR          NOT NULL SORTKEY,
    title       VARCHAR          NOT NULL,
    artist_id   VARCHAR          NOT NULL DISTKEY,
    year        INTEGER          NULL,
    duration    DECIMAL(10,6)    NULL,
    PRIMARY KEY (song_id)
);
""")

# data type recommendation for latitude and longitude
# https://dba.stackexchange.com/questions/107089/decimal-or-point-data-type-for-storing-geo-location-data-in-mysql#:~:text=p.,DECIMAL(11%2C8)%20.
artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists (
    artist_id   VARCHAR         NOT NULL SORTKEY,
    name        VARCHAR         NOT NULL,
    location    VARCHAR         NULL,
    latitude    DECIMAL(10,8)   NULL,
    longitude   DECIMAL(11,8)   NULL,
    PRIMARY KEY (artist_id)
) DISTSTYLE ALL;
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time (
    start_time  TIMESTAMP    NOT NULL SORTKEY,
    hour        SMALLINT     NOT NULL,
    day         SMALLINT     NOT NULL,
    week        SMALLINT     NOT NULL,
    month       SMALLINT     NOT NULL,
    year        SMALLINT     NOT NULL,
    weekday     SMALLINT     NOT NULL,
    PRIMARY KEY (start_time)
) DISTSTYLE ALL;
""")

# STAGING TABLES

staging_events_copy = ("""
COPY staging_events FROM {}
     credentials 'aws_iam_role={}'
     format as json {}
     STATUPDATE ON
     region '{}';
""").format(S3_LOG_DATA, IAM_ROLE_ARN, S3_LOG_JSONPATH, S3_BUCKET_REGION)

staging_songs_copy = ("""
COPY staging_songs FROM {}
     credentials 'aws_iam_role={}'
     format as json 'auto'
     STATUPDATE ON
     region '{}';
""").format(S3_SONG_DATA, IAM_ROLE_ARN, S3_BUCKET_REGION)

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplays (
        start_time
       ,user_id
       ,level
       ,song_id
       ,artist_id
       ,session_id
       ,location
       ,user_agent
    )
    SELECT  DISTINCT TIMESTAMP 'epoch' + ev.ts/1000 * INTERVAL '1 second'   AS start_time,
            ev.userId                   AS user_id,
            ev.level                    AS level,
            so.song_id                  AS song_id,
            so.artist_id                AS artist_id,
            ev.sessionId                AS session_id,
            ev.location                 AS location,
            ev.userAgent                AS user_agent
    FROM staging_events AS ev
    JOIN staging_songs AS so
        ON (
                ev.artist = so.artist_name
            AND ev.song = so.title
            )
    WHERE ev.page = 'NextSong';
""")

# for users, we will use the RANK () window function partitioned by userId and descending time stamp and filter the first row
# this ensures, that we will get the most recent user entry to load into the users table
user_table_insert = ("""
INSERT INTO users (
        user_id
       ,first_name
       ,last_name
       ,gender
       ,level
    )
    SELECT  user_id
           ,first_name
           ,last_name
           ,gender
           ,level
    FROM (
        SELECT  userId                                              AS user_id
               ,firstName                                           AS first_name
               ,lastName                                            AS last_name
               ,gender                                              AS gender
               ,level                                               AS level
               ,RANK () OVER (PARTITION BY userId ORDER BY ts desc) AS row_number
        FROM staging_events
        WHERE page ='NextSong'
     )
     WHERE row_number = 1;
""")

song_table_insert = ("""
INSERT INTO songs (
        song_id
       ,title
       ,artist_id
       ,year
       ,duration
    )
    SELECT  DISTINCT song_id AS song_id
           ,title            AS title
           ,artist_id        AS artist_id
           ,year             AS year
           ,duration         AS duration
    FROM staging_songs;
""")

artist_table_insert = ("""
INSERT INTO artists (
        artist_id
       ,name
       ,location
       ,latitude
       ,longitude
    )
    SELECT  DISTINCT artist_id                      AS artist_id
           ,artist_name                             AS name
           ,artist_location                         AS location
           ,CAST(artist_latitude AS DECIMAL(10,8))  AS latitude
           ,CAST(artist_longitude AS DECIMAL(11,8)) AS longitude
    FROM staging_songs;
""")

time_table_insert = ("""
INSERT INTO time (
        start_time
       ,hour
       ,day
       ,week
       ,month
       ,year
       ,weekday
    )
    SELECT  DISTINCT TIMESTAMP 'epoch' + ts / 1000 * INTERVAL '1 second' AS start_time
           ,EXTRACT(hour FROM start_time)                                AS hour
           ,EXTRACT(day FROM start_time)                                 AS day
           ,EXTRACT(week FROM start_time)                                AS week
           ,EXTRACT(month FROM start_time)                               AS month
           ,EXTRACT(year FROM start_time)                                AS year
           ,EXTRACT(dayofweek FROM start_time)                           AS weekday
    FROM staging_events
    WHERE page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
