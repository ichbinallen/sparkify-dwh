import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS staging_events;"
staging_songs_table_drop = "DROP TABLE IF EXISTS staging_songs;"
songplay_table_drop = "DROP TABLE IF EXISTS songplay;"
user_table_drop = "DROP TABLE IF EXISTS dim_user;"
song_table_drop = "DROP TABLE IF EXISTS song;"
artist_table_drop = "DROP TABLE IF EXISTS artist;"
time_table_drop = "DROP TABLE IF EXISTS time;"


# CREATE TABLES

staging_events_table_create= ("""
CREATE TABLE staging_events (
    artist VARCHAR(40),
    auth VARCHAR(20),
    firstName VARCHAR (20),
    gender VARCHAR(1),
    itemInSession INT,
    lastName VARCHAR(20),
    length REAL,
    level VARCHAR(40),
    location VARCHAR(80),
    method VARCHAR(20),
    page VARCHAR(20),
    registration VARCHAR(80),
    sessionID INT,
    song VARCHAR(80),
    status INT,
    ts timestamp,
    userAgent TEXT,
    userID INT
);
""")

staging_songs_table_create = ("""
CREATE TABLE staging_songs (
    num_songs INT,
    artist_id VARCHAR(40),
    artist_lattitude REAL,
    artist_longitude REAL,
    artist_location VARCHAR(40),
    artist_name VARCHAR(40),
    song_id VARCHAR(40),
    title VARCHAR(80),
    duration REAL,
    year INT
);
""")

songplay_table_create = ("""
CREATE TABLE songplay (
  songplay_id INT IDENTITY(1,1) PRIMARY KEY,
  start_time timestamp REFERENCES time(start_time),
  user_id INT REFERENCES dim_user(user_id),
  level   VARCHAR(40),
  song_id VARCHAR(40) REFERENCES song(song_id),
  artist_id VARCHAR(40) REFERENCES artist(artist_id),
  session_id INT,
  location VARCHAR(40)
);
""")

user_table_create = ("""
CREATE TABLE dim_user (
    user_id INT PRIMARY KEY,
    first_name VARCHAR(40),
    last_name VARCHAR(40),
    gender VARCHAR(10)
);
""")

song_table_create = ("""
CREATE TABLE song (
    song_id VARCHAR(40) PRIMARY KEY,
    title VARCHAR(80),
    artist_id VARCHAR(40) REFERENCES artist(artist_id),
    year INT,
    duration REAL
);
""")

artist_table_create = ("""
CREATE TABLE artist (
    artist_id VARCHAR(40) PRIMARY KEY,
    name VARCHAR(40),
    location VARCHAR(40),
    lattitude REAL,
    longitude REAL
);
""")

time_table_create = ("""
CREATE TABLE time (
    start_time timestamp PRIMARY KEY,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
);
""")

# STAGING TABLES
staging_events_copy = ("""
COPY staging_events
FROM {}
ACCESS_KEY_ID '{}'
SECRET_ACCESS_KEY '{}'
JSON AS {}
REGION 'us-west-2'
TIMEFORMAT as 'epochmillisecs'
;
""").format(
    config["S3"]["LOG_DATA"],
    config["IAM_ROLE"]["KEY"],
    config["IAM_ROLE"]["SECRET"],
    config["S3"]["LOG_JSONPATH"]
)


staging_songs_copy = ("""
COPY staging_songs
FROM {}
IAM_ROLE AS 'aws_iam_role={}'
REGION 'us-west-2'
JSON
""").format(config["S3"]["SONG_DATA"], config["IAM_ROLE"]["ARN"])

# FINAL TABLES

songplay_table_insert = ("""
INSERT INTO songplay (
    --songplay_id, using INT IDENTITY so we can omit
    start_time,
    user_id,
    level,
    song_id,
    artist_id,
    session_id,
    location
    )
SELECT 
    E.ts AS start_time,
    E.userId AS user_id,
    E.level AS level,
    S.song_id AS song_id,
    S.artist_id AS artist_id,
    E.sessionID as session_id,
    E.location as location
FROM
    staging_events AS E
JOIN
    staging_songs AS S
ON
    E.song=S.title AND
    E.artist=S.artist_name AND
    E.page='NextSong'
; 
""")

user_table_insert = ("""
INSERT INTO dim_user (
    user_id, 
    first_name, 
    last_name, 
    gender
)
SELECT DISTINCT
    E.userID as user_id,
    E.firstName as first_name,
    E.lastName as last_name,
    E.gender as gender
FROM
    staging_events as E
WHERE 
    E.page='NextSong'
;    
""")

song_table_insert = ("""
INSERT INTO song (
    song_id,
    title,
    artist_id,
    year,
    duration
)
SELECT DISTINCT 
    S.song_id AS song_id,
    S.title AS title,
    S.artist_id AS artist_id,
    S.year AS year,
    S.duration AS duration
FROM
    staging_songs S
;
""")

artist_table_insert = ("""
INSERT INTO artist (
    artist_id,
    name,
    location,
    lattitude,
    longitude
)
SELECT DISTINCT
    artist_id AS artist_id,
    artist_name AS name,
    artist_location AS location,
    artist_latitude AS lattitude,
    artist_longitude AS longitude
;
""")

time_table_insert = ("""
INSERT INTO time (
    start_time,
    hour INT,
    day INT,
    week INT,
    month INT,
    year INT,
    weekday INT
)
SELECT DISTINCT
    ts AS start_time,
    DATE_PART('hour', ts) AS hour INT,
    DATE_PART('day', ts)  AS day INT,
    DATE_PART('week', ts) AS week INT,
    DATE_PART('week', ts) AS month INT,
    DATE_PART('year', ts) AS year INT,
    DATE_PART('dow', ts)  AS weekday INT
FROM 
    staging_events AS E
WHERE 
    E.page='NextSong'
""")

# QUERY LISTS

create_table_queries = [
    staging_events_table_create, 
    staging_songs_table_create, 
    artist_table_create,
    song_table_create,
    user_table_create,
    time_table_create,
    songplay_table_create   
]
drop_table_queries = [
    staging_events_table_drop,
    staging_songs_table_drop,
    songplay_table_drop,
    user_table_drop,
    song_table_drop,
    artist_table_drop,
    time_table_drop
]
copy_table_queries = [
    staging_events_copy,
    staging_songs_copy
]
insert_table_queries = [
    songplay_table_insert,
    user_table_insert,
    song_table_insert,
    artist_table_insert,
    time_table_insert
]
