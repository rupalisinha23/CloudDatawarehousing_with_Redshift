import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')

# DROP TABLES

staging_events_table_drop = "DROP TABLE IF EXISTS log_staging CASCADE;"
staging_songs_table_drop = "DROP TABLE IF EXISTS song_staging CASCADE;"
songplay_table_drop = "DROP TABLE IF EXISTS songplays CASCADE;"
user_table_drop = "DROP TABLE IF EXISTS users CASCADE;"
song_table_drop = "DROP TABLE IF EXISTS songs CASCADE;"
artist_table_drop = "DROP TABLE IF EXISTS artists CASCADE;"
time_table_drop = "DROP TABLE IF EXISTS time CASCADE;"

# CREATE TABLES

# staging tables
staging_events_table_create= ("""
    CREATE TABLE IF NOT EXISTS log_staging (
        artist TEXT,
        auth TEXT,
        firstName TEXT,
        gender TEXT,
        itemInSession INT,
        lastName TEXT,
        length FLOAT8,
        level TEXT,
        location TEXT,
        method TEXT,
        page TEXT,
        registration FLOAT8,
        sessionId INT,
        song TEXT,
        status INT,
        ts TIMESTAMP,
        userAgent TEXT,
        userId INT);
""")

staging_songs_table_create = ("""
    CREATE TABLE IF NOT EXISTS song_staging (
        num_songs INT,
        artist_id TEXT,
        artist_latitude FLOAT8,
        artist_longitude FLOAT8,
        artist_location TEXT,
        artist_name TEXT,
        song_id TEXT PRIMARY KEY,
        title TEXT,
        duration FLOAT8,
        year INT);
""")

# fact table
songplay_table_create = ("""CREATE TABLE IF NOT EXISTS songplays (
    songpplay_id INT IDENTITY PRIMARY KEY,
    start_time TIMESTAMP NOT NULL REFERENCES time(start_time) sortkey,
    user_id INT NOT NULL REFERENCES users(user_id),
    level TEXT NOT NULL,
    song_id TEXT NOT NULL REFERENCES songs(song_id),
    artist_id TEXT NOT NULL REFERENCES artists(artist_id) distkey,
    session_id INT NOT NULL,
    location TEXT NOT NULL,
    user_agent TEXT NOT NULL
    );

""")

# dimension tables; users; songs; artists; time
user_table_create = ("""
    CREATE TABLE IF NOT EXISTS users (
        user_id INT PRIMARY KEY sortkey,
        first_name TEXT NOT NULL,
        last_name TEXT NOT NULL,
        gender TEXT NOT NULL,
        level TEXT NOT NULL)
        diststyle ALL;
""")

song_table_create = ("""
    CREATE TABLE IF NOT EXISTS songs (
        song_id TEXT PRIMARY KEY sortkey,
        title TEXT NOT NULL,
        artist_id TEXT NOT NULL,
        year INT NOT NULL,
        duration NUMERIC NOT NULL)
        diststyle ALL;
""")

artist_table_create = ("""
    CREATE TABLE IF NOT EXISTS artists (
        artist_id TEXT PRIMARY KEY distkey,
        name TEXT NOT NULL,
        location TEXT NOT NULL,
        latitude FLOAT8 NOT NULL,
        longitude FLOAT NOT NULL);
""")

time_table_create = ("""
    CREATE TABLE IF NOT EXISTS time (
        start_time TIMESTAMP PRIMARY KEY sortkey,
        hour INT NOT NULL,
        day INT NOT NULL,
        week INT NOT NULL,
        month INT NOT NULL,
        year INT NOT NULL,
        weekday INT NOT NULL);
""")

# STAGING TABLES; copy from s3 buckets; aws commands
copy_song_command = """
                    COPY {}.{}
                    FROM '{}' 
                    iam_role '{}'
                    region 'us-west-2'
                    JSON 'auto'
                    timeformat 'epochmillisecs';
                    """
copy_log_command = """
                   COPY {}.{} 
                   FROM '{}' 
                   iam_role '{}' 
                   region 'us-west-2' 
                   FORMAT AS JSON '{}' 
                   timeformat 'epochmillisecs'
                   """

# FINAL TABLES

songplay_table_insert = ("""
    INSERT INTO sparkify.songplays (start_time, user_id, level, song_id, artist_id,
                                     session_id, location, user_agent) 
         SELECT DISTINCT sl.ts, 
                         sl.userId, 
                         nvl(sl.level, 'empty'), 
                         ss.song_id, 
                         ss.artist_id,
                         sl.sessionId, 
                         nvl(sl.location, 'empty'), 
                         nvl(sl.userAgent, 'empty')
         FROM sparkify.log_staging sl
         INNER JOIN sparkify.song_staging ss ON sl.song = ss.title
         WHERE sl.page = 'NextSong';
""")

user_table_insert = ("""
    INSERT INTO sparkify.users (user_id, first_name, last_name, gender, level)
        SELECT DISTINCT sl.userId,
                        nvl(sl.firstName, 'empty'),
                        nvl(sl.lastName, 'empty'),
                        nvl(sl.gender, 'empty'),
                        nvl(sl.level, 'empty')
        FROM sparkify.log_staging sl
        WHERE sl.userId IS NOT NULL;
""")

song_table_insert = ("""
    INSERT INTO sparkify.songs (song_id, title, artist_id, year, duration)
        SELECT DISTINCT ss.song_id,
                        ss.title,
                        ss.artist_id,
                        ss.year,
                        nvl(ss.duration, 0.0)
        FROM sparkify.song_staging ss;
""")

artist_table_insert = ("""
    INSERT INTO sparkify.artists (artist_id, name, location, latitude, longitude)
        SELECT DISTINCT ss.artist_id,
                        ss.artist_name,
                        nvl(ss.artist_location, 'empty'),
                        nvl(ss.artist_latitude, 0.0),
                        nvl(ss.artist_longitude, 0.0)
        FROM sparkify.song_staging ss
        WHERE ss.artist_id IS NOT NULL;
""")

time_table_insert = ("""
    INSERT INTO sparkify.time (start_time, hour, day, week, month, year, weekday)
        SELECT DISTINCT sl.ts, 
                         DATE_PART(hour, sl.ts) :: INTEGER, 
                         DATE_PART(day, sl.ts) :: INTEGER, 
                         DATE_PART(week, sl.ts) :: INTEGER,
                         DATE_PART(month, sl.ts) :: INTEGER,
                         DATE_PART(year, sl.ts) :: INTEGER,
                         DATE_PART(dow, sl.ts) :: INTEGER
         FROM sparkify.log_staging sl
         WHERE sl.page = 'NextSong';
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, user_table_create, song_table_create, artist_table_create, time_table_create, songplay_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
