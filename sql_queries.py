import configparser


# CONFIG
config = configparser.ConfigParser()
config.read('dwh.cfg')


LOG_DATA = config.get("S3", "LOG_DATA")
LOG_JSONPATH = config.get("S3", "LOG_JSONPATH")
SONG_DATA = config.get("S3", "SONG_DATA")
ARN = config.get("IAM_ROLE", "ARN")

# DROP TABLES
drop_statement = "DROP TABLE IF EXISTS "

staging_events_table_drop = drop_statement + "staging_events;"
staging_songs_table_drop = drop_statement + "staging_songs;"
songplay_table_drop = drop_statement + "songplay;"
user_table_drop = drop_statement + "users;"
song_table_drop = drop_statement + "songs;"
artist_table_drop = drop_statement + "artists;"
time_table_drop = drop_statement +  "time;"

# CREATE TABLES

create_statement = "CREATE TABLE IF NOT EXISTS "

staging_events_table_create= create_statement + ("""staging_events (artist varchar, auth varchar, firstName varchar, gender varchar, 
itemInSession int, lastName varchar, length float, level varchar, location varchar, method varchar, page varchar, registration float, sessionId int, 
song varchar, status int, ts timestamp, userAgent varchar, userId int);
""")

staging_songs_table_create = create_statement + ("""staging_songs (artist_id varchar, artist_latitude float, artist_location varchar,
artist_longitude float, artist_name varchar, duration float, num_songs int, song_id varchar, title varchar, year int);
""")

songplay_table_create = create_statement + ("""songplays(songplay_id INT IDENTITY(0,1) PRIMARY KEY, start_time timestamp NOT NULL, level varchar, user_id int NOT NULL, song_id varchar, artist_id varchar, session_id int, location varchar, user_agent varchar);
""")

user_table_create = create_statement + ("""users(user_id int PRIMARY KEY, first_name varchar, last_name varchar, gender varchar, level varchar);
""")

song_table_create = create_statement + ("""songs(song_id varchar PRIMARY KEY, title varchar, artist_id varchar, year int, duration float);
""")

artist_table_create = create_statement + ("""artists(artist_id varchar PRIMARY KEY, name varchar, location varchar, latitude float, longitude float);
""")

time_table_create = create_statement + ("""time(start_time timestamp PRIMARY KEY, hour int, day int, week int, month int, year int, weekday int);
""")

# STAGING TABLES

staging_events_copy = ("""copy staging_events 
                          from {}
                          iam_role {}
                          json {}
                          timeformat as 'epochmillisecs';
""").format(LOG_DATA,ARN,LOG_JSONPATH)

staging_songs_copy = ("""COPY staging_songs from {} iam_role {} json 'auto';
""").format(SONG_DATA,ARN)

# FINAL TABLES

songplay_table_insert = ("""INSERT INTO songplays(start_time, user_id, level, song_id, artist_id, session_id, location, user_agent)
(SELECT e.ts as start_time, e.userId as user_id, e.level as level, s.song_id as song_id, 
s.artist_id as artist_id, e.sessionId as session_id, e.location as location, e.userAgent as user_agent FROM staging_events e LEFT JOIN staging_songs s ON e.song = s.title AND e.artist = s.artist_name AND e.length = s.duration WHERE e.page='NextSong'); 
""")

user_table_insert = ("""INSERT INTO users(user_id, first_name, last_name, gender, level)
(SELECT DISTINCT userId as user_id, firstName as first_name, lastName as last_name, gender as gender, level as level 
FROM staging_events WHERE page='NextSong' AND user_id is NOT NULL);
""")

song_table_insert = ("""INSERT INTO songs(song_id, title, artist_id, year, duration)
(SELECT DISTINCT song_id, title, artist_id, year, duration FROM staging_songs); 
""")

artist_table_insert = ("""INSERT INTO artists(artist_id, name, location, latitude, longitude)
(SELECT DISTINCT artist_id as artist_id, artist_name as name, artist_location as location, 
artist_latitude as latitude, artist_longitude as longitude FROM staging_songs);
""")

time_table_insert = ("""INSERT INTO time(start_time, hour, day, week, month, year, weekday)
(SELECT ts as start_time, DATE_PART(h,ts) as hour, DATE_PART(d,ts) as day, DATE_PART(w,ts) as week, 
DATE_PART(mon,ts) as month, DATE_PART(y,ts) as year, DATE_PART(weekday,ts) as weekday FROM staging_events WHERE page='NextSong'); 
""")

# QUERY LISTS

create_table_queries = [staging_events_table_create, staging_songs_table_create, songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [staging_events_table_drop, staging_songs_table_drop, songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
copy_table_queries = [staging_events_copy, staging_songs_copy]
insert_table_queries = [songplay_table_insert, user_table_insert, song_table_insert, artist_table_insert, time_table_insert]
