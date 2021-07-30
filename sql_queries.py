# DROP TABLES

songplay_table_drop = "DROP TABLE IF EXISTS songplays"
user_table_drop = "DROP TABLE IF EXISTS  users"
song_table_drop = "DROP TABLE IF EXISTS songs"
artist_table_drop = "DROP TABLE IF EXISTS artists"
time_table_drop = "DROP TABLE IF EXISTS time"

# CREATE TABLES

songplay_table_create = ("""
CREATE TABLE IF NOT EXISTS songplays (songplay_id serial PRIMARY KEY, \
start_time varchar, user_id int , level varchar, \
song_id varchar NOT NULL, artist_id varchar NOT NULL, session_id int ,\
location varchar, user_agent varchar);
""")

user_table_create = ("""
CREATE TABLE IF NOT EXISTS users ( user_id int PRIMARY KEY, \
first_name varchar, last_name varchar, gender varchar, \
level varchar);
""")

song_table_create = ("""
CREATE TABLE IF NOT EXISTS songs ( song_id varchar PRIMARY KEY, \
title varchar, artist_id varchar, year int, duration numeric);
""")

artist_table_create = ("""
CREATE TABLE IF NOT EXISTS artists ( artist_id varchar PRIMARY KEY, \
name varchar, location varchar, latitude varchar, longitude varchar);
""")

time_table_create = ("""
CREATE TABLE IF NOT EXISTS time ( start_time varchar PRIMARY KEY, \
hour varchar, day varchar, week varchar, month varchar, \
year varchar, weekday varchar);
""")

# INSERT RECORDS

song_table_insert = ("""
INSERT INTO songs (song_id, title, artist_id, year, duration) \
VALUES (%s, %s, %s, %s, %s) on conflict (song_id) do update \
SET title  = EXCLUDED.title, artist_id  = EXCLUDED.artist_id, \
duration  = EXCLUDED.duration 
""")

artist_table_insert = ("""
INSERT INTO artists (artist_id, name, location, latitude, longitude) \
VALUES (%s, %s, %s, %s, %s) on conflict (artist_id) do update \
SET name  = EXCLUDED.name, location  = EXCLUDED.location, \
latitude  = EXCLUDED.latitude, longitude  = EXCLUDED.longitude
""")

time_table_insert = ("""
INSERT INTO time (start_time,hour,day,week,month,year,weekday) \
VALUES (%s,%s,%s,%s,%s,%s,%s) on conflict (start_time) do nothing
""")

user_table_insert = ("""
INSERT INTO users (user_id,first_name,last_name,gender,level) \
VALUES (%s, %s, %s, %s, %s) on conflict (user_id) do update \
SET first_name=EXCLUDED.first_name, last_name=EXCLUDED.last_name, \
gender = EXCLUDED.gender, level = EXCLUDED.level 
""")
    
songplay_table_insert = ("""
INSERT INTO songplays (start_time,user_id,level,song_id,artist_id, \
session_id,location,user_agent) VALUES (%s, %s,%s,%s,%s,%s,%s,%s) 
""")

# FIND SONGS

song_select = ("""
Select a.song_id songplay_id, b.artist_id from \
songs a join artists b on a.artist_id=b.artist_id
""")


# QUERY LISTS

create_table_queries = [songplay_table_create, user_table_create, song_table_create, artist_table_create, time_table_create]
drop_table_queries = [songplay_table_drop, user_table_drop, song_table_drop, artist_table_drop, time_table_drop]
                     


