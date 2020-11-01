# TABLE NAMES

songplay_table = 'songplays'
user_table = 'users'
song_table = 'songs'
artist_table = 'artists'
time_table = 'time'

# DROP TABLES

songplay_table_drop = f"DROP TABLE IF EXISTS {songplay_table} CASCADE"
user_table_drop = f"DROP TABLE IF EXISTS {user_table}"
song_table_drop = f"DROP TABLE IF EXISTS {song_table}"
artist_table_drop = f"DROP TABLE IF EXISTS {artist_table}"
time_table_drop = f"DROP TABLE IF EXISTS {time_table}"

# CREATE TABLES

songplay_table_create = (f"""CREATE TABLE {songplay_table}
                        (songplay_id SERIAL PRIMARY KEY NOT NULL,
                        start_time TIMESTAMP REFERENCES {time_table} (start_time) NOT NULL,
                        user_id INTEGER REFERENCES {user_table} (user_id) NOT NULL,
                        level VARCHAR(4) NOT NULL,
                        song_id VARCHAR,
                        artist_id VARCHAR,
                        session_id INTEGER,
                        location TEXT,
                        user_agent TEXT)
                        """)

user_table_create = (f"""CREATE TABLE {user_table}
                    (user_id INTEGER PRIMARY KEY NOT NULL,
                    first_name VARCHAR,
                    last_name VARCHAR,
                    gender VARCHAR(1),
                    level VARCHAR(4) NOT NULL)
                    """)

song_table_create = (f"""CREATE TABLE {song_table}
                    (song_id VARCHAR PRIMARY KEY NOT NULL,
                    title VARCHAR,
                    artist_id VARCHAR NOT NULL,
                    year INTEGER,
                    duration DECIMAL)
                    """)

artist_table_create = (f"""CREATE TABLE {artist_table}
                      (artist_id VARCHAR PRIMARY KEY NOT NULL,
                      name VARCHAR,
                      artist_location TEXT,
                      latitude DECIMAL,
                      longitude DECIMAL)
                      """)

time_table_create = (f"""CREATE TABLE {time_table}
                    (start_time TIMESTAMP PRIMARY KEY NOT NULL,
                    hour INTEGER NOT NULL,
                    day INTEGER NOT NULL,
                    week INTEGER NOT NULL,
                    month INTEGER NOT NULL,
                    year INTEGER NOT NULL,
                    weekday INTEGER NOT NULL)
                    """)

# INSERT RECORDS

time_table_insert = (f"""INSERT INTO {time_table}
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                    ON CONFLICT (start_time) DO NOTHING""")

user_table_insert = (f"""INSERT INTO {user_table}
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (user_id)
                    DO UPDATE SET level=EXCLUDED.level""")

song_table_insert = (f"""INSERT INTO {song_table}
                    VALUES (%s, %s, %s, %s, %s)
                    ON CONFLICT (song_id) DO NOTHING""")

artist_table_insert = (f"""INSERT INTO {artist_table}
                      VALUES (%s, %s, %s, %s, %s)
                      ON CONFLICT (artist_id) DO NOTHING""")

# FIND SONGS

song_select = (f"""SELECT s.song_id, a.artist_id
              from {song_table} s
              JOIN {artist_table} a
              ON s.artist_id=a.artist_id
              WHERE s.title=%s AND a.name=%s AND s.duration=%s""")

# QUERY LISTS

create_table_queries = [user_table_create, artist_table_create,
                        song_table_create, time_table_create,
                        songplay_table_create]

drop_table_queries = [songplay_table_drop, user_table_drop,
                      song_table_drop, artist_table_drop,
                      time_table_drop]
