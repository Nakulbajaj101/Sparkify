import os
import glob
import logging
import pandas as pd
import psycopg2
from io import StringIO
from sql_queries import *

logging.basicConfig(level=logging.ERROR)

def copy_from_stringio(cur, df, table, with_header=False, sep="|", columns=None):
    """
    - Creates a buffer and reads the database as a string stream
    - Copies the string stream into database using copy_from cursor method
    """
    # save dataframe to an in memory buffer
    buffer = StringIO()
    df.to_csv(buffer, header=with_header,sep=sep, index=False)
    buffer.seek(0)

    try:
        cur.copy_from(buffer, table, sep=sep, columns=columns)
    except (Exception, psycopg2.DatabaseError) as error:

        logging.error("%s" % error)
        return 1
    logging.info(f"copying data from file to {table} table done")


def process_song_file(cur, filepath):
    """
    - Reads json song files from filepath into dataframe
    - Extracts songs and artists data and inserts into database
    """
    # open song file
    df = pd.read_json(filepath, lines=True)

    # insert song record
    song_data = list(df[['song_id','title','artist_id','year','duration']].values[0])
    cur.execute(song_table_insert, song_data)

    # insert artist record
    artist_data = list(df[['artist_id','artist_name', 'artist_location','artist_latitude','artist_longitude']].values[0])

    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    """
    - Reads json song log files from filepath into dataframe
    - Filters the records which have Next Song association
    - Extracts time in milliseconds and provides transformations for time table
    - Loads row by row data into time table and bulk inserts data into songplay table
    """
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df.page == 'NextSong']

    # convert timestamp column to datetime
    t = pd.to_datetime(df['ts'], unit='ms')
    df['ts'] = t

    # insert time data records
    time_data = (t, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday)
    column_labels = ("start_date","hour","day","week","month","year","weekday")
    time_df = pd.DataFrame(data={cols:values for cols,values in zip(column_labels,time_data)})

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, row)

    # load user table
    user_df = df[["userId","firstName","lastName","gender","level"]]

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # getting artist and song id records
    songid, artistid = [],[]
    for index, row in df.iterrows():

        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()

        if results:
            songid.append(results[0])
            artistid.append(results[1])
        else:
            songid.append(None)
            artistid.append(None)

    df['songid'] = songid
    df['artistid'] = artistid

    # renaming columns to database columns
    songplay_column_mapping = {"ts":"start_time", "userId":"user_id","songid":"song_id",
                           "artistid":"artist_id","sessionId":"session_id",
                           "userAgent":"user_agent"}
    df = df.rename(columns=songplay_column_mapping)

    # subsetting dataframe before bulk inserting
    songplay_columns = ["start_time", "user_id","level", "song_id", "artist_id", "session_id", "location","user_agent"]
    songplay_data = df[songplay_columns]

    # insert songplay records
    copy_from_stringio(cur=cur, table=songplay_table, df=songplay_data,columns=tuple(songplay_columns))

def process_data(cur, conn, filepath, func):
    """
    This is the main function to process all the data from extraction to load
    by iterating over all the json song files and json song play log files in the directory
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    logging.info('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile)
        logging.info('{}/{} files processed.'.format(i, num_files))


def main():
    """
    - Connects to the sparkifydb database
    - Sets auto committing to true
    - Loads the data from json into database tables
    - Closes the connection after data is loaded
    """
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    conn.set_session(autocommit=True)
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()
    
