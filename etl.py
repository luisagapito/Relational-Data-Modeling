"""
The script etl.py connects to the sparkifydb database, extract and process log and song JSON data to write it in a Postgess star schema, so it can be consumed by the analytics team.
"""

# Import libraries
import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *
from datetime import datetime

def process_song_file(cur, filepath, conn):
    """
    The process_song_file's method processes the song data and writes it in the song and artists tables.

    ...

    Attributes
    ----------
    cur : cursor of the connection with the sparkifydb database
    filepath : JSON paths for song and log files
    conn : connection with the sparkifydb database


    Returns
    -------
    None
    """
    # open song file
    df = pd.read_json(filepath, lines=True)
    
    # Process song data
    song_data = df[['song_id', 'title', 'artist_id', 'year', 'duration']]
    song_data = song_data.values
    song_data = song_data[0]
    song_data = song_data.tolist()
    
    # insert song record
    cur.execute(song_table_insert, song_data) 
    conn.commit()
    
    # Process artist data
    artist_data = df[['artist_id', 'artist_name', 'artist_location', 'artist_latitude', 'artist_longitude']]
    artist_data = artist_data.values
    artist_data = artist_data[0]
    artist_data = artist_data.tolist()
    
    # insert artist record
    cur.execute(artist_table_insert, artist_data)
    conn.commit()


def process_log_file(cur, filepath, conn):
    """
    The process_log_file's method processes the log data and writes it in the time and users tables. Also, this method writes data         into the songplays table from the song, artist tables, and log data.

    ...

    Attributes
    ----------
    cur : cursor of the connection with the sparkifydb database
    conn : connection with the sparkifydb database
    filepath : JSON paths for song and log files

    Returns
    -------
    None
    """
    # open log file
    df = pd.read_json(filepath, lines=True)
    
    # filter by NextSong action
    df = df[df['page'] == 'NextSong']
    
    # convert timestamp column to datetime
    t=(pd.to_datetime(df['ts'],unit='ms'))
    
    # Process time data records
    time_data = [t , t.dt.hour, t.dt.day, t.dt.weekofyear , t.dt.month, t.dt.year , t.dt.weekday ]
    column_labels = ['start_time','hour','day','week','month','year','weekday']
    dict1= dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(dict1)
    
    # insert time data records
    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))
        conn.commit()
    
    # load user table
    user_df = df[['userId' , 'firstName' , 'lastName' , 'gender' , 'level']]
    
    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)
        conn.commit()
        
    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None
            
        t=(pd.to_datetime(row['ts'],unit='ms'))
        
        # insert songplay record
        songplay_data = [t , row['userId'] , row['level'] , songid, artistid, row['sessionId']  , row['location'], row['userAgent'] ]
        cur.execute(songplay_table_insert, songplay_data)
        conn.commit()

def process_data(cur, conn, filepath, func):
    """
    A method to find the JSON paths from log ad song data. Then, it calls the function (func) process_song_file or process_log_file to     process that data.

    ...

    Attributes
    ----------
    cur : cursor of the connection with the sparkifydb database
    conn : connection with the sparkifydb database
    filepath : path for song and log files
    func : name of the method

    Returns
    -------
    None
    """
    # get all files matching extension from directory
    all_files = []
    for root, dirs, files in os.walk(filepath):
        files = glob.glob(os.path.join(root,'*.json'))
        for f in files :
            all_files.append(os.path.abspath(f))

    # get total number of files found
    num_files = len(all_files)
    print('{} files found in {}'.format(num_files, filepath))

    # iterate over files and process
    for i, datafile in enumerate(all_files, 1):
        func(cur, datafile, conn)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    """
    The main method connects to the sparkifydb. It calls the process_data to process data from song and log data. Finally, it closes       the connection.

    ...

    Attributes
    ----------
    None

    Returns
    -------
    None
    """
    # Connect to database
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()
    
    # Process data
    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    # Close connection
    conn.close()


if __name__ == "__main__":
    """
    It calls the main method.

    ...

    Attributes
    ----------
    None

    Returns
    -------
    None
    """
    main()