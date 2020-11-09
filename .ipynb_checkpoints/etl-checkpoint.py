import os
import glob
import psycopg2
import pandas as pd
from sql_queries import *


def process_song_file(cur, filepath):
    '''
        - cur, filepath - cursor of sql connection, a single file contained in the song_data file list
        - Extract song_id, title, artist_id, year and duration from song_data files and pass them into songs table
        - Extract artist_id, artist_name, artist_location, artist_latitude and artist_longitude from song_data and pass it into artists table
    '''
    # open song file
    df = pd.read_json(filepath, typ='series')

    # insert song record
    song_data = [df["song_id"], df["title"], df["artist_id"], df["year"], df["duration"]]
    cur.execute(song_table_insert, song_data)
    
    # insert artist record
    artist_data = [df["artist_id"], df["artist_name"], df["artist_location"], df["artist_latitude"], df["artist_longitude"]]
    cur.execute(artist_table_insert, artist_data)


def process_log_file(cur, filepath):
    '''
        - cur, filepath - cursor of sql connection, a single file contained in the log_data file list
        - convert df with a time-specific timestamp column
        - Extract specific entries from 3 tables, using a combination of both ids and song duration to get songid and artistid and insert
    '''
    # open log file
    df = pd.read_json(filepath, lines=True)

    # filter by NextSong action
    df = df[df["page"]=="NextSong"]

    # convert timestamp column to datetime
    t = pd.to_datetime(df["ts"], unit='ms')
    df['ts'] = pd.to_datetime(df['ts'], unit='ms')
    #print(type(t), type(df))
    
    # insert time data records
    time_data = [t.dt.date, t.dt.hour, t.dt.day, t.dt.week, t.dt.month, t.dt.year, t.dt.weekday]
    column_labels = ['timestamp', 'hour', 'day', 'week', 'month', 'year', 'weekday']
    time_dictionary = dict(zip(column_labels, time_data))
    time_df = pd.DataFrame.from_dict(time_dictionary)

    for i, row in time_df.iterrows():
        cur.execute(time_table_insert, list(row))

    # load user table
    user_data = [df['userId'], df['firstName'], df['lastName'], df['gender'], df['level']]
    user_columns = ['userId', 'firstName', 'lastName', 'gender', 'level']
    user_dictionary = dict(zip(user_columns, user_data))
    user_df = pd.DataFrame.from_dict(user_dictionary)

    # insert user records
    for i, row in user_df.iterrows():
        cur.execute(user_table_insert, row)

    # insert songplay records
    for index, row in df.iterrows():
        
        # get songid and artistid from song and artist tables
        cur.execute(song_select, (row.song, row.artist, row.length))
        results = cur.fetchone()
        
        if results:
            songid, artistid = results
        else:
            songid, artistid = None, None

        # insert songplay record
        songplay_data = [index, row.ts, row.userId, row.level, songid, artistid, row.sessionId, row.location, row.userAgent]
        cur.execute(songplay_table_insert, songplay_data)


def process_data(cur, conn, filepath, func):
    '''
        - cur, conn, filepath, func - cursor, sql connection, data directory, proper function used given which type of data to be processed
        - This is a helper function used to determine which data processing function is going to use
    '''
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
        func(cur, datafile)
        conn.commit()
        print('{}/{} files processed.'.format(i, num_files))


def main():
    '''
        - sql connection operations
    '''
    conn = psycopg2.connect("host=127.0.0.1 dbname=sparkifydb user=student password=student")
    cur = conn.cursor()

    process_data(cur, conn, filepath='data/song_data', func=process_song_file)
    process_data(cur, conn, filepath='data/log_data', func=process_log_file)

    conn.close()


if __name__ == "__main__":
    main()