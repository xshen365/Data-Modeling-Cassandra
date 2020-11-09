# Sparkify

## Purpose

The purpose of this database is to analyze the startup Sparkify and their users/user song play data.

## Schema Design

The database of star schema consists of 5 tables, with the songplay table being the fact table which queries all information needed to do the analytics work, and other 4 tables being dimensional tables which contains necessary information needed to identify objects such as user, song, artist, time.

## Tables

Fact table is songplays table which includes a bunch of columns to do analytics, like song_id, artist_id, duration, start time etc. Dimension tables are songs, artists, time, and users. These are basic object tables which includes necessary attributes to identify characteristic such as how long does a song play, what gender is a customer, what device does a customer use and so on. 

## ETL

The ETL process contains the steps to extract data from a list of json files. 1. Extract song and artist information from 'song_data' json files, insert into 'songs' and 'artist' table. 2. Extract log information from 'log_data' and convert time in milisecond to 'datetime' and insert into 'songplay' table. 

# Run steps

Run by first creating tables using 'python create_tables'; Do sql operations(insert, create, drop) by run 'python etl.py'; All previous steps should be done in command line in correct project folder; See results by running 'python test.ipynb' in jupyter notebook.