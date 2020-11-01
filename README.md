# Sparkify Music Analytical Engine

The purpose of the project is to build a database and design an
ETL pipeline that would power the analytical needs for Sparkify.

## Motivation

A startup called Sparkify wants to analyze the data they've been
collecting on songs and user activity on their new music streaming app.
The analytics team is particularly interested in understanding what
songs users are listening to. Currently, they don't have an easy way
to query their data, which resides in a directory of JSON logs on user
activity on the app, as well as a directory with JSON metadata on the
songs in their app. In order to provide better service to their users,
Sparkify would like to bring their data to a data store/database so
analysts can perform queries faster, and analyse user patterns in
an optimized manner.


## Built With

The section covers tools used to enable the project.

1. PostgresDB to store data
2. Python to process and carry out ETL
3. Jupyter Notebook to test ETL and connections

## Schema for Song Play Analysis

Using the song and log datasets, created a star schema optimized for
queries on song play analysis. This includes the following tables listed
below. Using this schema majority of data required for analysis can be
in few tables, and only few joins may have to be used. This is a common
design utilised in industry which maintains a balance of optimum
normalisation with enhanced performance. Since the goal is having a
database which analysts can use efficiently, focus was on optimisation
rather than storage reduction and redundancy.

### Fact Table

1. songplays - records in log data associated with song plays i.e. records with page NextSong
- songplay_id, start_time, user_id, level, song_id, artist_id, session_id, location, user_agent

### Dimension Tables

2. users - users in the app
- user_id, first_name, last_name, gender, level

3. songs - songs in music database
- song_id, title, artist_id, year, duration

4. artists - artists in music database
-  artist_id, name, location, latitude, longitude

5. time - timestamps of records in songplays broken down into specific units
- start_time, hour, day, week, month, year, weekday


## Files and Folders

1. create_tables.py - Contains logic to clean up and recreate database with tables.
2. sql_queries.py - Contains table definitions, DDL/DML SQL scripts to create tables, drop table, select data from table and insert data into tables.
3. etl.py - Contains all the logic to extract data from files and load into database.
4. etl.ipynb - Jupyter notebook to test the ETL pipeline over single data files
5. test.ipynb - Jupyter notebook to test database is populated after successfully running etl.ipynb file or etl.py file.

## Running the ETL pipeline

1. Open terminal
2. Run create_tables.py script to clean the database and recreate database and table definitions.

`python3 create_tables.py`

3. Run etl.py to extract data from files and load into database

`python3 etl.py`

4. Open test.ipynb and run all cells. This will help to test the results of the pipeline.

## Testing and modifying the pipeline

1. Open terminal and run create_tables.py script.

`python3 create_tables.py`

2. Open etl.ipynb and run cell by cell to test individual step of the pipeline.
3. Edit create_tables.py and sql_queries.py to update design or pipeline, and retest with etl.ipynb file and validate results in test.ipynb file. Make sure to clean database by running step 1, before testing. Once satisfied edit the etl.py file by adding changes from etl.ipynb file.

### Example of fetching data from database

Connect to the database using connection id:

> postgresql://student:student@127.0.0.1/sparkifydb
> Selecting 5 rows from songs table

Run the query below
`SELECT * FROM songplays LIMIT 1;`

| user_id | first_name | last_name | gender | level |
|---------|------------|-----------|--------|-------|
| 92      | Ryann      | Smith     | F      | free  |

# Contact for questions or support

Nakul Bajaj
