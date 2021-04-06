'''
Below is an ETL pipeline that connects to the Spotify API and extracts recent song information (ex. song name, song artist,
release year, time played, and timestamp). The data is then validated using basic data engineering principles and loaded into an
SQLite database using SQLAlchemy.

'''

# importing libraries

import os
from dotenv import load_dotenv
import pandas as pd
import numpy as np
import sqlite3
import sqlalchemy
from sqlalchemy.orm import sessionmaker
import json
import requests
import datetime

load_dotenv()

# loading in environment variables

DATABASE_LOCATION = "sqlite:///played_tracks.sqlite"
USER_ID = os.getenv('USER_NAME')
TOKEN = os.getenv('OAUTH_TOKEN')

# data validation function (transform)

def check_if_valid_data(df: pd.DataFrame) -> bool:
    # Check if empty dataframe
    if df.empty:
        print("No songs downloaded. Finishing execution...")
        return False

    # Check if all primary keys are unique (primary key here being played at value)
    if not pd.Series(df['played_at']).is_unique:
        raise Exception("Primary key check is violated.")

    # Check if any values in the dataframe are null
    if df.isnull().values.any():
        raise Exception("Null values found.")

    # Ensure all data is from the past 24 hours
    today = datetime.datetime.now()
    today = today.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

    yesterday = datetime.datetime.now() - datetime.timedelta(days=1)
    yesterday = yesterday.replace(hour = 0, minute = 0, second = 0, microsecond = 0)

    timestamps = df["timestamp"].tolist()
    for timestamp in timestamps:
        if datetime.datetime.strptime(timestamp, "%Y-%m-%d") != yesterday and datetime.datetime.strptime(timestamp, "%Y-%m-%d") != today:
            raise Exception("At least one of the returned values is not from the last 24 hours.")

    # if all conditions met, return True
    return True

if __name__ == '__main__':
    headers = {
        "Accept": "application/json",
        "Content-Type":"application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    # obtain today and yesterday's dates along with Unix timestamp for Spotify API

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp()) * 1000

    # generate get request and convert to JSON

    get_request = "https://api.spotify.com/v1/me/player/recently-played?limit=30&after={time}".format(time=yesterday_unix_timestamp)
    r = requests.get(get_request, headers = headers)

    data = r.json()

    # keep track of song, album, artist, release year, and played at/timestamp information

    song_names = []
    album_names = []
    artist_names = []
    release_years = []
    played_at = []
    timestamps = []

    for song in data["items"]:
        song_names.append(song["track"]["name"])
        album_names.append(song["track"]["album"]["name"])
        artist_names.append(song["track"]["album"]["artists"][0]["name"])
        release_years.append(song["track"]["album"]["release_date"][:4])
        played_at.append(song["played_at"])
        timestamps.append(song["played_at"][:10])

    # convert above information to dictionary and then Pandas DataFrame

    song_dict = {
        "song_name": song_names,
        "album_name": album_names,
        "artist_name": artist_names,
        "release_year": release_years,
        "played_at": played_at,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "album_name", "artist_name", "release_year", "played_at", "timestamp"])

    # Validate data
    if check_if_valid_data(song_df):
        print("Data valid, proceeding to loading stage.")
        
    # Load data
    
    engine = sqlalchemy.create_engine(DATABASE_LOCATION)
    connection = sqlite3.connect('played_tracks.sqlite')
    cursor = connection.cursor()

    sql_query = """
    CREATE TABLE IF NOT EXISTS my_played_tracks(
        song_name VARCHAR(200),
        album_name VARCHAR(200),
        artist_name VARCHAR(200),
        release_year VARCHAR(200),
        played_at VARCHAR(200),
        timestamp VARCHAR(200),
        CONSTRAINT primary_key_constraint PRIMARY KEY (played_at)        
    )
    """

    cursor.execute(sql_query)
    print("Opened database successfully.")

    try:
        song_df.to_sql("my_played_tracks", engine, index=False, if_exists="append")
    except:
        print("Data already exists in the database (unique constraint failed).")

    connection.close()
    print("Closed database successfully.")