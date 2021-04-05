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

DATABASE_LOCATION = "sqlite:///played_tracks.sqlite"
USER_ID = os.getenv('USER_NAME')
TOKEN = os.getenv('OAUTH_TOKEN')

if __name__ == '__main__':
    headers = {
        "Accept": "application/json",
        "Content-Type":"application/json",
        "Authorization": "Bearer {token}".format(token=TOKEN)
    }

    today = datetime.datetime.now()
    yesterday = today - datetime.timedelta(days=1)
    yesterday_unix_timestamp = int(yesterday.timestamp())

    get_request = "https://api.spotify.com/v1/me/player/recently-played?limit=30&after={time}".format(time=yesterday_unix_timestamp)
    r = requests.get(get_request, headers = headers)

    data = r.json()

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

    song_dict = {
        "song_name": song_names,
        "album_name": album_names,
        "artist_name": artist_names,
        "release_year": release_years,
        "played_at": played_at,
        "timestamp": timestamps
    }

    song_df = pd.DataFrame(song_dict, columns = ["song_name", "album_name", "artist_name", "release_year", "played_at", "timestamp"])








