# Spotify-ETL-Pipeline

The above is an ETL pipeline that connects to the Spotify API and extracts recent song information. The data is then validated using basic data engineering principles and loaded into an SQLite database using SQLAlchemy.

The script can be run locally by cloning this repository and creating an `.env` file to hold username and OAuth information. Spotify allows developers to access this via their API [here](https://developer.spotify.com/console/get-recently-played/?limit=10&after=1484811043508&before=).