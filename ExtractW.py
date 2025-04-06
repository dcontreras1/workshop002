import os
import pandas as pd
import requests
import logging
import time
from urllib.parse import quote
from airflow.hooks.base import BaseHook
from sqlalchemy import create_engine

TEMP_PATH = "/home/dcontreras/Workshop002/temp"

def extract_grammy_data():
    logging.info("Extracting Grammy Data")
    try:
        connection = BaseHook.get_connection("PSQL_Workshop_conn")
        db_url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
        engine = create_engine(db_url)

        query = "SELECT * FROM grammy_awards_table"
        df = pd.read_sql_query(query, con=engine)

        os.makedirs(TEMP_PATH, exist_ok=True)
        df.to_csv(f"{TEMP_PATH}/grammy_data.csv", index=False)

        logging.info("Grammy data extracted and saved")
        return True

    except Exception as e:
        logging.error(f"Error extracting Grammy data: {e}")
        return False

def extract_spotify_data(csv_path="/home/dcontreras/Workshop002/dataset.csv"):
    try:
        df = pd.read_csv(csv_path)

        os.makedirs(TEMP_PATH, exist_ok=True)
        df.to_csv(f"{TEMP_PATH}/spotify_data.csv", index=False)

        logging.info("Spotify data extracted and saved")
        return True

    except Exception as e:
        logging.error(f"Error extracting Spotify data: {e}")
        return False

def extract_musicbrainz_data(csv_path=f"{TEMP_PATH}/spotify_data.csv"):
    def search_artist_API(artist, retries=3, cooldown=2):
        artist_encoded = quote(artist)
        url = f"https://musicbrainz.org/ws/2/artist?query={artist_encoded}&fmt=json&limit=1"
        headers = {
            'User-Agent': 'WorkshopAPIClient/1.0 (daniel.contreras_d@uao.edu.co)'
        }

        for attempt in range(retries):
            try:
                response = requests.get(url, timeout=10, headers=headers)
                response.raise_for_status()
                data = response.json()

                if "artists" in data and len(data["artists"]) > 0:
                    return data["artists"][0]["id"]
                else:
                    return None
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for {artist}: {e}")
                if attempt < retries - 1:
                    time.sleep(cooldown)
                else:
                    return None

    try:
        df = pd.read_csv(csv_path)

        if "artists" not in df.columns:
            raise ValueError("Missing 'artists' column in Spotify dataset")

        df_sample = df.sample(n=2000, random_state=42)

        logging.info("Starting API Extraction on sample")
        df_sample["musicbrainz_id"] = df_sample["artists"].apply(search_artist_API)

        os.makedirs(TEMP_PATH, exist_ok=True)
        df_sample.to_csv(f"{TEMP_PATH}/spotify_API.csv", index=False)

        logging.info("API extraction complete and saved to spotify_API.csv")
        return True

    except Exception as e:
        logging.error(f"Error extracting with MusicBrainz API: {e}")
        return False
