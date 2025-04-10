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
    output_path = f"{TEMP_PATH}/spotify_API.csv"

    # Verificar si ya existe el archivo final
    if os.path.exists(output_path):
        logging.info("spotify_API.csv already exists. Skipping API extraction.")
        return True

    def search_artist_API(artist, retries=3, cooldown=5):
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
                    logging.warning(f"No artist found for {artist}")
                    return None
            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed for {artist}: {e}")
                if attempt < retries - 1:
                    time.sleep(cooldown)
                else:
                    logging.error(f"Max retries reached for {artist}")
                    return None
        return None

    try:
        df = pd.read_csv(csv_path)

        if "artists" not in df.columns:
            raise ValueError("Missing 'artists' column in Spotify dataset")

        df_sample = df.sample(n=18000, random_state=42)
        unique_artists = pd.Series(df_sample['artists'].dropna().unique())

        # Cargar artistas ya procesados
        if os.path.exists(output_path):
            existing_df = pd.read_csv(output_path)
            processed_artists = set(existing_df["artists"].dropna().unique())
            logging.info(f"{len(processed_artists)} artists already processed")
        else:
            existing_df = pd.DataFrame(columns=["artists", "musicbrainz_id"])
            processed_artists = set()

        # Filtrar solo nuevos artistas
        new_artists = [a for a in unique_artists if a not in processed_artists]
        logging.info(f"Processing {len(new_artists)} new artists")

        if not new_artists:
            logging.info("No new artists to process.")
            return True

        # Llamar a la API para los nuevos artistas
        new_df = pd.DataFrame()
        new_df["artists"] = new_artists
        new_df["musicbrainz_id"] = new_df["artists"].apply(lambda artist: search_artist_API(artist))

        # Combinar y guardar
        final_df = pd.concat([existing_df, new_df], ignore_index=True)
        final_df.to_csv(output_path, index=False)

        logging.info("Updated spotify_API.csv with new artists")
        return True

    except Exception as e:
        logging.error(f"Error extracting with MusicBrainz API: {e}")
        return False
