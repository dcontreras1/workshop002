import pandas as pd
import os
import logging

TEMP_PATH = "/home/dcontreras/Workshop002/temp"

def transform_api_data():
    try:
        logging.info("Reading API Spotify data")
        df = pd.read_csv(f"{TEMP_PATH}/spotify_API.csv")

        if "artists" in df.columns:
            df.rename(columns={"artists": "artist"}, inplace=True)

        df.columns = df.columns.str.lower().str.replace(' ', '_')

        df.dropna(subset=["artist", "musicbrainz_id"], inplace=True)

        # Eliminar columna duration_ms si existe
        if "duration_ms" in df.columns:
            df.drop(columns=["duration_ms"], inplace=True)
            logging.info("Removed 'duration_ms' column from API dataset")

        output_path = os.path.join(TEMP_PATH, "spotify_api_transformed.csv")
        df.to_csv(output_path, index=False)
        logging.info(f"API-enriched data transformed and saved to {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error transforming API data: {e}")
        return False
