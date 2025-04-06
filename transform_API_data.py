import pandas as pd
import os
import logging

TEMP_PATH = "/home/dcontreras/Workshop002/temp"

def transform_api_data():
    try:
        logging.info("Reading API-enriched Spotify data")
        df = pd.read_csv(f"{TEMP_PATH}/spotify_API.csv")

        df.drop_duplicates(inplace=True)

        df.dropna(subset=["artist", "musicbrainz_id"], inplace=True)

        df.columns = df.columns.str.lower().str.replace(' ', '_')

        output_path = os.path.join(TEMP_PATH, "api_transformed.csv")
        df.to_csv(output_path, index=False)
        logging.info(f"API data transformed and saved to {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error transforming API data: {e}")
        return False
