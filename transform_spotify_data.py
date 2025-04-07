import pandas as pd
import os
import logging

TEMP_PATH = "/home/dcontreras/Workshop002/temp"

def transform_spotify_data():
    try:
        logging.info("Reading Spotify dataset")
        df = pd.read_csv(f"{TEMP_PATH}/spotify_data.csv")

        df.drop_duplicates(inplace=True)
        df.dropna(subset=["track_name", "artists"], inplace=True)

        df.rename(columns={"artists": "artist"}, inplace=True)
        df.columns = df.columns.str.lower().str.replace(' ', '_')

        # Convertir duración de milisegundos a minutos (dos decimales)
        if "duration_ms" in df.columns:
            df["duration_min"] = (df["duration_ms"] / 60000).round(2)

        # Redondear tempo a un decimal
        if "tempo" in df.columns:
            df["tempo"] = df["tempo"].round(1)

        output_path = os.path.join(TEMP_PATH, "spotify_transformed.csv")
        df.to_csv(output_path, index=False)
        logging.info(f"Spotify data transformed and saved to {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error transforming Spotify data: {e}")
        return False
