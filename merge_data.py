import pandas as pd
import os
import logging

TEMP_PATH = "/home/dcontreras/Workshop002/temp"

def merge_datasets():
    try:
        logging.info("Loading transformed datasets")

        grammy_path = os.path.join(TEMP_PATH, "grammy_transformed.csv")
        spotify_path = os.path.join(TEMP_PATH, "spotify_transformed.csv")
        api_path = os.path.join(TEMP_PATH, "spotify_api_transformed.csv")

        grammy_df = pd.read_csv(grammy_path)
        spotify_df = pd.read_csv(spotify_path)
        api_df = pd.read_csv(api_path)

        # Normalizar nombres de artistas para mejor coincidencia
        grammy_df["nominee"] = grammy_df["nominee"].str.strip().str.lower()
        spotify_df["artist"] = spotify_df["artist"].str.strip().str.lower()

        logging.info("Merging Spotify data with API data")
        spotify_enriched = pd.merge(
            spotify_df,
            api_df[["track_name", "artist", "musicbrainz_id"]],
            on=["track_name", "artist"],
            how="left"
        )

        logging.info("Merging Grammy data with enriched Spotify data")
        merged_df = pd.merge(
            grammy_df,
            spotify_enriched,
            left_on="nominee",
            right_on="artist",
            how="left"
        )

        if "artist" in merged_df.columns:
            merged_df.drop(columns=["artist"], inplace=True)

        output_path = os.path.join(TEMP_PATH, "merged_dataset.csv")
        merged_df.to_csv(output_path, index=False)

        logging.info(f"Merged dataset saved to {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error during merging datasets: {e}")
        return False
