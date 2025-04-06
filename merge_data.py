import pandas as pd
import os
import logging

TEMP_PATH = "/home/dcontreras/Workshop002/temp"

def merge_datasets():
    try:
        logging.info("Loading transformed datasets")

        grammy_df = pd.read_csv(os.path.join(TEMP_PATH, "grammy_transformed.csv"))
        spotify_df = pd.read_csv(os.path.join(TEMP_PATH, "spotify_transformed.csv"))
        api_df = pd.read_csv(os.path.join(TEMP_PATH, "spotify_api_transformed.csv"))

        logging.info("Merging Spotify data with API data")
        spotify_enriched = pd.merge(spotify_df, api_df[["name", "artist", "musicbrainz_id"]],
                                    on=["name", "artist"], how="left")

        logging.info("Merging enriched Spotify data with Grammy data")
        merged_df = pd.merge(spotify_enriched, grammy_df, on="artist", how="inner")

        output_path = os.path.join(TEMP_PATH, "merged_dataset.csv")
        merged_df.to_csv(output_path, index=False)
        logging.info(f"Merged dataset saved to {output_path}")
        return True

    except Exception as e:
        logging.error(f"Error during merging datasets: {e}")
        return False
    