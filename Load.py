import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
import logging
import os

def load_data():
    logging.info("Starting loading process")

    try:
        file_path = "/home/dcontreras/Workshop002/temp/merged_dataset.csv"
        
        if not os.path.exists(file_path):
            raise FileNotFoundError("Merged dataset file not found.")

        df = pd.read_csv(file_path)
        logging.info(f"Loaded merged dataset with shape: {df.shape}")

        connection = BaseHook.get_connection("PSQL_Workshop_conn")
        db_url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
        engine = create_engine(db_url)

        df.to_sql("merged_music_data", engine, if_exists="replace", index=False)
        logging.info("Merged dataset successfully loaded into PostgreSQL")

    except Exception as e:
        logging.error(f"Error loading data: {e}")
