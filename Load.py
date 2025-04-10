import pandas as pd
from sqlalchemy import create_engine
from airflow.hooks.base import BaseHook
import logging
import os

def load_data():
    logging.info("Starting loading process")

    try:
        file_path = "/home/dcontreras/Workshop002/temp/merged_dataset.csv"
        table_name = "merged_music_data"

        if not os.path.exists(file_path):
            raise FileNotFoundError(f"Merged dataset file not found at: {file_path}")

        df = pd.read_csv(file_path)
        logging.info(f"Loaded merged dataset with shape: {df.shape}")

        if df.empty:
            raise ValueError("Merged dataset is empty. Aborting load.")

        connection = BaseHook.get_connection("PSQL_Workshop_conn")
        db_url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
        engine = create_engine(db_url)

        df.to_sql(table_name, engine, if_exists="replace", index=False)
        logging.info(f"Merged dataset successfully loaded into PostgreSQL table: {table_name}")

    except Exception as e:
        logging.error(f"Error loading data: {e}")