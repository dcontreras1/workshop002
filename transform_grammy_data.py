from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
import pandas as pd
import logging
import os

TEMP_PATH = "/home/dcontreras/Workshop002/temp"

def transform_grammy_data():
    logging.info("Transforming Grammy Data")

    try:
        connection = BaseHook.get_connection("PSQL_Workshop_conn")
        db_url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
        engine = create_engine(db_url)

        with engine.connect() as conn:
            logging.info("Connected to the database")

            # Eliminar duplicados
            conn.execute(text("""
                DELETE FROM grammy_awards_table
                WHERE ctid NOT IN (
                    SELECT MIN(ctid)
                    FROM grammy_awards_table
                    GROUP BY artist, nominee, category, year
                )
            """))

            # Reemplazar valores nulos
            conn.execute(text("""
                UPDATE grammy_awards_table
                SET artist = COALESCE(artist, 'Unknown'),
                    nominee = COALESCE(nominee, 'Unknown')
            """))

            # Añadir columna binaria para "winner"
            conn.execute(text("""
                ALTER TABLE grammy_awards_table ADD COLUMN IF NOT EXISTS winner_binary INT;
                UPDATE grammy_awards_table
                SET winner_binary = CASE WHEN winner = 'Yes' THEN 1 ELSE 0 END;
            """))

        # Cargar los datos a un DataFrame para transformar columnas específicas
        df = pd.read_sql("SELECT * FROM grammy_awards_table", engine)

        # Formatear las fechas
        for col in ["published_at", "updated_at"]:
            if col in df.columns:
                df[col] = pd.to_datetime(df[col]).dt.date

        # Eliminar la columna year si existe
        if "year" in df.columns:
            df.drop(columns=["year"], inplace=True)

        output_path = os.path.join(TEMP_PATH, "grammy_transformed.csv")
        df.to_csv(output_path, index=False)
        logging.info(f"Transformed Grammy data saved to {output_path}")

    except Exception as e:
        logging.error(f"Error transforming data: {e}")
