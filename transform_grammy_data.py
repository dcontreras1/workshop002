from sqlalchemy import create_engine, text
from airflow.hooks.base import BaseHook
import logging

def transform_grammy_data():
    logging.info("Transforming Grammy Data")

    try:
        connection = BaseHook.get_connection("PSQL_Workshop_conn")
        db_url = f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}"
        engine = create_engine(db_url)

        with engine.connect() as conn:
            logging.info("Connected to the database")

            conn.execute(text("""
                DELETE FROM grammy_awards
                WHERE id IN (
                    SELECT id FROM (
                        SELECT id, ROW_NUMBER() OVER (PARTITION BY artist, song, category, year ORDER BY id) AS row_num
                        FROM grammy_awards
                    ) t WHERE row_num > 1
                )
            """))

            conn.execute(text("""
            UPDATE grammy_awards
            SET artist = COALESCE(artist, 'Unknown'),
                song = COALESCE(song, 'Unknown')
            """))

            conn.execute(text("""
                ALTER TABLE grammy_awards ADD COLUMN IF NOT EXISTS winner_binary INT;
                UPDATE grammy_awards
                SET winner_binary = CASE WHEN winner = 'Yes' THEN 1 ELSE 0 END;
            """))

            logging.info("Data from grammy_awards transformed successfully")

    except Exception as e:
        logging.error(f"Error transforming data: {e}")