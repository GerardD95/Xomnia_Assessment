import os
import logging
import duckdb
from sqlalchemy import create_engine, text
from config import DB_HOST, DB_PASS

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def log_new_file(df, epoch_id):
    logging.info(f"New file detected at epoch {epoch_id}")

def get_google_cloud_sql_connection():
    logging.info(f"Connecting to Google Cloud SQL...")
    engine = create_engine(f'postgresql+psycopg2://postgres:{DB_PASS}@{DB_HOST}:5432/postgres')
    return engine.connect()

def read_sql_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        return file.read()
    
def create_tables_if_not_exist(strategy_key: str):

    if strategy_key == "console":
        return
    elif strategy_key == "localdb":
        schema_file = 'schemas/duckdb_message_schemas.sql'
        query = read_sql_file(schema_file)
        with duckdb.connect('../local_duckdb.db') as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
    elif strategy_key == "cloud":
        schema_file = 'schemas/gcp_message_schemas.sql'
        query = read_sql_file(schema_file)
        with get_google_cloud_sql_connection() as conn:
            with conn.begin() as trans:
                try:
                    conn.execute(text(query))
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    logging.error(f"Error creating tables: {e}")
                    raise
    else:
        logging.error("Invalid strategy key. Use 'local' or 'cloud'.")
        raise ValueError("Invalid strategy key")

    logging.info("Tables created successfully")

if __name__ == "__main__":
    create_tables_if_not_exist("cloud")

    