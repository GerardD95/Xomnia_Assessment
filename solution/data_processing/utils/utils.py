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
    

    