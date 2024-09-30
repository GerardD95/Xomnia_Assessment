import os
import logging
from sqlalchemy import create_engine
import duckdb

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

def log_new_file(df, epoch_id):
    logging.info(f"New file detected at epoch {epoch_id}")

def get_google_cloud_sql_connection():
    # Replace these variables with your actual connection details
    user = "your_username"
    password = "your_password"
    database = "your_database"
    instance_connection_name = "your_project_id:your_region:your_instance_id"

    # Create the SQLAlchemy engine
    engine = create_engine(
        f"mysql+pymysql://{user}:{password}@/{database}?unix_socket=/cloudsql/{instance_connection_name}"
    )
    return engine.connect()

def read_sql_file(file_path: str) -> str:
    with open(file_path, 'r') as file:
        return file.read()

def create_tables_if_not_exist(strategy_key: str):
    if strategy_key == "console":
        return
    elif strategy_key == "localdb":
        with duckdb.connect('../local_duckdb.db') as conn:
                sql_statement = read_sql_file(os.path.join('schemas', 'schemas.sql'))
                conn.execute(sql_statement)
    elif strategy_key == "cloud":
        ...
        #     conn = get_google_cloud_sql_connection()
        #     with conn.cursor() as cursor:
        #         for sql_file in sql_files:
        #             sql_statement = read_sql_file(os.join('schemas', sql_file))
        #             cursor.execute(sql_statement)
        #     conn.commit()
    else:
        logging.error("Invalid strategy key. Use 'local' or 'cloud'.")
        raise ValueError("Invalid strategy key")

    logging.info("Tables created successfully")