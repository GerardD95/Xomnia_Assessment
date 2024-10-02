import logging
from typing import Protocol

import duckdb
from pyspark.sql import DataFrame
from sqlalchemy import text

from config import DB_HOST, DB_PASS
from utils.utils import read_sql_file, get_google_cloud_sql_connection


class WriteStrategy(Protocol):
    def write(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        ...

    def create_tables(self):
        ...

class ConsoleWriteStrategy(WriteStrategy):
    def write(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        logging.info(f"Writing batch {batch_id} to console for table: {table_name}")
        batch_df.show(truncate=False)  
        logging.info(f"Batch {batch_id} contents logged to console for table: {table_name}")

    def create_tables(self):
        logging.info("No tables to create for console write strategy")

class DuckDBWriteStrategy(WriteStrategy):
    def __init__(self, schema_file: str = 'schemas/duckdb_message_schemas.sql', db_file: str = '../local_duckdb.db'):
        self.schema_file = schema_file
        self.db_file = db_file

    def write(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        logging.info(f"Writing batch {batch_id} to DuckDB for table: {table_name}")
        pd_df = batch_df.toPandas()
        try:
            with duckdb.connect(self.db_file) as conn:
                conn.register('pd_df', pd_df)
                pk_columns = ', '.join(pk)
                conn.execute(f"""
                    INSERT INTO {table_name} SELECT * FROM pd_df
                    ON CONFLICT ({pk_columns}) DO NOTHING 
                """)
            logging.info(f"Contents written to DuckDB for batch {batch_id}")
        except Exception as e:
            logging.error(f"Error writing batch {batch_id} to DuckDB for table {table_name}: {e}")

    def create_tables(self):
        query = read_sql_file(self.schema_file)
        with duckdb.connect(self.db_file) as conn:
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
        logging.info("Tables created successfully in DuckDB")

class GoogleCloudSQLWriteStrategy(WriteStrategy):
    def __init__(self, schema_file: str = 'schemas/gcp_message_schemas.sql'):
        self.schema_file = schema_file
        self.jdbc_url = f"jdbc:postgresql://{DB_HOST}:5432/postgres"
        self.properties = {
            "user": "postgres",
            "password": DB_PASS,
            "driver": "org.postgresql.Driver"
        }
        
    def write(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        logging.info(f"Writing batch {batch_id} to Google Cloud SQL for table: {table_name}")
        try:
            batch_df.write.jdbc(self.jdbc_url, table_name, properties=self.properties, mode="append")
            logging.info(f"Contents written to Google Cloud SQL for batch {batch_id}")
        except Exception as e:
            logging.error(f"Error writing batch {batch_id} to Google Cloud SQL for table {table_name}: {e}")
            raise

    def create_tables(self):
        query = read_sql_file(self.schema_file)
        with get_google_cloud_sql_connection() as conn:
            with conn.begin() as trans:
                try:
                    conn.execute(text(query))
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    logging.error(f"Error creating tables: {e}")
                    raise
        logging.info("Tables created successfully in Google Cloud SQL")

class WriteContext:
    def __init__(self, strategy: WriteStrategy):
        self._strategy = strategy

    def execute_strategy(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        self._strategy.write(batch_df, batch_id, table_name, pk)
