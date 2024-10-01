import logging
from typing import Protocol

import duckdb
from pyspark.sql import DataFrame
from config import DB_HOST, DB_PASS


class WriteStrategy(Protocol):
    def write(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        ...

class ConsoleWriteStrategy(WriteStrategy):
    def write(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        logging.info(f"Writing batch {batch_id} to console for table: {table_name}")
        batch_df.show(truncate=False)  
        logging.info(f"Batch {batch_id} contents logged to console for table: {table_name}")

class DuckDBWriteStrategy(WriteStrategy):
    def write(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        logging.info(f"Writing batch {batch_id} to DuckDB for table: {table_name}")
        pd_df = batch_df.toPandas()
        try:
            with duckdb.connect('../local_duckdb.db') as conn:
                conn.register('pd_df', pd_df)
                pk_columns = ', '.join(pk)
                conn.execute(f"""
                    INSERT INTO {table_name} SELECT * FROM pd_df
                    ON CONFLICT ({pk_columns}) DO NOTHING 
                """)
            logging.info(f"Contents written to DuckDB for batch {batch_id}")
        except Exception as e:
            logging.error(f"Error writing batch {batch_id} to DuckDB for table {table_name}: {e}")

class GoogleCloudSQLWriteStrategy(WriteStrategy):
    def write(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        logging.info(f"Writing batch {batch_id} to Google Cloud SQL for table: {table_name}")
        try:
            jdbc_url = f"jdbc:postgresql://{DB_HOST}:5432/postgres"
            properties = {
                "user": "postgres",
                "password": DB_PASS,
                "driver": "org.postgresql.Driver"
            }
            batch_df.write.jdbc(jdbc_url, table_name, properties=properties, mode="append")
            logging.info(f"Contents written to Google Cloud SQL for batch {batch_id}")
        except Exception as e:
            logging.error(f"Error writing batch {batch_id} to Google Cloud SQL for table {table_name}: {e}")
            raise

class WriteContext:
    def __init__(self, strategy: WriteStrategy):
        self._strategy = strategy

    def execute_strategy(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        self._strategy.write(batch_df, batch_id, table_name, pk)
