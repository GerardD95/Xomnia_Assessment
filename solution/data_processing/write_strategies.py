from typing import Protocol
import logging
import duckdb
from pyspark.sql import DataFrame
from utils import get_google_cloud_sql_connection

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
        pd_df = batch_df.toPandas()
        conn = get_google_cloud_sql_connection()
        pd_df.to_sql(table_name, conn, if_exists='append', index=False)
        logging.info(f"Contents written to Google Cloud SQL for batch {batch_id}")

class WriteContext:
    def __init__(self, strategy: WriteStrategy):
        self._strategy = strategy

    def execute_strategy(self, batch_df: DataFrame, batch_id: int, table_name: str, pk: list):
        self._strategy.write(batch_df, batch_id, table_name, pk)
