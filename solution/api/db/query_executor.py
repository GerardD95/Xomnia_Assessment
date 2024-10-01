from typing import Protocol
import logging
import duckdb
import pandas as pd

from utils.utils import get_google_cloud_sql_connection

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')


class QueryExecutor(Protocol):
    def execute_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        pass


class DuckDBQueryExecutor(QueryExecutor):
    def __init__(self, source_type: str, source_path: str):
        self.source_type = source_type
        self.source_path = source_path

    def execute_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        try:
            if self.source_type == 'database':
                with duckdb.connect(self.source_path) as conn:
                    return conn.execute(query, params).fetch_df()
            elif self.source_type == 'csv':
                with duckdb.connect() as conn:
                    conn.execute(f"CREATE TABLE raw_messages AS SELECT * FROM read_csv_auto('{self.source_path}/raw_messages_clean.csv')")
                    conn.execute(f"CREATE TABLE weather AS SELECT * FROM read_csv_auto('{self.source_path}/df_weather_final.csv')")
                    return conn.execute(query, params).fetch_df()
        except duckdb.CatalogException as e:
            if 'Table' in str(e) and 'does not exist' in str(e):
                logging.error(f"Error: Table does not exist. {e}")
                return pd.DataFrame()
            else:
                raise e


class PostgresQueryExecutor(QueryExecutor):
    def __init__(self, db_url: str):
        self.db_url = db_url

    def execute_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        try:
            with get_google_cloud_sql_connection(self.db_url) as conn:
                return pd.read_sql(query, conn, params=params)
        except Exception as e:
            logging.error(f"Error: {e}")
            return pd.DataFrame()


class QueryExecutorFactory:
    def __init__(self, local_execution: bool, db_url: str, source_type: str, source_path: str):
        self.local_execution = local_execution
        self.db_url = db_url
        self.source_type = source_type
        self.source_path = source_path

    def get_executor(self) -> QueryExecutor:
        if self.local_execution:
            logging.info("QueryExecutorFactory: Using DuckDBQueryExecutor")
            return DuckDBQueryExecutor(self.source_type, self.source_path)
        else:
            logging.info("QueryExecutorFactory: Using PostgresQueryExecutor")
            return PostgresQueryExecutor(self.db_url)