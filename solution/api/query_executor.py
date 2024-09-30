from typing import Protocol
import logging
import duckdb
import pandas as pd

logging.basicConfig(level=logging.INFO, format='%(asctime)s %(levelname)s: %(message)s')

class QueryExecutor(Protocol):
    def execute_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        pass


class LocalQueryExecutor(QueryExecutor):
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
                    # Create tables from CSV files
                    conn.execute(f"CREATE TABLE raw_messages AS SELECT * FROM read_csv_auto('{self.source_path}/raw_messages_clean.csv')")
                    conn.execute(f"CREATE TABLE weather AS SELECT * FROM read_csv_auto('{self.source_path}/df_weather_final.csv')")
                    
                    # Execute query
                    return conn.execute(query, params).fetch_df()
        except duckdb.CatalogException as e:
            if 'Table' in str(e) and 'does not exist' in str(e):
                logging.error(f"Error: Table does not exist. {e}")
                return pd.DataFrame()
            else:
                raise e


class RemoteQueryExecutor(QueryExecutor):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def execute_query(self, query: str, params: tuple = ()) -> pd.DataFrame:
        ...

