from typing import Protocol
import duckdb
import pandas as pd


class QueryExecutor(Protocol):
    def execute_query(self, query: str) -> pd.DataFrame:
        pass


class LocalQueryExecutor(QueryExecutor):
    def execute_query(self, query: str) -> pd.DataFrame:
        return duckdb.sql(query).fetchdf()


class RemoteQueryExecutor(QueryExecutor):
    def __init__(self, base_url: str):
        self.base_url = base_url

    def execute_query(self, query: str) -> pd.DataFrame:
        ...

