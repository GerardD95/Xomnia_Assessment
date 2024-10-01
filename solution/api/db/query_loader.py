from db.duckdb_queries import (
    get_distinct_ship_count as duckdb_get_distinct_ship_count,
    get_avg_speed_by_hour as duckdb_get_avg_speed_by_hour,
    get_max_min_wind_speed as duckdb_get_max_min_wind_speed,
    get_join_ship_weather_query as duckdb_get_join_ship_weather_query
)
from db.postgres_queries import (
    get_distinct_ship_count as postgres_get_distinct_ship_count,
    get_avg_speed_by_hour as postgres_get_avg_speed_by_hour,
    get_max_min_wind_speed as postgres_get_max_min_wind_speed,
    get_join_ship_weather_query as postgres_get_join_ship_weather_query
)

class QueryFactory:
    def __init__(self, local_execution: bool):
        self.local_execution = local_execution
        self.queries = self._load_queries()

    def _load_queries(self):
        if self.local_execution:
            return {
                'get_distinct_ship_count': duckdb_get_distinct_ship_count,
                'get_avg_speed_by_hour': duckdb_get_avg_speed_by_hour,
                'get_max_min_wind_speed': duckdb_get_max_min_wind_speed,
                'get_join_ship_weather_query': duckdb_get_join_ship_weather_query
            }
        else:
            return {
                'get_distinct_ship_count': postgres_get_distinct_ship_count,
                'get_avg_speed_by_hour': postgres_get_avg_speed_by_hour,
                'get_max_min_wind_speed': postgres_get_max_min_wind_speed,
                'get_join_ship_weather_query': postgres_get_join_ship_weather_query
            }

    def get_query(self, query_name: str) -> str:
        query_func = self.queries.get(query_name)
        if query_func:
            return query_func()
        else:
            raise ValueError(f"Query '{query_name}' not found")

if __name__ == "__main__":
    local_execution = True  # or False, depending on your environment
    query_factory = QueryFactory(local_execution)
    query = query_factory.get_query('get_avg_speed_by_hour')
    print(query)