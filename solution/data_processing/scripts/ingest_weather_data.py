import logging 
import duckdb
import pandas as pd
from sqlalchemy import text
from utils.utils import read_sql_file, get_google_cloud_sql_connection


def main(target='local'):
    weather_data = '../../data/df_weather_final.csv'

    if target == 'local':
        with duckdb.connect('../local_duckdb.db') as conn:
            schema_file = 'schemas/duckdb_weather_schema.sql'
            query = read_sql_file(schema_file)
            with conn.cursor() as cur:
                cur.execute(query)
                conn.commit()
            conn.execute(f"INSERT INTO weather SELECT * FROM read_csv_auto('{weather_data}')")
    else:
        with get_google_cloud_sql_connection() as conn:
            schema_file = 'schemas/gcp_weather_schema.sql'
            query = read_sql_file(schema_file)
            df = pd.read_csv(weather_data)
            df['timestamp_local'] = pd.to_datetime(df['timestamp_local'])
            with conn.begin() as trans:
                try:
                    logging.info(f"Creating tables: \n{query}")
                    conn.execute(text("DROP TABLE IF EXISTS weather"))
                    conn.execute(text(query))
                    df.to_sql('weather', conn, if_exists='append', index=False)
                    trans.commit()
                except Exception as e:
                    trans.rollback()
                    logging.error(f"Error creating tables: {e}")
                    raise

    print('Weather data inserted.')
                
if __name__ == '__main__':
    import sys
    if len(sys.argv) < 2 or sys.argv[1] not in ['local', 'gcp']:
        print('Please provide an argument: local|gcp')
        raise ValueError('Invalid argument')

    main(sys.argv[1])