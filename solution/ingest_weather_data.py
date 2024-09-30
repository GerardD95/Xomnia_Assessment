import duckdb


def main(target='local'):
    if target == 'local':
        with duckdb.connect('solution/local_duckdb.db') as conn:
            conn.execute(f"CREATE TABLE weather AS SELECT * FROM read_csv_auto('data/df_weather_final.csv')")
        print('Weather data table added to local database')
    elif target == 'gcp':
        print('Running on GCP')

if __name__ == '__main__':
    import sys
    if sys.argv[1] == 'local':
        main()
    elif sys.argv[1] == 'gcp':
        main()
    else: 
        print('Invalid argument. Please use "local" or "gcp"')