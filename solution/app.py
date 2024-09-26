from flask import Flask
from routes import routes
from config import LOCAL_EXECUTION

if LOCAL_EXECUTION:
    import duckdb
    duckdb.sql("CREATE TABLE raw_messages AS SELECT * FROM read_csv_auto('data/raw_messages_clean.csv')")
    duckdb.sql("CREATE TABLE weather AS SELECT * FROM read_csv_auto('data/df_weather_final.csv')")

app = Flask(__name__)
app.register_blueprint(routes)

if __name__ == '__main__':
    app.run(debug=True)