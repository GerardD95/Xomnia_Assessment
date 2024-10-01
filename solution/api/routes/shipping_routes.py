import logging
from datetime import datetime
from flask import Blueprint, render_template_string, request

from db.query_loader import QueryFactory
from db.query_executor import QueryExecutorFactory
from config import LOCAL_EXECUTION, SOURCE_TYPE, SOURCE_PATH, DB_URL
from utils.utils import get_plot, is_valid_date, DeviceID


logging.basicConfig(level=logging.INFO)
routes = Blueprint('routes', __name__)
query_factory = QueryFactory(LOCAL_EXECUTION)
query_executor = QueryExecutorFactory(LOCAL_EXECUTION, DB_URL, SOURCE_TYPE, SOURCE_PATH).get_executor()

@routes.route('/get-distinct-ship-count')
def get_ship_count():
    logging.info("Getting distinct ship count")
    query = query_factory.get_query('get_distinct_ship_count')
    df = query_executor.execute_query(query)
    return df.to_json(orient="records")

@routes.route('/get-avg-speed-by-hour')
def get_avg_speed():
    date = request.args.get("date", "2019-02-13")
    if not is_valid_date(date):
        return "Invalid date format. Expected YYYY-MM-DD.", 400
    date = datetime.strptime(date, "%Y-%m-%d").date()
    logging.info(f"Getting average ship speeds by hour for date: {date}")
    query = query_factory.get_query('get_avg_speed_by_hour')
    df = query_executor.execute_query(query, (date,))
    return df.to_json(orient="records")

@routes.route('/get-max-min-wind-speed')
def get_wind_speed():
    device_id = request.args.get("device_id", "DEVICE_ST_1A2090")
    try:
        device_id = DeviceID[device_id]
    except KeyError:
        return "Invalid device_id. Expected 'DEVICE_ST_1A2090' or 'DEVICE_0001'.", 400
    logging.info(f"Getting max and min wind speed for device: {device_id.value}")
    query = query_factory.get_query('get_max_min_wind_speed')
    df = query_executor.execute_query(query, (device_id.value,))
    return df.to_json(orient="records")

@routes.route('/get-weather-conditions-along-route')
def get_weather_conditions_along_route():
    device_id = request.args.get("device_id", "DEVICE_ST_1A2090")
    date = request.args.get("date", "2019-02-13")
    try:
        device_id = DeviceID[device_id]
    except KeyError:
        return "Invalid device_id. Expected 'DEVICE_ST_1A2090' or 'DEVICE_0001'.", 400
    if not is_valid_date(date):
        return "Invalid date format. Expected YYYY-MM-DD.", 400
    date = datetime.strptime(date, "%Y-%m-%d").date()
    logging.info(f"Getting weather conditions along route for device: {device_id.value}, date: {date}") 
    query = query_factory.get_query('get_join_ship_weather_query')
    df = query_executor.execute_query(query, (device_id.value, date)) 
    if df.empty:
        return "No data found for the given parameters.", 404
    fig = get_plot(df, device_id.value, date)
    plot_html = fig.to_html(full_html=False, include_plotlyjs='cdn')

    # Return the Plotly figure as an HTML response
    return render_template_string("""
        <!DOCTYPE html>
        <html>
        <head>
            <title>Ship Route Plot</title>
            <style>
                .plot-container {
                    display: flex;
                    justify-content: center;
                    align-items: center;
                    height: 100vh;
                    overflow: hidden
                }
                .plotly-graph-div {
                    width: 1000px;
                    height: 800px;
                }
            </style>
        </head>
        <body class="plot-container">
            <div class="plotly-graph-div">
                {{ plot_html|safe }}
            </div>
        </body>
        </html>
    """, plot_html=plot_html)
