from datetime import datetime
import os
import logging
import pandas as pd
from flask import Blueprint, render_template_string, request
from queries import get_distinct_ship_count, get_avg_speed_by_hour, get_max_min_wind_speed, get_join_ship_weather_query
from query_executor import LocalQueryExecutor, RemoteQueryExecutor
from utils import get_plot, is_valid_date, DeviceID
from config import LOCAL_EXECUTION

logging.basicConfig(level=logging.INFO)
routes = Blueprint('routes', __name__)

def execute_query(query: str) -> pd.DataFrame:
    if LOCAL_EXECUTION:
        return LocalQueryExecutor().execute_query(query)
    else:
        return RemoteQueryExecutor(os.getenv("REMOTE_BASE_URL")).execute_query(query)

@routes.route('/get-distinct-ship-count')
def get_ship_count():
    query = get_distinct_ship_count()
    logging.info("Getting distinct ship count")
    df = execute_query(query)
    return df.to_json(orient="records")

@routes.route('/get-avg-speed-by-hour')
def get_avg_speed():
    date = request.args.get("date", "2019-02-13")
    if not is_valid_date(date):
        return "Invalid date format. Expected YYYY-MM-DD.", 400
    date = datetime.strptime(date, "%Y-%m-%d").date()
    logging.info(f"Getting average ship speeds by hour for date: {date}")
    query = get_avg_speed_by_hour(date)
    df = execute_query(query)
    return df.to_json(orient="records")

@routes.route('/get-max-min-wind-speed')
def get_wind_speed():
    device_id = request.args.get("device_id", "DEVICE_ST_1A2090")
    try:
        device_id = DeviceID[device_id]
    except KeyError:
        return "Invalid device_id. Expected 'DEVICE_ST_1A2090' or 'DEVICE_0001'.", 400
    logging.info(f"Getting max and min wind speed for device: {device_id.value}")
    query = get_max_min_wind_speed(device_id.value)
    df = execute_query(query)
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
    query = get_join_ship_weather_query(device_id.value, date)
    df = execute_query(query) 
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
