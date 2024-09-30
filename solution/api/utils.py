from datetime import datetime
from enum import Enum
import pandas as pd
import plotly.express as px


class DeviceID(Enum):
    DEVICE_ST_1A2090 = "st-1a2090"
    DEVICE_0001 = "0001"

def is_valid_date(date: str) -> bool:
    try:
        datetime.strptime(date, "%Y-%m-%d")
        return True
    except ValueError:
        return False

def get_plot(df: pd.DataFrame, device_id: str, date: str) -> px.scatter_mapbox:
    df['has_weather_data'] = df['weather_temp'].notnull() & df['weather_wind_speed'].notnull() & df['weather_description'].notnull()
    df['marker_size'] = df['has_weather_data'].map({True: 1, False: .1})
    fig = px.scatter_mapbox(
        df,
        lat="lat",
        lon="lon",
        color="weather_temp",
        size="marker_size",
        hover_name="datetime",
        hover_data={
            "marker_size": False,
            "ship_speed": True,
            "weather_temp": True, 
            "weather_wind_speed": True, 
            "weather_description": True
        },
        title=f"Route of Ship {device_id} with Weather Conditions on {date}",
        mapbox_style="carto-positron",
        zoom=7
    )
    fig.update_layout(width=1000, height=800)
    return fig
    