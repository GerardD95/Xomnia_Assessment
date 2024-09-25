# 1. distinct ship count
# 2. Avg speed for all available ships for each hour of the date 2019-02-13
# 3. Max & min wind speed for every available day for ship ”st-1a2090” only
# 4. A way to visualize full weather conditions (example fields: general description, temperature, wind speed) across route of the ship ”st-1a2090” for date 2019-02-13. In case of time pressure, the application could simply return the requested data instead of a visualization.  
    # Hint: The “visualization” could be as simple or complex you want, from a simple table to a using a map layer to visualize the route of the ship, is up to you :)

import duckdb
import plotly.express as px


# 1. distinct ship count
print(duckdb.sql("SELECT COUNT(DISTINCT device_id) FROM 'data/raw_messages_clean.csv'"))

# 2. Avg speed for all available ships for each hour of the date 2019-02-13
print(duckdb.sql("""
    SELECT strftime(datetime, '%H') AS hour, AVG(spd_over_grnd) AS avg_speed
    FROM 'data/raw_messages_clean.csv'
    WHERE strftime(datetime, '%Y-%m-%d') = '2019-02-13'
    GROUP BY hour
    ORDER BY hour
"""))

# 3. Max & min wind speed for every available day for ship ”st-1a2090” only
query = """
    SELECT 
        CAST(s.datetime AS DATE) as date,
        MAX(w.wind_spd) AS max_wind_speed,
        MIN(w.wind_spd) AS min_wind_speed
    FROM 
        'data/raw_messages_clean.csv' s
    JOIN 
        'data/df_weather_final.csv' w
    ON 
        strftime(s.datetime, '%Y-%m-%d %H:00:00') = strftime(w.timestamp_local, '%Y-%m-%d %H:00:00')
        AND ROUND(s.lat, 4) = ROUND(w.lat, 4)
        AND ROUND(s.lon, 4) = ROUND(w.lon, 4)
    WHERE 
        s.device_id = 'st-1a2090'
    GROUP BY CAST(s.datetime AS DATE)
    ORDER BY CAST(s.datetime AS DATE)
"""

print(duckdb.sql(query))

# 4. A way to visualize full weather conditions (example fields: general description, temperature, wind speed) across route of the ship ”st-1a2090” for date 2019-02-13. In case of time pressure, the application could simply return the requested data instead of a visualization.
query = """
    SELECT 
        s.datetime,
        s.device_id,
        s.lat,
        s.lon,
        s.spd_over_grnd AS ship_speed,
        w.wind_spd AS weather_wind_speed,
        w.temp AS weather_temp,
        w.weather_description 
    FROM 
        'data/raw_messages_clean.csv' s
    JOIN 
        'data/df_weather_final.csv' w
    ON 
        strftime(s.datetime, '%Y-%m-%d %H:00:00') = strftime(w.timestamp_local, '%Y-%m-%d %H:00:00')
        AND ROUND(s.lat, 4) = ROUND(w.lat, 4)
        AND ROUND(s.lon, 4) = ROUND(w.lon, 4)
    WHERE 
        s.device_id = 'st-1a2090'
        AND strftime(s.datetime, '%Y-%m-%d') = '2019-02-13'
"""

# merged_df = duckdb.sql(query).fetchdf()

# # Create a Plotly scatter mapbox figure
# fig = px.scatter_mapbox(
#     merged_df,
#     lat="lat",
#     lon="lon",
#     color="weather_wind_speed",
#     size="ship_speed",
#     hover_name="weather_description",
#     hover_data={"weather_temp": True, "weather_wind_speed": True, "ship_speed": True},
#     title="Route of Ship st-1a2090 with Weather Conditions on 2019-02-13",
#     mapbox_style="carto-positron",
#     zoom=5
# )

# # Show the figure
# fig.show()