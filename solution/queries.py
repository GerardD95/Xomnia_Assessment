def get_distinct_ship_count() -> str:
    """Distinct ship count"""
    query = "SELECT COUNT(DISTINCT device_id) AS ship_count FROM raw_messages"
    return query

def get_avg_speed_by_hour(date: str) -> str:
    """Avg speed for all available ships for each hour of the date: {date}"""
    query = f"""
        SELECT device_id, strftime(datetime, '%H') AS hour, AVG(spd_over_grnd) AS avg_speed
        FROM raw_messages
        WHERE strftime(datetime, '%Y-%m-%d') = '{date}'
        GROUP BY 1,2
        ORDER BY 1,2
    """
    return query

def get_max_min_wind_speed(device_id: str) -> str:
    """Max & min wind speed for every available day for ship: {device_id}"""
    query = f"""
        SELECT 
            strftime('%Y-%m-%d', s.datetime) as date,
            MAX(w.wind_spd) AS max_wind_speed,
            MIN(w.wind_spd) AS min_wind_speed
        FROM 
            raw_messages s
        JOIN 
            weather w
                ON 
            strftime('%Y-%m-%d', s.datetime) = strftime('%Y-%m-%d', w.timestamp_local)
        WHERE 
            s.device_id = '{device_id}'
        GROUP BY strftime('%Y-%m-%d', s.datetime)
        ORDER BY strftime('%Y-%m-%d', s.datetime)
    """
    return query

def get_join_ship_weather_query(device_id: str, date: str) -> str:
    """
        A way to visualize full weather conditions across route of the ship: {device_id}, for date: {date}.
        (example fields: general description, temperature, wind speed)

        Args:
            device_id (str): Device ID of the ship
            date (str): Date in the format 'YYYY-MM-DD'

        Returns:
            str: SQL query to join ship and weather data
    """

    return f"""
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
            raw_messages s
        LEFT JOIN 
            weather w
        ON 
            strftime(s.datetime, '%Y-%m-%d %H:00:00') = strftime(w.timestamp_local, '%Y-%m-%d %H:00:00')
            AND ROUND(s.lat, 4) = ROUND(w.lat, 4)
            AND ROUND(s.lon, 4) = ROUND(w.lon, 4)
        WHERE 
            s.device_id = '{device_id}'
            AND strftime(s.datetime, '%Y-%m-%d') = '{date}'
    """