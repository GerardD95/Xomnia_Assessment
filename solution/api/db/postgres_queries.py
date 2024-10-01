def get_distinct_ship_count() -> str:
    """Distinct ship count"""
    query = "SELECT COUNT(DISTINCT device_id) AS ship_count FROM raw_messages"
    return query

def get_avg_speed_by_hour() -> str:
    """Avg speed for all available ships for each hour of the date: {date}"""
    query = """
        SELECT device_id, TO_CHAR(datetime, 'HH24') AS hour, AVG(spd_over_grnd) AS avg_speed
        FROM raw_messages
        WHERE TO_CHAR(datetime, 'YYYY-MM-DD') = TO_CHAR(%s, 'YYYY-MM-DD')
        GROUP BY 1,2
        ORDER BY 1,2
    """
    return query

def get_max_min_wind_speed() -> str:
    """Max & min wind speed for every available day for ship: {device_id}"""
    query = """
        SELECT 
            TO_CHAR(s.datetime, 'YYYY-MM-DD') as date,
            MAX(w.wind_spd) AS max_wind_speed,
            MIN(w.wind_spd) AS min_wind_speed
        FROM 
            raw_messages s
        JOIN 
            weather w
                ON 
            TO_CHAR(s.datetime, 'YYYY-MM-DD') = TO_CHAR(w.timestamp_local, 'YYYY-MM-DD')
        WHERE 
            s.device_id = %s
        GROUP BY TO_CHAR(s.datetime, 'YYYY-MM-DD')
        ORDER BY TO_CHAR(s.datetime, 'YYYY-MM-DD')
    """
    return query

def get_join_ship_weather_query() -> str:
    """
        A way to visualize full weather conditions across route of the ship: {device_id}, for date: {date}.
        (example fields: general description, temperature, wind speed)

        Args:
            device_id (str): Device ID of the ship
            date (str): Date in the format 'YYYY-MM-DD'

        Returns:
            str: SQL query to join ship and weather data
    """

    return """
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
            TO_CHAR(s.datetime, 'YYYY-MM-DD HH24:00:00') = TO_CHAR(w.timestamp_local, 'YYYY-MM-DD HH24:00:00')
            AND ROUND(s.lat::numeric, 4) = ROUND(w.lat::numeric, 4)
            AND ROUND(s.lon::numeric, 4) = ROUND(w.lon::numeric, 4)
        WHERE 
            s.device_id = %s
            AND TO_CHAR(s.datetime, 'YYYY-MM-DD') = TO_CHAR(%s, 'YYYY-MM-DD')
    """