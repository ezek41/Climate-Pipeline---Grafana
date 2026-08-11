import os
import requests
import psycopg2
from datetime import datetime, timezone

DB_HOST = os.getenv("DB_HOST")
DB_PORT = os.getenv("DB_PORT", "5432")
DB_NAME = os.getenv("DB_NAME")
DB_USER = os.getenv("DB_USER")
DB_PASS = os.getenv("DB_PASS")

CITIES = {
    "Rosario": {"lat": -32.9468, "lon": -60.6393},
    "Buenos Aires": {"lat": -34.6037, "lon": -58.3816}
}

def fetch_weather_data(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "current": [
            "temperature_2m",
            "relative_humidity_2m",
            "surface_pressure",
            "wind_speed_10m",
            "precipitation"
        ],
        "daily": ["sunrise", "sunset"],
        "forecast_days": 1,
        "timezone": "auto"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def run_pipeline():
    rows_to_insert = []
    
    for city_name, coords in CITIES.items():
        try:
            raw_data = fetch_weather_data(coords["lat"], coords["lon"])
            current = raw_data.get("current", {})
            daily = raw_data.get("daily", {})
            
            # Formatear timestamps
            time_str = current.get("time")
            dt = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
            
            sunrise_str = daily.get("sunrise", [None])[0]
            sunset_str = daily.get("sunset", [None])[0]
            
            sunrise_dt = datetime.fromisoformat(sunrise_str).replace(tzinfo=timezone.utc) if sunrise_str else None
            sunset_dt = datetime.fromisoformat(sunset_str).replace(tzinfo=timezone.utc) if sunset_str else None
            
            row = (
                dt,
                city_name,
                current.get("temperature_2m"),
                current.get("relative_humidity_2m"),
                current.get("wind_speed_10m"),
                current.get("surface_pressure"),
                current.get("precipitation", 0.0),
                sunrise_dt,
                sunset_dt
            )
            rows_to_insert.append(row)
        except Exception as e:
            print(f"Error procesando {city_name}: {e}")
            
    if not rows_to_insert:
        print("No hay registros para insertar.")
        return

    query = """
        INSERT INTO weather_metrics 
        (timestamp, city, temperature, humidity, wind_speed, pressure, precipitation, sunrise, sunset)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s);
    """
    
    try:
        conn = psycopg2.connect(
            host=DB_HOST,
            port=DB_PORT,
            dbname=DB_NAME,
            user=DB_USER,
            password=DB_PASS
        )
        with conn.cursor() as cursor:
            cursor.executemany(query, rows_to_insert)
        conn.commit()
        conn.close()
        print(f"[{datetime.now().isoformat()}] Inserción exitosa de {len(rows_to_insert)} registros.")
    except Exception as e:
        print(f"Error de conexión o inserción: {e}")

if __name__ == "__main__":
    run_pipeline()