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

def fetch_weather(lat, lon):
    url = "https://api.open-meteo.com/v1/forecast"
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": ["temperature_2m", "relative_humidity_2m", "wind_speed_10m", "surface_pressure", "precipitation"],
        "forecast_days": 7,
        "timezone": "UTC"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def run_pipeline():
    rows_to_insert = []
    
    for city_name, coords in CITIES.items():
        try:
            data = fetch_weather(coords["lat"], coords["lon"])
            hourly = data.get("hourly", {})
            
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            hums = hourly.get("relative_humidity_2m", [])
            winds = hourly.get("wind_speed_10m", [])
            press = hourly.get("surface_pressure", [])
            precips = hourly.get("precipitation", [])
            
            for i in range(len(times)):
                # Convertir timestamp ISO string directamente
                dt = datetime.fromisoformat(times[i]).replace(tzinfo=timezone.utc)
                rows_to_insert.append((
                    dt,
                    city_name,
                    temps[i],
                    hums[i],
                    winds[i],
                    press[i],
                    precips[i]
                ))
        except Exception as e:
            print(f"Error procesando {city_name}: {e}")
            
    if not rows_to_insert:
        print("No hay registros para insertar.")
        return

    query = """
        INSERT INTO weather_metrics 
        (timestamp, city, temperature, humidity, wind_speed, pressure, precipitation)
        VALUES (%s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT (city, timestamp) DO UPDATE 
        SET temperature = EXCLUDED.temperature,
            humidity = EXCLUDED.humidity,
            wind_speed = EXCLUDED.wind_speed,
            pressure = EXCLUDED.pressure,
            precipitation = EXCLUDED.precipitation;
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
        print(f"[{datetime.now().isoformat()}] Insercion exitosa de {len(rows_to_insert)} registros de pronostico.")
    except Exception as e:
        print(f"Error de conexion o insercion: {e}")

if __name__ == "__main__":
    run_pipeline()
