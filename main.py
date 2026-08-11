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

def fetch_weather_and_forecast(lat, lon):
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
        "hourly": [
            "temperature_2m",
            "relative_humidity_2m",
            "surface_pressure",
            "wind_speed_10m",
            "precipitation"
        ],
        "daily": ["sunrise", "sunset"],
        "forecast_days": 7,
        "timezone": "auto"
    }
    response = requests.get(url, params=params, timeout=10)
    response.raise_for_status()
    return response.json()

def run_pipeline():
    rows_to_insert = []
    
    for city_name, coords in CITIES.items():
        try:
            raw_data = fetch_weather_and_forecast(coords["lat"], coords["lon"])
            
            # 1. Registro actual
            current = raw_data.get("current", {})
            daily = raw_data.get("daily", {})
            
            time_str = current.get("time")
            dt = datetime.fromisoformat(time_str).replace(tzinfo=timezone.utc)
            
            sunrise_str = daily.get("sunrise", [None])[0]
            sunset_str = daily.get("sunset", [None])[0]
            sunrise_dt = datetime.fromisoformat(sunrise_str).replace(tzinfo=timezone.utc) if sunrise_str else None
            sunset_dt = datetime.fromisoformat(sunset_str).replace(tzinfo=timezone.utc) if sunset_str else None
            
            rows_to_insert.append((
                dt,
                city_name,
                current.get("temperature_2m"),
                current.get("relative_humidity_2m"),
                current.get("wind_speed_10m"),
                current.get("surface_pressure"),
                current.get("precipitation", 0.0),
                sunrise_dt,
                sunset_dt
            ))
            
            # 2. Registros de pronóstico futuro (cada 3 horas para no saturar)
            hourly = raw_data.get("hourly", {})
            times = hourly.get("time", [])
            temps = hourly.get("temperature_2m", [])
            hums = hourly.get("relative_humidity_2m", [])
            winds = hourly.get("wind_speed_10m", [])
            press = hourly.get("surface_pressure", [])
            precips = hourly.get("precipitation", [])
            
            now_utc = datetime.now(timezone.utc)
            
            for i in range(0, len(times), 3):  # Muestra cada 3 horas
                f_dt = datetime.fromisoformat(times[i]).replace(tzinfo=timezone.utc)
                if f_dt > now_utc:
                    rows_to_insert.append((
                        f_dt,
                        city_name,
                        temps[i],
                        hums[i],
                        winds[i],
                        press[i],
                        precips[i],
                        None,
                        None
                    ))
                    
        except Exception as e:
            print(f"Error procesando {city_name}: {e}")
            
    if not rows_to_insert:
        print("No hay registros para insertar.")
        return

    # Usamos ON CONFLICT / UPDATE o inserción simple
    query = """
        INSERT INTO weather_metrics 
        (timestamp, city, temperature, humidity, wind_speed, pressure, precipitation, sunrise, sunset)
        VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        ON CONFLICT DO NOTHING;
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
