from weather_etl.scripts.supabase.config import upload_to_supabase
import pandas as pd
import os
import requests

# ========== Extraction météo ==========
def extract_meteo(latitude: str, longitude: str, city: str , date: str) -> bool:
    try:
        url = "https://api.open-meteo.com/v1/forecast"
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'current': 'temperature_2m,apparent_temperature,relative_humidity_2m,wind_speed_10m,wind_direction_10m,wind_gusts_10m,precipitation,rain,showers,snowfall,weather_code,cloud_cover,pressure_msl,surface_pressure',
            'timezone': 'auto'
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        daily_data = data.get("current", {})

        if not daily_data or "time" not in daily_data:
            logging.error(f"Aucune donnée horaire disponible pour {city}")
            return False

        df = pd.DataFrame([daily_data])
        df['city'] = city
        df = df.sort_values(by='time')

        output_dir = f"data/raw/open_meteo/{date}"
        os.makedirs("data/raw/open_meteo", exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/meteo_{city}.csv"
        df.to_csv(output_path, index=False)

        # Upload vers Supabase
        upload_to_supabase(output_path, f"data/raw/open_meteo/{date}/meteo_{city}.csv")

        return True
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction pour {city}: {str(e)}")
        return False
