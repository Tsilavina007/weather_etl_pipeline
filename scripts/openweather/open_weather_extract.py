from datetime import datetime
import pandas as pd
import os
import requests
import logging
from weather_etl.scripts.supabase.config import upload_to_supabase

# ========== Extraction météo ==========
def extract_meteo(city: str, api_key: str, date: str) -> bool:
    try:
        url = "https://api.openweathermap.org/data/2.5/weather"
        params = {
            'q': city,
            'appid': api_key,
            'units': 'metric',
            'lang': 'fr'
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()
        data = response.json()

        weather_data = {
            'ville': data.get('name', 'Inconnue'),
            'pays': data.get('sys', {}).get('country', 'N/A'),
            'date_extraction': datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
            'temperature': data['main']['temp'],
            'temperature_min': data['main']['temp_min'],
            'temperature_max': data['main']['temp_max'],
            'ressentie': data['main']['feels_like'],
            'humidite': data['main']['humidity'],
            'pression_mer': data['main']['pressure'],
            'pression_sol': data['main'].get('grnd_level'),
            'visibilite': data.get('visibility'),
            'vent_vitesse': data['wind']['speed'],
            'vent_direction': data['wind']['deg'],
            'nuages': data['clouds']['all'],
            'condition': data['weather'][0]['main'],
            'description': data['weather'][0]['description'],
            'heure_lever_soleil': data['sys']['sunrise'],
            'heure_coucher_soleil': data['sys']['sunset'],
            'timezone': data.get('timezone', 0)
        }

        output_dir = f"data/raw/open_weather/{date}"
        os.makedirs(output_dir, exist_ok=True)
        output_path = f"{output_dir}/meteo_{city}.csv"
        pd.DataFrame([weather_data]).to_csv(output_path, index=False)

        # Upload vers Supabase
        upload_to_supabase(output_path, f"data/raw/open_weather/{date}/meteo_{city}.csv")

        return True
    except Exception as e:
        logging.error(f"Erreur lors de l'extraction pour {city}: {str(e)}")
        return False
