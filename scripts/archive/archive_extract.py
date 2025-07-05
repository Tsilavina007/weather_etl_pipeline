import os
import requests
import pandas as pd
import logging
from weather_etl.scripts.supabase.config import upload_to_supabase


def extract_meteo(latitude: str, longitude: str, city: str , start_date: str, end_date: str, timezone: str) -> bool:
    try:
        url = "https://archive-api.open-meteo.com/v1/archive"
        params = {
            'latitude': latitude,
            'longitude': longitude,
            'start_date': start_date,
            'end_date': end_date,
            'daily': 'weather_code,temperature_2m_min,temperature_2m_max,wind_speed_10m_max,wind_gusts_10m_max,uv_index_max,precipitation_sum,precipitation_probability_max,temperature_2m_mean,dew_point_2m_mean,cloud_cover_mean,cloud_cover_min,cloud_cover_max,dew_point_2m_max,dew_point_2m_min,relative_humidity_2m_max,relative_humidity_2m_min,relative_humidity_2m_mean,visibility_mean,visibility_min,visibility_max,wind_speed_10m_mean',
            'timezone': timezone
        }
        response = requests.get(url, params=params, timeout=10)
        response.raise_for_status()

        data = response.json()
        daily_data = data.get("daily", {})

        if not daily_data or "time" not in daily_data:
            logging.error(f"Aucune donnée horaire disponible pour {city}")
            return False

        df = pd.DataFrame(daily_data)
        df['city'] = city
        df = df.sort_values(by='time')

        output_dir = f"data/raw/archive/{start_date}_to_{end_date}"
        os.makedirs("data/raw/archive", exist_ok=True)
        os.makedirs(output_dir, exist_ok=True)
        output_file = f"{output_dir}/meteo_{city.lower().replace(' ', '_')}.csv"
        df.to_csv(output_file, index=False)

        print(f"✅ Données météo sauvegardées dans : {output_file}")

        # Uploader dans Supabase
        upload_to_supabase(output_file, output_file)

        return True

    except requests.exceptions.RequestException as e:
        logging.error(f"Erreur réseau/API pour {city}: {str(e)}")
    except KeyError as e:
        logging.error(f"Champ manquant dans la réponse pour {city}: {str(e)}")
    except Exception as e:
        logging.error(f"Erreur inattendue pour {city}: {str(e)}")

    return False
