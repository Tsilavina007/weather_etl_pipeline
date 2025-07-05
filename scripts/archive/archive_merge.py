import os
import pandas as pd
from weather_etl.scripts.supabase.config import upload_to_supabase

def merge_files(start_date: str, end_date: str) -> str:
    input_dir = f"data/raw/archive/{start_date}_to_{end_date}"
    output_filename = f"data/processed/archive/meteo_global_{start_date}_to_{end_date}.csv"

    new_data = []
    for file in os.listdir(input_dir):
        if file.startswith('meteo_') and file.endswith('.csv'):
            new_data.append(pd.read_csv(f"{input_dir}/{file}"))

    if not new_data:
        raise ValueError(f"Aucune nouvelle donnée à fusionner pour {start_date} -> {end_date}")

    updated_df = pd.concat(new_data, ignore_index=True)

    updated_df.to_csv(output_filename, index=False)

    print(f"✅ Données météo sauvegardées dans : {output_filename}")

    upload_to_supabase(output_filename, output_filename)

    return output_filename
