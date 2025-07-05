import pandas as pd
import os

from weather_etl.scripts.supabase.config import upload_to_supabase

# ========== Fusion des fichiers ==========
def merge_files(date: str) -> str:
    input_dir = f"data/raw/open_meteo/{date}"
    output_path = "data/processed/open_meteo/meteo_global.csv"

    os.makedirs(os.path.dirname(output_path), exist_ok=True)

    if os.path.exists(output_path):
        global_df = pd.read_csv(output_path)
    else:
        global_df = pd.DataFrame()

    new_data = []
    for file in os.listdir(input_dir):
        if file.startswith('meteo_') and file.endswith('.csv'):
            new_data.append(pd.read_csv(f"{input_dir}/{file}"))

    if not new_data:
        raise ValueError(f"Aucune nouvelle donnée pour {date}")

    updated_df = pd.concat([global_df] + new_data, ignore_index=True)
    updated_df = updated_df.drop_duplicates(
        subset=['city', 'time'],
        keep='last'
    )

    updated_df.to_csv(output_path, index=False)

    # Upload vers Supabase
    upload_to_supabase(output_path, "data/processed/open_meteo/meteo_global.csv")

    return output_path
