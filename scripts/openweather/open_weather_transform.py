import pandas as pd
import os
from weather_etl.scripts.supabase.config import upload_to_supabase


# ========== Transformation (Schéma en étoile) ==========
def transform_to_star() -> str:
    input_file = "data/processed/open_weather/meteo_global.csv"
    output_dir = "data/star_schema"
    os.makedirs(output_dir, exist_ok=True)
    meteo_data = pd.read_csv(input_file)

    # Dimension Pays
    dim_pays_path = f"{output_dir}/dim_pays.csv"
    if os.path.exists(dim_pays_path):
        dim_pays = pd.read_csv(dim_pays_path)
    else:
        dim_pays = pd.DataFrame(columns=['pays_id', 'pays'])

    pays_existants = set(dim_pays['pays'])
    nouveaux_pays = set(meteo_data['pays']) - pays_existants

    if nouveaux_pays:
        next_id = dim_pays['pays_id'].max() + 1 if not dim_pays.empty else 1
        new_rows = pd.DataFrame({
            'pays_id': range(next_id, next_id + len(nouveaux_pays)),
            'pays': list(nouveaux_pays)
        })
        dim_pays = pd.concat([dim_pays, new_rows], ignore_index=True)
        dim_pays.to_csv(dim_pays_path, index=False)

    meteo_data = meteo_data.merge(dim_pays, on='pays', how='left')

    # Dimension Ville
    dim_ville_path = f"{output_dir}/dim_ville.csv"
    if os.path.exists(dim_ville_path):
        dim_ville = pd.read_csv(dim_ville_path)
    else:
        dim_ville = pd.DataFrame(columns=['ville_id', 'ville', 'pays_id'])

    villes_existantes = set(zip(dim_ville['ville'], dim_ville['pays_id']))
    nouvelles_villes = {
        (row['ville'], row['pays_id'])
        for _, row in meteo_data.iterrows()
        if (row['ville'], row['pays_id']) not in villes_existantes
    }

    if nouvelles_villes:
        next_id = dim_ville['ville_id'].max() + 1 if not dim_ville.empty else 1
        new_villes = pd.DataFrame({
            'ville_id': range(next_id, next_id + len(nouvelles_villes)),
            'ville': [v[0] for v in nouvelles_villes],
            'pays_id': [v[1] for v in nouvelles_villes]
        })
        dim_ville = pd.concat([dim_ville, new_villes], ignore_index=True)
        dim_ville.to_csv(dim_ville_path, index=False)

    # Table de faits
    faits = meteo_data.merge(dim_ville, on=['ville', 'pays_id'], how='left')
    faits = faits.drop(columns=['ville', 'pays', 'pays_id'])

    facts_path = f"{output_dir}/fact_weather.csv"
    faits.to_csv(facts_path, index=False)

    return facts_path

