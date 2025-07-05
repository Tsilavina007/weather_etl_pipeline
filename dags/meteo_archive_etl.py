from airflow import DAG
from airflow.providers.standard.operators.python import PythonOperator
from datetime import datetime

from weather_etl.scripts.archive.archive_extract import extract_meteo
from weather_etl.scripts.archive.archive_merge import merge_files
from weather_etl.utils.cities import CITIES


# Configuration par défaut du DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 5, 30),
}

with DAG(
    'meteo_archive_etl',
    default_args=default_args,
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Crée une tâche d'extraction pour chaque ville
    extract_tasks = [
        PythonOperator(
            task_id=f'extract_meteo_{city.lower().replace(" ", "_")}',
            python_callable=extract_meteo,
            op_args=[lat, lon, city,  "2023-06-10", "{{ ds }}", "auto"]
        )
        for city, (lat, lon) in CITIES.items()
    ]

    merge_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_files,
        op_args=["2023-06-10", "{{ ds }}"],
    )

    extract_tasks >> merge_task

merge_files("2023-06-01", "2025-07-05")
