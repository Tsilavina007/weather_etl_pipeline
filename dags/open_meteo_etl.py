from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from weather_etl.scripts.openmeteo.open_meteo_extract import extract_meteo
from weather_etl.scripts.openmeteo.open_meteo_merge import merge_files
from weather_etl.utils.cities import CITIES

# DAG Airflow
with DAG(
    'open_meteo_etl_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 5, 30),
    },
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    # Crée une tâche d'extraction pour chaque ville
    extract_tasks = [
        PythonOperator(
            task_id=f'extract_{city.lower().replace(" ", "_")}',
            python_callable=extract_meteo,
            op_args=[lat, lon, city, "{{ ds }}"]
        )
        for city, (lat, lon) in CITIES.items()
    ]

    merge_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_files,
        op_args=["{{ ds }}"],
    )


    extract_tasks >> merge_task

