from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
from weather_etl.scripts.openweather.open_weather_extract import extract_meteo
from weather_etl.scripts.openweather.open_weather_merge import merge_files
from weather_etl.utils.cities import CITIES

# DAG Airflow
with DAG(
    'open_weather_etl_pipeline',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2025, 5, 30),
    },
    schedule='@daily',
    catchup=False,
    max_active_runs=1,
) as dag:

    extract_tasks = [
        PythonOperator(
            task_id=f'extract_{city.lower().replace(" ", "_")}',
            python_callable=extract_meteo,
            op_args=[city, "{{ var.value.API_KEY }}", "{{ ds }}"]
        )
        for city in CITIES
    ]

    merge_task = PythonOperator(
        task_id='merge_files',
        python_callable=merge_files,
        op_args=["{{ ds }}"],
    )

    extract_tasks >> merge_task

