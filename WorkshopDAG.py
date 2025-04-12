from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta

from ExtractW import extract_grammy_data, extract_spotify_data, extract_musicbrainz_data
from transform_spotify_data import transform_spotify_data
from transform_grammy_data import transform_grammy_data
from transform_API_data import transform_api_data
from merge_data import merge_datasets
from Load import load_data
from Store import store_to_drive

default_args = {
    "owner": "airflow",
    "start_date": datetime(2025, 3, 25),
    "retries": 1,
}

dag = DAG(
    dag_id="WorkshopDAG",
    default_args=default_args,
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Extraction Tasks
extract_grammy = PythonOperator(
    task_id="extract_grammy_data",
    python_callable=extract_grammy_data,
    dag=dag,
)

extract_spotify = PythonOperator(
    task_id="extract_spotify_data",
    python_callable=lambda: extract_spotify_data("/home/dcontreras/Workshop002/dataset.csv"),
    dag=dag,
)

extract_api = PythonOperator(
    task_id="extract_musicbrainz_data",
    python_callable=extract_musicbrainz_data,
    dag=dag,
)

# Transformation Tasks
transform_grammy = PythonOperator(
    task_id="transform_grammy_data",
    python_callable=transform_grammy_data,
    dag=dag,
)

transform_spotify = PythonOperator(
    task_id="transform_spotify_data",
    python_callable=transform_spotify_data,
    dag=dag,
)

transform_api = PythonOperator(
    task_id="transform_api_data",
    python_callable=transform_api_data,
    dag=dag,
)

# Merge Task
merge_task = PythonOperator(
    task_id="merge_datasets",
    python_callable=merge_datasets,
    dag=dag,
)

# Load Task
load_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

# Store Task
store_task = PythonOperator(
    task_id="store_data",
    python_callable=store_to_drive,
    dag=dag,
)

# DAG Dependencies
extract_grammy >> transform_grammy
extract_spotify >> transform_spotify
extract_api >> transform_api

[transform_grammy, transform_spotify, transform_api] >> merge_task
merge_task >> load_task >> store_task