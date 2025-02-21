import os
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.docker.operators.docker import DockerOperator
from docker.types import Mount

# Gunakan absolute path untuk mounting
BASE_DIR = os.getenv("AIRFLOW_HOME", "/opt/airflow/dbt")

dag = DAG(
    "dbt_bigquery_update",
    schedule_interval="0 15 * * *",
    start_date=days_ago(1),
    catchup=False,
)

# Task untuk menjalankan dbt run di staging
dbt_staging = DockerOperator(
    task_id="run_dbt_staging",
    image="ghcr.io/dbt-labs/dbt-bigquery:latest",
    api_version="auto",
    auto_remove=True,
    command="dbt run --select staging.stg_youtube_data",
    network_mode="bridge",
    mounts=[
        Mount(source=f"{BASE_DIR}/youtube_trending", target="/usr/app/dbt", type="bind"),
        Mount(source=f"{BASE_DIR}/profiles.yml", target="/root/.dbt/profiles.yml", type="bind"),
    ],
    dag=dag,
)

# Task untuk menjalankan dbt run di fact table
dbt_fact = DockerOperator(
    task_id="run_dbt_fact",
    image="ghcr.io/dbt-labs/dbt-bigquery:latest",
    api_version="auto",
    auto_remove=True,
    command="dbt run --select marts.fact_video_performance",
    network_mode="bridge",
    mounts=[
        Mount(source=f"{BASE_DIR}/youtube_trending", target="/usr/app/dbt", type="bind"),
        Mount(source=f"{BASE_DIR}/profiles.yml", target="/home/airflow/.dbt/profiles.yml", type="bind"),
    ],
    dag=dag,
)

dbt_staging >> dbt_fact
