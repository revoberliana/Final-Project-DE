from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import os
import requests

# Konfigurasi default
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 2, 14),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

# Fungsi untuk menjalankan skrip YouTube API
def fetch_youtube_data():
    YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
    url = f"https://www.googleapis.com/youtube/v3/search?part=snippet&q=tech&type=video&key={YOUTUBE_API_KEY}"
    response = requests.get(url)
    print(response.json())

# Definisi DAG
dag = DAG(
    'test_connection_youtube_API_dag',
    default_args=default_args,
    description='DAG untuk menjalankan skrip YouTube API',
    schedule_interval=timedelta(days=1),
    catchup=False
)

# Task untuk menjalankan skrip Python
task_fetch_youtube_data = PythonOperator(
    task_id='fetch_youtube_data',
    python_callable=fetch_youtube_data,
    dag=dag,
)

task_fetch_youtube_data
