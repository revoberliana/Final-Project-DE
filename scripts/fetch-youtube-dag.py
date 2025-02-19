from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import logging
import requests
import pandas as pd
import os
import sys


QUERIES = [
    "Technology Trends 2024",
    "Machine Learning Trends 2024",
    "Big Data Trends 2024",
    "Cloud Computing Trends 2024",
    "Best Databases for AI Applications",
    "MLOps Best Practices",
    "Data Engineering Roadmap",
    "Serverless Computing Explained",
]

# Konfigurasi default args
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 2, 14),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

YOUTUBE_API_KEY = os.getenv("YOUTUBE_API_KEY")
YOUTUBE_API_URL = "https://www.googleapis.com/youtube/v3/search"

def fetch_youtube_data(query):
    """ Mengambil data dari YouTube API berdasarkan query """
    if not YOUTUBE_API_KEY:
        logging.error("❌ API Key tidak ditemukan! Periksa konfigurasi.")
        return
    
    params = {
        "part": "snippet",
        "q": query,
        "maxResults": 10,
        "order": "date",
        "type": "video",
        "key": YOUTUBE_API_KEY,
    }

    response = requests.get(YOUTUBE_API_URL, params=params)
    
    if response.status_code == 200:
        data = response.json()
        videos = []

        for item in data.get("items", []):
            video = {
                "video_id": item["id"]["videoId"],
                "title": item["snippet"]["title"],
                "published_at": item["snippet"]["publishedAt"],
                "channel_title": item["snippet"]["channelTitle"],
            }
            videos.append(video)

        # Simpan ke CSV
        output_path = f"/opt/airflow/data/youtube_{query.replace(' ', '_')}.csv"
        df = pd.DataFrame(videos)
        df.to_csv(output_path, index=False)
        logging.info(f"✅ Data berhasil disimpan: {output_path}")
    else:
        logging.error(f"❌ Gagal mengambil data YouTube: {response.text}")

# Definisi DAG
dag = DAG(
    "youtube_tech_trends_dag",
    default_args=default_args,
    description="DAG untuk mengambil data dari YouTube API terkait tren teknologi dan data science",
    schedule_interval=timedelta(days=1),  # Jalankan setiap hari
    catchup=False,
)

# Membuat task untuk setiap query
tasks = []
for query in QUERIES:
    task = PythonOperator(
        task_id=f"fetch_youtube_data_{query.replace(' ', '_').lower()}",
        python_callable=fetch_youtube_data,
        op_kwargs={"query": query},
        dag=dag,
    )
    tasks.append(task)

# Set urutan eksekusi task
tasks
