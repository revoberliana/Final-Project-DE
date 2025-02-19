import os
import requests
import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from google.cloud import bigquery
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Konfigurasi API & BigQuery
API_KEY = os.getenv("YOUTUBE_API_KEY")
BQ_PROJECT = os.getenv("GCP_PROJECT_ID")
BQ_DATASET = os.getenv("GCP_DATASET_ID")
BQ_TABLE = "video_stats"
CATEGORIES = {"gaming": 20, "tutorial": 27, "vlog": 22}

# Fetch video dari YouTube API
def fetch_youtube_videos(category, **kwargs):
    category_id = CATEGORIES[category]
    url = "https://www.googleapis.com/youtube/v3/search"
    params = {
        "part": "snippet",
        "maxResults": 1000,
        "videoCategoryId": category_id,
        "type": "video",
        "order": "viewCount",
        "key": API_KEY
    }
    
    response = requests.get(url, params=params).json()
    video_ids = [item["id"]["videoId"] for item in response.get("items", [])]
    kwargs['ti'].xcom_push(key=f"videos_{category}", value=video_ids)
    print(f"Fetched {len(video_ids)} videos for category {category}")

# Fetch statistik video
def fetch_video_stats(category, **kwargs):
    ti = kwargs['ti']
    video_ids = ti.xcom_pull(task_ids=f"fetch_videos_{category}", key=f"videos_{category}")
    
    stats_list = []
    for video_id in video_ids:
        url = "https://www.googleapis.com/youtube/v3/videos"
        params = {
            "part": "statistics",
            "id": video_id,
            "key": API_KEY
        }
        res = requests.get(url, params=params).json()
        
        if "items" in res and res["items"]:
            stats = res["items"][0]["statistics"]
            stats_list.append({
                "category": category,
                "video_id": video_id,
                "views": int(stats.get("viewCount", 0)),
                "likes": int(stats.get("likeCount", 0)),
                "comments": int(stats.get("commentCount", 0))
            })
    
    ti.xcom_push(key=f"stats_{category}", value=stats_list)
    print(f"Fetched stats for {len(stats_list)} videos in category {category}")

# Simpan ke BigQuery
def save_to_bigquery(**kwargs):
    ti = kwargs['ti']
    client = bigquery.Client()
    table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}"
    
    all_data = []
    for category in CATEGORIES.keys():
        stats = ti.xcom_pull(task_ids=f"fetch_stats_{category}", key=f"stats_{category}")
        if stats:
            all_data.extend(stats)
    
    if all_data:
        df = pd.DataFrame(all_data)
        job = client.load_table_from_dataframe(df, table_id)
        job.result()
        print(f"Data berhasil dimasukkan ke {table_id}")
    else:
        print("Tidak ada data untuk dimasukkan ke BigQuery.")

# Definisi DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "catchup": False
}

dag = DAG(
    "youtube_data_pipeline",
    default_args=default_args,
    schedule_interval="@daily",
    catchup=False
)

for category in CATEGORIES.keys():
    fetch_videos = PythonOperator(
        task_id=f"fetch_videos_{category}",
        python_callable=fetch_youtube_videos,
        op_kwargs={"category": category},
        dag=dag
    )

    fetch_stats = PythonOperator(
        task_id=f"fetch_stats_{category}",
        python_callable=fetch_video_stats,
        op_kwargs={"category": category},
        dag=dag
    )

    fetch_videos >> fetch_stats

save_data = PythonOperator(
    task_id="save_to_bigquery",
    python_callable=save_to_bigquery,
    dag=dag
)

fetch_stats >> save_data
