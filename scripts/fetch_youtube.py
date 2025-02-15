import os
import requests
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from dotenv import load_dotenv

# Load environment variables
load_dotenv()

# Konfigurasi API
API_KEY = os.getenv("YOUTUBE_API_KEY")
CATEGORIES = {"gaming": 20, "tutorial": 27, "vlog": 22}

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

# Definisi DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
    "catchup": False
}

dag = DAG(
    "fetch_youtube_data",
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
