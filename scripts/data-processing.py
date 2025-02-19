from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import json
import isodate
import os

def process_youtube_data(source_file_path: str, target_file_path: str, categories_file_path: str):
    """Process raw YouTube JSON data based on given specifications."""

    # Cek apakah file sumber ada
    if not os.path.exists(source_file_path):
        raise FileNotFoundError(f"Source file not found: {source_file_path}")

    if not os.path.exists(categories_file_path):
        raise FileNotFoundError(f"Categories file not found: {categories_file_path}")

    # Load JSON data
    with open(source_file_path, 'r') as f:
        videos_list = json.load(f)

    with open(categories_file_path, 'r') as f:
        categories = json.load(f)

    processed_videos = []

    # Proses data video
    for video in videos_list:
        processed_video = {}

        # Tambahkan field videoId, trendingAt, publishedAt, title, dan channelTitle
        processed_video['videoId'] = video.get('videoId')
        processed_video['trendingAt'] = video.get('trendingAt')
        processed_video['publishedAt'] = video.get('publishedAt')
        processed_video['title'] = video.get('title')
        processed_video['channelId'] = video.get('channelId')
        processed_video['channelTitle'] = video.get('channelTitle')

        # Konversi durasi ISO 8601 ke detik
        processed_video['durationSec'] = int(isodate.parse_duration(video['duration']).total_seconds()) if video.get('duration') else None

        # Konversi tags list ke string
        processed_video['tags'] = video['tags'] = ', '.join(video['tags']) if video['tags'] is not None else None
        
        # Mapping categoryId ke nama kategori
        processed_video['category'] = categories.get(video.get('categoryId'), "Unknown")

        # Parsing thumbnail URL
        processed_video['thumbnailUrl'] = video.get('thumbnails', {}).get('standard', {}).get('url')

        # Konversi viewCount, likeCount, dan commentCount ke integer, handle None or empty string
        for field in ['viewCount', 'likeCount', 'commentCount']:
            value = video.get(field, "0")  # Default to "0" if the field is missing or None
            processed_video[field] = int(value) if value else 0  # Convert to int only if value is not None/empty

        # Konversi caption ke boolean
        processed_video['caption'] = video.get('caption') == 'true'

        # Tambahkan hasil ke daftar
        processed_videos.append(processed_video)

    # Pastikan folder target tersedia sebelum menyimpan file
    os.makedirs(os.path.dirname(target_file_path), exist_ok=True)

    # Simpan hasil pemrosesan
    with open(target_file_path, "w") as f:
        json.dump(processed_videos, f, indent=4)

    print(f"Processed data saved to {target_file_path}")


# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "process_youtube_data",
    default_args=default_args,
    description="DAG to process YouTube trending videos data",
    schedule_interval="@daily",
    catchup=False,
)

# Define the task
process_data_task = PythonOperator(
    task_id="process_youtube_data",
    python_callable=process_youtube_data,
    op_kwargs={
        "source_file_path": "/opt/airflow/data/trending_videos.json",
        "target_file_path": "/opt/airflow/data/processed_youtube_data.json",
        "categories_file_path": "/opt/airflow/data/youtube_categories.json"
    },
    dag=dag,
)

process_data_task
