from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from googleapiclient.discovery import build
import json

def fetch_youtube_categories():
    """Fetches YouTube video categories for Indonesia and saves to a JSON file."""
    
    # Load environment variables
    load_dotenv()
    api_key = os.environ.get("YOUTUBE_API_KEY")
    
    if not api_key:
        raise ValueError("API Key not found. Make sure YOUTUBE_API_KEY is set in .env file.")
    
    # Initialize YouTube API client
    youtube = build("youtube", "v3", developerKey=api_key)
    
    all_categories = {}
    
    try:
        request = youtube.videoCategories().list(
            part="snippet",
            regionCode="ID"  # Mengambil kategori untuk Indonesia
        )
        response = request.execute()

        # Extract and store categories
        categories = {item['id']: item['snippet']['title'] for item in response.get('items', [])}
        all_categories["ID"] = categories
        
        print(f"Fetched {len(categories)} categories for Indonesia.")
    
    except Exception as e:
        print(f"Error fetching categories: {e}")
    
    # Save all categories to a JSON file
    output_file = '/opt/airflow/data/youtube_categories.json'
    with open(output_file, 'w') as f:
        json.dump(all_categories, f)
        
# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fetch_youtube_categories",
    default_args=default_args,
    description="DAG to fetch YouTube categories",
    schedule_interval="@daily",  # Runs daily
    catchup=False,
)

# Define the task using PythonOperator
fetch_categories_task = PythonOperator(
    task_id="fetch_youtube_categories",
    python_callable=fetch_youtube_categories,
    dag=dag,
)

fetch_categories_task
