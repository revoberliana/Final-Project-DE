from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os
from googleapiclient.discovery import build
import json

def fetch_trending_videos():
    """Fetches trending videos from YouTube regions in Asia."""

    # Load environment variables
    load_dotenv()
    api_key = os.getenv("YOUTUBE_API_KEY")
    youtube = build("youtube", "v3", developerKey=api_key)
    
    region_codes = ["ID"]
    
    all_videos = []
    for region_code in region_codes:
        print(f"Fetching trending videos for region: {region_code}")
        
        try:
            videos_list = []
            next_page_token = ""
            while len(videos_list) < 500 and next_page_token is not None:
                request = youtube.videos().list(
                    part="snippet,contentDetails,statistics",
                    chart="mostPopular",
                    regionCode=region_code,
                    maxResults=500,
                    pageToken=next_page_token,
                )
                response = request.execute()
                
                videos = response.get("items", [])
                next_page_token = response.get("nextPageToken", None)
                
                infos = {
                    'snippet': ['title', 'publishedAt', 'channelId', 'channelTitle',
                                'description', 'tags', 'thumbnails', 'categoryId', 'defaultAudioLanguage'],
                    'contentDetails': ['duration', 'caption'],
                    'statistics': ['viewCount', 'likeCount', 'commentCount']
                }
                
                now = datetime.utcnow().strftime('%Y-%m-%dT%H:%M:%SZ')
                
                for video in videos:
                    video_details = {
                        'videoId': video["id"],
                        'regionCode': region_code,
                        'trendingAt': now
                    }
                    for k in infos.keys():
                        for info in infos[k]:
                            try:
                                video_details[info] = video[k][info]
                            except KeyError:
                                video_details[info] = None
                    videos_list.append(video_details)
            
            all_videos.extend(videos_list)
            print(f"Fetched {len(videos_list)} videos for {region_code}")
        
        except Exception as e:
            print(f"Error fetching videos for region {region_code}: {e}")
    
    output_file = "/opt/airflow/data/trending_videos.json"
    with open(output_file, "w", encoding="utf-8") as f:
        json.dump(all_videos, f, ensure_ascii=False, indent=4)
    print(f"Total videos fetched: {len(all_videos)}")

     
# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

dag = DAG(
    "fetch_youtube_trending_videos",
    default_args=default_args,
    description="DAG to fetch trending YouTube videos for regions in Asia",
    schedule_interval="@daily",
    catchup=False,
)

fetch_videos_task = PythonOperator(
    task_id="fetch_youtube_videos",
    python_callable=fetch_trending_videos,
    dag=dag,
)

fetch_videos_task