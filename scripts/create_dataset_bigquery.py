from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCreateEmptyTableOperator
from datetime import datetime, timedelta
from dotenv import load_dotenv
import os

# Load environment variables
load_dotenv()

# Retrieve GCP environment variables
gcp_project_id = os.getenv("GCP_PROJECT_ID")
gcp_dataset_id = os.getenv("GCP_DATASET_ID")

# Ensure project and dataset IDs are set
if not gcp_project_id or not gcp_dataset_id:
    raise ValueError("Missing GCP_PROJECT_ID or GCP_DATASET_ID in environment variables")

# Define the table schema in a compatible format
schema = [
    {"name": "videoId", "type": "STRING"},
    {"name": "trendingAt", "type": "TIMESTAMP"},
    {"name": "title", "type": "STRING"},
    {"name": "publishedAt", "type": "TIMESTAMP"},
    {"name": "channelId", "type": "STRING"},
    {"name": "channelTitle", "type": "STRING"},
    {"name": "description", "type": "STRING"},
    {"name": "tags", "type": "STRING"},
    {"name": "category", "type": "STRING"},
    {"name": "defaultAudioLanguage", "type": "STRING"},
    {"name": "durationSec", "type": "INTEGER"},
    {"name": "caption", "type": "BOOLEAN"},
    {"name": "viewCount", "type": "INTEGER"},
    {"name": "likeCount", "type": "INTEGER"},
    {"name": "commentCount", "type": "INTEGER"},
    {"name": "thumbnailUrl", "type": "STRING"}
]

# Default DAG arguments
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2025, 2, 18),
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "create_bigquery_tables",
    default_args=default_args,
    description="DAG to create BigQuery tables for trending videos",
    schedule_interval=None,
    catchup=False,
)

# Task to create the `trending_videos` table
create_table1 = BigQueryCreateEmptyTableOperator(
    task_id="create_trending_videos_table",
    project_id=gcp_project_id,
    dataset_id=gcp_dataset_id,
    table_id="trending_videos",
    schema_fields=schema,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

# Task to create the `trending_videos_test` table
create_table2 = BigQueryCreateEmptyTableOperator(
    task_id="create_trending_videos_test_table",
    project_id=gcp_project_id,
    dataset_id=gcp_dataset_id,
    table_id="trending_videos_test",
    schema_fields=schema,
    gcp_conn_id="google_cloud_default",
    dag=dag,
)

# Set task dependencies
create_table1 >> create_table2
