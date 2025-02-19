from airflow import DAG
from airflow.operators.python import PythonOperator
from google.oauth2 import service_account
from google.cloud import bigquery
from datetime import datetime, timedelta
import json
import os

def load_to_bigquery(source_file_path: str, table_name: str):
    """
    Loads the processed data to BigQuery.

    Args:
        source_file_path: A string representing the path to the file to be loaded.
        table_name: A string representing the name of the table to load the data to.
    """
    key_path = 'dags/service_account_key.json'
    credentials = service_account.Credentials.from_service_account_file(
        key_path,
        scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    
    client = bigquery.Client(credentials=credentials)
    dataset_id = "dibimbing"  # Sesuaikan dengan dataset yang digunakan
    table_ref = client.dataset(dataset_id).table(table_name)
    
    with open(source_file_path, 'r') as f:
        json_data = json.load(f)
    
    job_config = bigquery.LoadJobConfig(source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON)
    job = client.load_table_from_json(json_data, table_ref, job_config=job_config)
    job.result()
    
    print(f"Loaded {job.output_rows} rows to {table_name}")

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
    "load_json_to_bigquery",
    default_args=default_args,
    description="DAG to load JSON data into BigQuery",
    schedule_interval=None,  # Run on trigger
    catchup=False,
)

# Task to load JSON data to BigQuery
load_task = PythonOperator(
    task_id="load_to_bigquery",
    python_callable=load_to_bigquery,
    op_kwargs={
        "source_file_path": "tmp_file_processed.json",
        "table_name": "trending_videos_test",
    },
    dag=dag,
)

load_task
