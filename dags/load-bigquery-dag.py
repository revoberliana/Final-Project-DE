import os
import logging
import pandas as pd
from google.cloud import bigquery

DATA_FOLDER = "data"  # Folder tempat CSV disimpan
PROJECT_ID = "dibimbing-442703"
DATASET_ID = "dibimbing"

def load_all_csv_to_bigquery():
    """ Loop semua file di folder data/ lalu upload ke BigQuery """
    client = bigquery.Client()
    
    for file_name in os.listdir(DATA_FOLDER):
        if file_name.endswith(".csv"):
            file_path = os.path.join(DATA_FOLDER, file_name)
            table_name = file_name.replace(".csv", "").lower()
            table_id = f"{PROJECT_ID}.{DATASET_ID}.{table_name}"

            logging.info(f"📤 Mengupload {file_name} ke {table_id}...")

            try:
                df = pd.read_csv(file_path)

                if df.empty:
                    logging.warning(f"⚠️ File {file_name} kosong. Lewati upload.")
                    continue

                job_config = bigquery.LoadJobConfig(
                    schema=[
                        bigquery.SchemaField("video_id", "STRING"),
                        bigquery.SchemaField("title", "STRING"),
                        bigquery.SchemaField("published_at", "TIMESTAMP"),
                        bigquery.SchemaField("channel_title", "STRING"),
                    ],
                    source_format=bigquery.SourceFormat.CSV,
                    skip_leading_rows=1,
                    write_disposition="WRITE_APPEND"
                )

                job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
                job.result()

                logging.info(f"✅ {len(df)} baris dari {file_name} berhasil diunggah ke {table_id}")

            except Exception as e:
                logging.error(f"❌ Gagal upload {file_name}: {e}")

# Panggil fungsi untuk upload semua file
load_all_csv_to_bigquery()
