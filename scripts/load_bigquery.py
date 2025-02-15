import os
import logging
import pandas as pd
from google.cloud import bigquery

def load_to_bigquery(file_path, table_name):
    """ Mengupload data CSV ke BigQuery """
    client = bigquery.Client()
    table_id = f"dibimbing-442703.dibimbing.{table_name}"

    if not os.path.exists(file_path):
        logging.warning(f"⚠️ File {file_path} tidak ditemukan. Skip upload ke BigQuery.")
        return

    try:
        df = pd.read_csv(file_path)

        if df.empty:
            logging.warning(f"⚠️ File {file_path} kosong. Tidak ada data yang di-upload.")
            return

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

        logging.info(f"✅ Loaded {len(df)} rows to {table_id}")

    except Exception as e:
        logging.error(f"❌ Gagal mengupload data ke BigQuery: {e}")
