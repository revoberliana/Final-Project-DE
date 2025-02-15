from airflow import DAG
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.utils.dates import days_ago

# Konfigurasi default DAG
default_args = {
    "owner": "airflow",
    "start_date": days_ago(1),
}

# Inisialisasi DAG
with DAG(
    "test_bigquery_connection",
    default_args=default_args,
    schedule_interval=None,  # Manual trigger
    catchup=False,
) as dag:

    check_bigquery = BigQueryCheckOperator(
        task_id="check_bigquery",
        sql="SELECT 1",
        use_legacy_sql=False,
        gcp_conn_id="google_cloud_default",
    )

    check_bigquery
