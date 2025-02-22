from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2024, 2, 22),
    'retries': 1,
}

with DAG(
    dag_id='dbt_run_dag',
    default_args=default_args,
    schedule_interval='@daily',
    catchup=False,
) as dag:

    dbt_staging = BashOperator(
        task_id='dbt_staging',
        bash_command='cd /opt/airflow/dbt && dbt run --select staging',
        do_xcom_push=True
    )

    dbt_mart = BashOperator(
        task_id='dbt_mart',
        bash_command='cd /opt/airflow/dbt && dbt run --select marts',
        do_xcom_push=True
    )

    dbt_staging >> dbt_mart  
