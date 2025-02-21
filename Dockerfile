FROM apache/airflow:2.7.3

# Install dbt hanya untuk BigQuery
RUN pip install dbt-bigquery

# Pastikan folder dbt tersedia
RUN mkdir -p /home/airflow/dbt
WORKDIR /home/airflow/dbt

# Install git (opsional, jika perlu)
USER root
RUN apt update && apt install -y git
USER airflow
