# **Youtube Trending Analysis**

📌 Deskripsi Proyek

Proyek ini membangun arsitektur ELT (Extract, Load, Transform) yang mengambil data dari YouTube API setiap hari, menyimpannya di Google BigQuery sebagai Data Warehouse (DWH), dan menggunakan dbt untuk transformasi data sebelum divisualisasikan di Google Data Studio.


![image](https://github.com/user-attachments/assets/6f1429f4-e1d3-4e84-bc9b-93d154356507)


🚀 Data Platform

Apache Airflow → Untuk orkestrasi workflow
Docker → Untuk menjalankan Airflow dalam lingkungan yang terisolasi
YouTube API → Untuk mengambil data video trending setiap hari
Google BigQuery → Sebagai Data Warehouse
dbt (Data Build Tool) → Untuk transformasi data
Google Data Studio → Untuk visualisasi data

📂 Arsitektur Proyek

Extract: Airflow mengeksekusi script Python yang mengambil data dari YouTube API(most popular).
Load: Data mentah disimpan ke Google BigQuery.
Transform: Menggunakan dbt membuat 2 fact table , video performance dan channel performance.
Visualisasi: Data yang sudah diproses dianalisis menggunakan Google Data Studio.

📜 Struktur Direktori

Final-Project-DE/

│── dags/                  # DAG untuk Airflow  
│── youtube_trending/      # Model dbt untuk transformasi data  
│── scripts/               # Script Python untuk ekstraksi data  
│── data/                  # Data mentah sebelum masuk ke BigQuery  
│── logs/                  # Log Airflow  
│── config/                # Konfigurasi tambahan  
│── .env                   # Variabel lingkungan (API Key, dll)  
│── docker-compose.yml      # Konfigurasi Docker
│── Dockefile
│── README.md              # Dokumentasi proyek  


📊 Workflow Airflow

Task 1: extract_youtube_data → Mengambil data dari YouTube API
Task 2: process data dari tmp dan cleaning data
Task 3: load_to_bigquery → Menyimpan data ke Google BigQuery
Task 4: run_dbt_staging → Menjalankan transformasi awal dengan dbt
Task 5: run_dbt_mart → Menjalankan model akhir dbt untuk analisis

📈 Visualisasi Data

link dashboard : https://lookerstudio.google.com/reporting/9cf20d1d-85a1-46ee-a039-65afe1468da4


About the Author 👩‍💻

Revo Berliana | 📧 Email: berlianarevo@Gmail.com | 🌐 LinkedIn: www.linkedin.com/in/revo-berliana-92232515a 
