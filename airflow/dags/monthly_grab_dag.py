from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from minio import Minio
import urllib.request
import os

def grab_last_month_data(**context):
    """
    Downloads the NYC Taxi data for the month corresponding to the execution_date (logical date).
    If the DAG runs on Feb 1st, the execution_date is Jan 1st, so it downloads Jan data.
    """

    pass

    execution_date = context['execution_date']
    year = execution_date.year
    month = execution_date.month
    
    
    month_str = f"{year}-{month:02d}"
    
    if year >= 2024: 
        print(f"WARN: Date {month_str} is in the future or too recent. Defaulting to 2023-01 for demo purposes.")
        month_str = "2023-01"
    
    file_name = f"yellow_tripdata_{month_str}.parquet"
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    url = f"{base_url}/{file_name}"
    
    # Validations / Directories
    local_dir = "/tmp"
    local_path = os.path.join(local_dir, file_name)
    
    print(f"Looking for data for: {month_str}")
    print(f"Downloading {file_name} from {url}...")
    
    try:
        urllib.request.urlretrieve(url, local_path)
        print(f"Downloaded to: {local_path}")
    except Exception as e:
        print(f"Failed to download {url}: {e}")
        raise e

    # Minio Configuration (Internal Docker Service)
    minio_client = Minio(
        "minio:9000",
        secure=False,
        access_key="admin",
        secret_key="password123"
    )
    bucket_name = "nybuck"
    
    # Check bucket
    if not minio_client.bucket_exists(bucket_name):
        print(f"Bucket {bucket_name} does not exist. Creating it...")
        minio_client.make_bucket(bucket_name)
    
    # Upload
    print(f"Uploading {file_name} to Minio bucket {bucket_name}...")
    try:
        minio_client.fput_object(bucket_name, file_name, local_path)
        print("Upload successful.")
    except Exception as e:
        print(f"Failed to upload to Minio: {e}")
        raise e
    finally:
        # Cleanup local file to save space
        if os.path.exists(local_path):
            os.remove(local_path)
            print("Local file cleaned up.")

# DAG Definition
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'monthly_nyc_data_grab',
    default_args=default_args,
    description='Downloads NYC Taxi data for the previous month on the 1st of every month',
    schedule_interval='0 0 1 * *',  # Run at 00:00 on day 1 of every month
    start_date=datetime(2023, 1, 1),
    catchup=False, # Do not run for past dates automatically
    tags=['datamart', 'nyc-taxi'],
) as dag:

    grab_parquet_task = PythonOperator(
        task_id='grab_last_month_parquet',
        python_callable=grab_last_month_data,
        provide_context=True,
    )
