# Import Python dependencies needed for the workflow
from urllib import request
from minio import Minio, S3Error
from airflow.utils.dates import days_ago
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
import pendulum
import os
import urllib.error


def download_parquet(**kwargs):
    # folder_path: str = r'..\..\data\raw'
    # Construct the relative path to the folder
    url: str = "https://d37ci6vzurychx.cloudfront.net/trip-data/"
    filename: str = "yellow_tripdata"
    extension: str = ".parquet"

    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    
    file_name = f"{filename}_{month}{extension}"
    destination_path = os.path.join(".", file_name)
    download_url = f"{url}{file_name}"
    
    print(f"Downloading {download_url} to {destination_path}")
    
    try:
        request.urlretrieve(download_url, destination_path)
    except urllib.error.URLError as e:
        raise RuntimeError(f"Failed to download the parquet file : {str(e)}") from e


# Python Function
def upload_file(**kwargs):
    ###############################################
    # Upload generated file to Minio

    client = Minio(
        "minio:9000",
        secure=False,
        access_key="admin",
        secret_key="password123"
    )
    bucket: str = 'nybuck'

    month: str = pendulum.now().subtract(months=2).format('YYYY-MM')
    print(client.list_buckets())
    
    # Ensure bucket exists
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    file_name = f"yellow_tripdata_{month}.parquet"
    local_path = os.path.join(".", file_name)

    client.fput_object(
        bucket_name=bucket,
        object_name=file_name,
        file_path=local_path
    )
    # suppression des fichiers recemments telechargés
    if os.path.exists(local_path):
        os.remove(local_path)


###############################################
with DAG(dag_id='grab_nyc_data_minio',
         start_date=days_ago(1),
         schedule_interval='0 0 1 * *',
         catchup=False,
         tags=['minio', 'read', 'write'],
         ) as dag:
    ###############################################
    # Create a task to call your processing function
    t1 = PythonOperator(
        task_id='download_parquet',
        provide_context=True,
        python_callable=download_parquet
    )
    t2 = PythonOperator(
        task_id='upload_file_task',
        provide_context=True,
        python_callable=upload_file
    )
###############################################  

###############################################
# first upload the file, then read the other file.
t1 >> t2
###############################################
