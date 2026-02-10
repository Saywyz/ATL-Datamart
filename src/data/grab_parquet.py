from minio import Minio
import urllib.request
import pandas as pd
import sys
import os

def main():
    grab_data()
    write_data_minio()
    

def grab_data() -> None:
    """Grab the data from New York Yellow Taxi

    This method download x files of the New York Yellow Taxi. 
    
    Files need to be saved into "../../data/raw" folder
    This methods takes no arguments and returns nothing.
    """
    base_url = "https://d37ci6vzurychx.cloudfront.net/trip-data"
    
    # Download data from January 2023 to August 2023
    months = [f"2023-{month:02d}" for month in range(1, 9)]
    
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the path to the data/raw directory
    raw_data_dir = os.path.join(script_dir, '../../data/raw')
    
    # Ensure the directory exists
    os.makedirs(raw_data_dir, exist_ok=True)
    
    for month in months:
        file_name = f"yellow_tripdata_{month}.parquet"
        url = f"{base_url}/{file_name}"
        destination = os.path.join(raw_data_dir, file_name)
        
        print(f"Downloading {file_name}...")
        try:
            urllib.request.urlretrieve(url, destination)
            print(f"Downloaded: {destination}")
        except Exception as e:
            print(f"Error downloading {file_name}: {e}")



def write_data_minio():
    """
    This method put all Parquet files into Minio
    Ne pas faire cette méthode pour le moment
    """
    client = Minio(
        "localhost:9000",
        secure=False,
        access_key="admin",
        secret_key="password123"
    )
    bucket: str = "nybuck"
    found = client.bucket_exists(bucket)
    if not found:
        client.make_bucket(bucket)
    else:
        print("Bucket " + bucket + " existe déjà")
        
    # Get the directory of the current script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    # Construct the path to the data/raw directory
    raw_data_dir = os.path.join(script_dir, '../../data/raw')

    if os.path.exists(raw_data_dir):
        for file_name in os.listdir(raw_data_dir):
            if file_name.endswith('.parquet'):
                file_path = os.path.join(raw_data_dir, file_name)
                print(f"Uploading {file_name} to Minio bucket {bucket}...")
                try:
                    client.fput_object(bucket, file_name, file_path)
                    print(f"Uploaded {file_name}")
                except Exception as e:
                    print(f"Error uploading {file_name}: {e}")
    else:
        print(f"Directory {raw_data_dir} does not exist.")

if __name__ == '__main__':
    sys.exit(main())
