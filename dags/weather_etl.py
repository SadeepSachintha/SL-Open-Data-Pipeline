from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
import requests
import json
import pandas as pd
import os
import sys

# Add scripts directory to path
sys.path.append('/opt/airflow/scripts')
from utils import upload_to_s3, create_bucket_if_not_exists, get_mongo_client
from data_quality import validate_weather_data

default_args = {
    'owner': 'antigravity',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

RAW_BUCKET = "sl-data-lake-raw"
PROCESSED_BUCKET = "sl-data-lake-processed"

def extract_weather():
    """Extract weather data from Open-Meteo API for Colombo."""
    url = "https://api.open-meteo.com/v1/forecast?latitude=6.9271&longitude=79.8612&current_weather=true&hourly=temperature_2m,relativehumidity_2m,windspeed_10m"
    response = requests.get(url)
    data = response.json()
    
    # Save to local temporary file
    temp_file = "/tmp/weather_raw.json"
    with open(temp_file, 'w') as f:
        json.dump(data, f)
    
    # Upload to S3
    create_bucket_if_not_exists(RAW_BUCKET)
    s3_key = f"weather/{datetime.now().strftime('%Y-%m-%d_%H-%M')}/weather.json"
    upload_to_s3(temp_file, RAW_BUCKET, s3_key)
    
    return s3_key

def transform_weather(ti):
    """Transform raw weather data into a structured format."""
    s3_key = ti.xcom_pull(task_ids='extract_weather_task')
    
    import boto3
    from botocore.client import Config
    
    s3 = boto3.client(
        's3',
        endpoint_url="http://minio:9000",
        aws_access_key_id="minioadmin",
        aws_secret_access_key="minioadmin",
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )
    
    response = s3.get_object(Bucket=RAW_BUCKET, Key=s3_key)
    data = json.loads(response['Body'].read().decode('utf-8'))
    
    # Extract current weather
    current = data['current_weather']
    df = pd.DataFrame([current])
    df['timestamp'] = datetime.now()
    df['city'] = 'Colombo'
    
    # Save processed data to local Parquet
    processed_file = "/tmp/weather_processed.parquet"
    df.to_parquet(processed_file)
    
    # Upload to Processed S3
    create_bucket_if_not_exists(PROCESSED_BUCKET)
    processed_s3_key = s3_key.replace("weather.json", "weather.parquet")
    upload_to_s3(processed_file, PROCESSED_BUCKET, processed_s3_key)
    
    return processed_file

def load_to_mongodb(ti):
    """Load transformed data into MongoDB."""
    processed_file = ti.xcom_pull(task_ids='transform_weather_task')
    df = pd.read_parquet(processed_file)
    
    # Data Quality Check
    validate_weather_data(df)
    
    client = get_mongo_client()
    db = client['sl_open_data']
    collection = db['weather_colombo']
    
    # Convert dataframe to dictionary records
    records = df.to_dict('records')
    collection.insert_many(records)
    print(f"Loaded {len(records)} records to MongoDB.")

with DAG(
    'weather_etl_dag',
    default_args=default_args,
    description='ETL pipeline for Sri Lankan Weather Data',
    schedule_interval=timedelta(hours=1),
    catchup=False
) as dag:

    extract_task = PythonOperator(
        task_id='extract_weather_task',
        python_callable=extract_weather,
    )

    transform_task = PythonOperator(
        task_id='transform_weather_task',
        python_callable=transform_weather,
    )

    load_mongodb_task = PythonOperator(
        task_id='load_to_mongodb_task',
        python_callable=load_to_mongodb,
    )

    extract_task >> transform_task >> load_mongodb_task
