from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.postgres.operators.postgres import PostgresOperator
from datetime import datetime, timedelta
import requests
import pandas as pd
import os
import sys
import io

# Add scripts directory to path
sys.path.append('/opt/airflow/scripts')
from utils import upload_to_s3, create_bucket_if_not_exists
from data_quality import validate_market_data

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
CSV_URL = "https://data.humdata.org/dataset/0689b70b-6007-4e7a-8f9f-098525b6824f/resource/3638f0d6-9969-48cf-a919-1d879d037ec6/download/wfp_food_prices_lka.csv"

def extract_market_prices():
    """Extract food price data from HDX/WFP."""
    response = requests.get(CSV_URL)
    content = response.content
    
    # Save to local temporary file
    temp_file = "/tmp/market_prices_raw.csv"
    with open(temp_file, 'wb') as f:
        f.write(content)
    
    # Upload to S3
    create_bucket_if_not_exists(RAW_BUCKET)
    s3_key = f"market_prices/{datetime.now().strftime('%Y-%m-%d')}/food_prices.csv"
    upload_to_s3(temp_file, RAW_BUCKET, s3_key)
    
    return s3_key

def transform_market_prices(ti):
    """Transform raw CSV data into a cleaned format for PostgreSQL."""
    s3_key = ti.xcom_pull(task_ids='extract_market_prices_task')
    
    # Simulate download from S3
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
    df = pd.read_csv(io.BytesIO(response['Body'].read()))
    
    # Basic Cleaning: Remove metadata row (usually the 2nd row in HDX CSVs)
    if df.iloc[0].str.contains('#').any():
        df = df.iloc[1:].reset_index(drop=True)
    
    # Select relevant columns
    cols = ['date', 'market', 'category', 'commodity', 'unit', 'price']
    df = df[cols]
    
    # Convert types
    df['date'] = pd.to_datetime(df['date'])
    df['price'] = pd.to_numeric(df['price'], errors='coerce')
    
    # Filter for Sri Lankan markets and essential items
    df = df.dropna(subset=['price'])
    
    # Save processed data
    processed_file = "/tmp/market_prices_processed.csv"
    df.to_csv(processed_file, index=False)
    
    # Upload to Processed S3
    create_bucket_if_not_exists(PROCESSED_BUCKET)
    processed_s3_key = s3_key.replace("food_prices.csv", "food_prices_cleaned.csv")
    upload_to_s3(processed_file, PROCESSED_BUCKET, processed_s3_key)
    
    return processed_file

def load_to_postgres(ti):
    """Load transformed CSV data into PostgreSQL."""
    processed_file = ti.xcom_pull(task_ids='transform_market_prices_task')
    df = pd.read_csv(processed_file)
    
    # Data Quality Check
    validate_market_data(df)
    
    from sqlalchemy import create_engine
    # Airflow postgres connection URI
    engine = create_engine('postgresql+psycopg2://airflow:airflow@postgres/airflow')
    
    df.to_sql('market_prices', engine, if_exists='replace', index=False)
    print(f"Loaded {len(df)} records to PostgreSQL.")

with DAG(
    'market_prices_etl_dag',
    default_args=default_args,
    description='ETL pipeline for Sri Lankan Market Prices',
    schedule_interval='@daily',
    catchup=False
) as dag:

    create_table = PostgresOperator(
        task_id='create_table_task',
        postgres_conn_id='postgres_default',
        sql="""
            CREATE TABLE IF NOT EXISTS market_prices (
                date TIMESTAMP,
                market VARCHAR(255),
                category VARCHAR(255),
                commodity VARCHAR(255),
                unit VARCHAR(50),
                price FLOAT
            );
        """
    )

    extract_task = PythonOperator(
        task_id='extract_market_prices_task',
        python_callable=extract_market_prices,
    )

    transform_task = PythonOperator(
        task_id='transform_market_prices_task',
        python_callable=transform_market_prices,
    )

    load_postgres_task = PythonOperator(
        task_id='load_to_postgres_task',
        python_callable=load_to_postgres,
    )

    create_table >> extract_task >> transform_task >> load_postgres_task
