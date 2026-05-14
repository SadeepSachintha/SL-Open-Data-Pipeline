import os
import boto3
from botocore.client import Config
from pymongo import MongoClient

# MinIO Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")

def get_s3_client():
    """Returns an S3 client configured for MinIO."""
    return boto3.client(
        's3',
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version='s3v4'),
        region_name='us-east-1'
    )

def create_bucket_if_not_exists(bucket_name):
    """Creates a bucket in MinIO if it doesn't already exist."""
    s3 = get_s3_client()
    try:
        s3.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' already exists.")
    except:
        s3.create_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' created.")

def upload_to_s3(file_path, bucket_name, s3_key):
    """Uploads a file to MinIO/S3."""
    s3 = get_s3_client()
    s3.upload_file(file_path, bucket_name, s3_key)
    print(f"File {file_path} uploaded to {bucket_name}/{s3_key}")

def get_mongo_client():
    """Returns a MongoDB client."""
    mongo_uri = os.getenv("MONGO_URI", "mongodb://admin:password@mongodb:27017")
    return MongoClient(mongo_uri)

if __name__ == "__main__":
    # Initialize buckets
    create_bucket_if_not_exists("sl-data-lake-raw")
    create_bucket_if_not_exists("sl-data-lake-processed")
