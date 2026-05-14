#!/bin/bash
echo "Starting Sri Lankan Open Data ETL Pipeline..."

# Check if Docker is running
if ! command -v docker &> /dev/null
then
    echo "Error: Docker is not installed."
    exit
fi

# Start the containers
docker compose up -d

echo ""
echo "======================================================"
echo "Pipeline is starting up!"
echo "======================================================"
echo "Airflow: http://localhost:8081 (airflow/airflow)"
echo "MinIO:   http://localhost:9001 (minioadmin/minioadmin)"
echo "======================================================"
echo ""
