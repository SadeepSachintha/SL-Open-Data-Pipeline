@echo off
echo Starting Sri Lankan Open Data ETL Pipeline...

:: Check if Docker is running
docker --version >nul 2>&1
if %errorlevel% neq 0 (
    echo Error: Docker is not installed or not in PATH.
    pause
    exit /b
)

:: Start the containers
docker compose up -d

echo.
echo ======================================================
echo Pipeline is starting up!
echo ======================================================
echo Airflow: http://localhost:8080 (airflow/airflow)
echo MinIO:   http://localhost:9001 (minioadmin/minioadmin)
echo ======================================================
echo.
pause
