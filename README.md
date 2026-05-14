# 📊 Sri Lankan Open Data ETL Pipeline (DataOps)

> **"Building a Scalable Data Pipeline for Sri Lankan Open Data Using DataOps Principles"**

This project demonstrates a production-grade ETL (Extract, Transform, Load) architecture designed to ingest, process, and analyze open data from Sri Lanka. It implements a **Hybrid Data Lakehouse** pattern using industry-standard tools.

---

## 🌟 Key Features

*   **Automated Orchestration**: Managed by **Apache Airflow** with custom DAGs for multi-source ingestion.
*   **S3-Compatible Data Lake**: Uses **MinIO** to implement a Bronze/Silver/Gold data architecture locally.
*   **Multi-Model Storage**: 
    - **PostgreSQL (SQL)** for highly structured Market Prices.
    - **MongoDB (NoSQL)** for flexible, high-frequency Weather data.
*   **DataOps Integrity**: Integrated validation layer using custom logic to ensure data quality and schema enforcement.
*   **Containerized Portability**: Entire stack is deployable via a single `docker compose` command.

---

## 🏗️ Architecture & Data Flow

1.  **Extract**: Python operators fetch data from **data.gov.lk**, **Open-Meteo**, and **HDX**.
2.  **Raw (Bronze)**: Data is archived as immutable JSON/CSV files in the `sl-data-lake-raw` bucket.
3.  **Transform (Silver)**: Pandas/PyArrow clean, normalize, and convert data into optimized **Parquet** formats.
4.  **Validate**: Data quality checks verify temperature ranges, price non-negativity, and field completeness.
5.  **Load (Gold)**: Cleaned data is upserted into PostgreSQL and MongoDB for dashboard consumption.

---

## 📁 Project Directory

```text
.
├── dags/                  # Airflow Workflow Definitions
│   ├── weather_etl.py     # Weather Ingestion (API -> MinIO -> Mongo)
│   └── market_prices_etl.py # Food Prices (CSV -> MinIO -> Postgres)
├── scripts/               # Core Logic & Utilities
│   ├── utils.py           # MinIO S3 & Database connection helpers
│   └── data_quality.py    # DataOps Validation Rules
├── docker-compose.yml     # Multi-container service definitions
├── requirements.txt       # Python dependency manifest
└── README.md              # Project Documentation
```

---

## 🛠️ Getting Started

### 1. Prerequisites
- Docker & Docker Compose (V2)
- 4GB+ RAM allocated to Docker

### 2. Launching the Stack
**Windows:**
Double-click `start.bat` or run:
```bash
./start.bat
```

**Ubuntu/Linux:**
```bash
chmod +x start.sh stop.sh
./start.sh
```

### 3. Service Dashboard
| Service | URL | Credentials |
| :--- | :--- | :--- |
| **Airflow UI** | [http://localhost:8081](http://localhost:8081) | `airflow` / `airflow` |
| **MinIO Console** | [http://localhost:9001](http://localhost:9001) | `minioadmin` / `minioadmin` |
| **MongoDB** | `localhost:27017` | `admin` / `password` |
| **PostgreSQL** | `localhost:5432` | `airflow` / `airflow` |

---

## 🎓 Portfolio Impact (For Students)
This project stands out by moving beyond simple "CSV analysis" and implementing **Enterprise Data Engineering** patterns:
- **Scalability**: Demonstrates handling of both API and flat-file sources.
- **Cloud-Readiness**: MinIO code is 100% compatible with **AWS S3 / Oracle Cloud**.
- **Data Governance**: Highlights the importance of Data Quality in automated systems.

---

## 📝 License
MIT License. Feel free to use this for your academic submissions or portfolio!
