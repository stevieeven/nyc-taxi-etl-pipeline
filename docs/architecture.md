# Architecture Overview

## 📌 Summary

This project implements an end-to-end ETL pipeline using NYC Taxi data. The pipeline extracts raw data, loads it into a PostgreSQL data warehouse, transforms it using dbt, and validates data quality using automated checks.

---

## 🔄 Pipeline Flow

```
NYC TLC Data (Parquet)
        ↓
Python Ingestion Layer
        ↓
PostgreSQL (raw schema)
        ↓
dbt Staging Models (cleaned views)
        ↓
dbt Analytics Models (aggregated tables)
        ↓
Data Quality Tests
```

---

## 🧱 Components

### 1. Ingestion Layer (Python)

* Extracts NYC Taxi data from public source
* Loads Parquet data into PostgreSQL
* Performs initial data validation checks

### 2. Data Warehouse (PostgreSQL)

* Stores raw ingested data
* Serves as source for transformations

### 3. Transformation Layer (dbt)

#### Staging Layer

* Cleans and standardizes raw data
* Renames columns
* Applies basic transformations

#### Analytics Layer

* Aggregates business-level metrics
* Example:

    * daily revenue
    * trip counts
    * average fare

### 4. Data Quality Layer

* Enforces non-null constraints
* Validates timestamp consistency
* Ensures reliability of transformed data

---

## ⚙️ Orchestration (Planned)

The pipeline is currently executed manually via scripts. Future enhancements include:

* Airflow DAG for scheduling and orchestration
* Automated execution of ingestion + dbt workflows

---

## 🐳 Infrastructure

* Docker used to containerize PostgreSQL
* Local development environment ensures reproducibility

---

## 🚀 Future Improvements

* Add Airflow orchestration
* Implement incremental dbt models
* Deploy to cloud (Azure or GCP)
* Add BI dashboard layer (Metabase or Superset)
