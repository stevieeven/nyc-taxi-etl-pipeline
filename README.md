# nyc-taxi-cloud-etl starter

## Suggested repo tree

```text
nyc-taxi-cloud-etl/
├── README.md
├── requirements.txt
├── docker-compose.yml
├── .env.example
├── .gitignore
├── airflow/
│   └── dags/
│       └── nyc_taxi_etl_dag.py
├── ingestion/
│   ├── extract_tlc_data.py
│   ├── load_to_postgres.py
│   ├── quality_checks.py
│   └── utils.py
├── sql/
│   ├── create_schemas.sql
│   └── create_raw_tables.sql
├── dbt/
│   └── nyc_taxi_project/
│       ├── dbt_project.yml
│       ├── profiles.yml
│       └── models/
│           ├── staging/
│           │   ├── stg_yellow_taxi_trips.sql
│           │   └── staging.yml
│           └── analytics/
│               ├── mart_daily_revenue.sql
│               └── analytics.yml
└── tests/
    └── test_smoke.py
```

---

## README.md

```md
# nyc-taxi-cloud-etl

Open-source cloud ETL pipeline that ingests public NYC TLC taxi trip data into a cloud PostgreSQL warehouse using Airflow, dbt, and Docker.

## Architecture

NYC TLC dataset -> Python ingestion -> PostgreSQL raw schema -> dbt staging -> dbt analytics marts -> Airflow orchestration

## Tech stack

- Apache Airflow
- Python
- PostgreSQL
- dbt Core
- Docker Compose
- Terraform (planned)

## Schemas

- `raw`: landed source data
- `staging`: cleaned and standardized data
- `analytics`: reporting marts

## Local quick start

1. Copy `.env.example` to `.env`
2. Start Postgres with Docker Compose
3. Run schema SQL scripts
4. Run the ingestion script for one sample month
5. Run dbt models
6. Trigger the Airflow DAG

## Example use case

Load January 2025 yellow taxi trip data, standardize it, and publish daily revenue metrics.

## Future improvements

- Azure deployment with Terraform
- Blob Storage landing zone
- Great Expectations checks
- GitHub Actions CI/CD
- Metabase dashboard
```

---

## requirements.txt

```txt
pandas==2.2.3
pyarrow==18.1.0
requests==2.32.3
SQLAlchemy==2.0.36
psycopg2-binary==2.9.10
apache-airflow==2.10.4
apache-airflow-providers-postgres==5.13.0
dbt-core==1.8.8
dbt-postgres==1.8.2
pytest==8.3.4
python-dotenv==1.0.1
```

---

## .env.example

```bash
POSTGRES_HOST=localhost
POSTGRES_PORT=5432
POSTGRES_DB=nyc_taxi
POSTGRES_USER=postgres
POSTGRES_PASSWORD=postgres
TLC_DATA_URL=https://d37ci6vzurychx.cloudfront.net/trip-data/yellow_tripdata_2025-01.parquet
```

---

## docker-compose.yml

```yaml
version: '3.9'

services:
  postgres:
    image: postgres:16
    container_name: nyc_taxi_postgres
    environment:
      POSTGRES_DB: nyc_taxi
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: postgres
    ports:
      - "5432:5432"
    volumes:
      - postgres_data:/var/lib/postgresql/data

volumes:
  postgres_data:
```

---

## sql/create_schemas.sql

```sql
create schema if not exists raw;
create schema if not exists staging;
create schema if not exists analytics;
```

---

## sql/create_raw_tables.sql

```sql
create table if not exists raw.yellow_taxi_trips (
    vendorid integer,
    tpep_pickup_datetime timestamp,
    tpep_dropoff_datetime timestamp,
    passenger_count numeric,
    trip_distance numeric,
    ratecodeid numeric,
    store_and_fwd_flag text,
    pulocationid integer,
    dolocationid integer,
    payment_type integer,
    fare_amount numeric,
    extra numeric,
    mta_tax numeric,
    tip_amount numeric,
    tolls_amount numeric,
    improvement_surcharge numeric,
    total_amount numeric,
    congestion_surcharge numeric,
    airport_fee numeric,
    source_file text,
    load_batch_id text,
    ingested_at timestamp default current_timestamp
);

create table if not exists raw.ingestion_log (
    batch_id text primary key,
    dataset_name text,
    source_file text,
    year_month text,
    row_count integer,
    status text,
    loaded_at timestamp default current_timestamp
);
```

---

## ingestion/utils.py

```python
import os
from dotenv import load_dotenv


def load_env() -> None:
    load_dotenv()


def get_postgres_uri() -> str:
    load_env()
    host = os.getenv("POSTGRES_HOST", "localhost")
    port = os.getenv("POSTGRES_PORT", "5432")
    db = os.getenv("POSTGRES_DB", "nyc_taxi")
    user = os.getenv("POSTGRES_USER", "postgres")
    password = os.getenv("POSTGRES_PASSWORD", "postgres")
    return f"postgresql+psycopg2://{user}:{password}@{host}:{port}/{db}"
```

---

## ingestion/extract_tlc_data.py

```python
from pathlib import Path
import os
import requests
from dotenv import load_dotenv


def download_file() -> Path:
    load_dotenv()
    url = os.getenv("TLC_DATA_URL")
    if not url:
        raise ValueError("TLC_DATA_URL is not set")

    output_dir = Path("data")
    output_dir.mkdir(exist_ok=True)
    filename = url.split("/")[-1]
    output_path = output_dir / filename

    response = requests.get(url, timeout=120)
    response.raise_for_status()
    output_path.write_bytes(response.content)
    return output_path


if __name__ == "__main__":
    file_path = download_file()
    print(f"Downloaded to {file_path}")
```

---

## ingestion/load_to_postgres.py

```python
from pathlib import Path
from uuid import uuid4
import pandas as pd
from sqlalchemy import create_engine, text

from ingestion.utils import get_postgres_uri


def load_parquet_to_raw(file_path: str) -> None:
    path = Path(file_path)
    if not path.exists():
        raise FileNotFoundError(f"Missing file: {file_path}")

    df = pd.read_parquet(path)
    df["source_file"] = path.name
    df["load_batch_id"] = str(uuid4())
    df["ingested_at"] = pd.Timestamp.utcnow()

    engine = create_engine(get_postgres_uri())

    df.to_sql(
        "yellow_taxi_trips",
        engine,
        schema="raw",
        if_exists="append",
        index=False,
        method="multi",
        chunksize=10000,
    )

    batch_id = df["load_batch_id"].iloc[0]
    year_month = path.stem.replace("yellow_tripdata_", "")

    with engine.begin() as conn:
        conn.execute(
            text(
                """
                insert into raw.ingestion_log (
                    batch_id, dataset_name, source_file, year_month, row_count, status
                ) values (
                    :batch_id, :dataset_name, :source_file, :year_month, :row_count, :status
                )
                """
            ),
            {
                "batch_id": batch_id,
                "dataset_name": "yellow_taxi_trips",
                "source_file": path.name,
                "year_month": year_month,
                "row_count": len(df),
                "status": "SUCCESS",
            },
        )


if __name__ == "__main__":
    load_parquet_to_raw("data/yellow_tripdata_2025-01.parquet")
    print("Load complete")
```

---

## ingestion/quality_checks.py

```python
from sqlalchemy import create_engine, text
from ingestion.utils import get_postgres_uri


def run_basic_checks() -> None:
    engine = create_engine(get_postgres_uri())
    checks = {
        "raw_row_count": "select count(*) from raw.yellow_taxi_trips",
        "null_pickup_ts": "select count(*) from raw.yellow_taxi_trips where tpep_pickup_datetime is null",
        "negative_trip_distance": "select count(*) from raw.yellow_taxi_trips where trip_distance < 0",
        "pickup_after_dropoff": "select count(*) from raw.yellow_taxi_trips where tpep_pickup_datetime > tpep_dropoff_datetime",
    }

    with engine.begin() as conn:
        results = {name: conn.execute(text(sql)).scalar() for name, sql in checks.items()}

    if results["raw_row_count"] == 0:
        raise ValueError("raw.yellow_taxi_trips is empty")
    if results["null_pickup_ts"] > 0:
        raise ValueError("Found null pickup timestamps")
    if results["negative_trip_distance"] > 0:
        raise ValueError("Found negative trip distance values")
    if results["pickup_after_dropoff"] > 0:
        raise ValueError("Found pickup timestamps after dropoff timestamps")

    print("Quality checks passed:", results)


if __name__ == "__main__":
    run_basic_checks()
```

---

## airflow/dags/nyc_taxi_etl_dag.py

```python
from datetime import datetime
from airflow import DAG
from airflow.operators.bash import BashOperator

with DAG(
    dag_id="nyc_taxi_etl",
    start_date=datetime(2025, 1, 1),
    schedule="@monthly",
    catchup=False,
    tags=["portfolio", "etl", "nyc_taxi"],
) as dag:

    download = BashOperator(
        task_id="download_source_file",
        bash_command="python ingestion/extract_tlc_data.py",
    )

    load_raw = BashOperator(
        task_id="load_raw_table",
        bash_command="python ingestion/load_to_postgres.py",
    )

    quality = BashOperator(
        task_id="run_quality_checks",
        bash_command="python ingestion/quality_checks.py",
    )

    run_dbt_staging = BashOperator(
        task_id="run_dbt_staging",
        bash_command="cd dbt/nyc_taxi_project && dbt run --select staging",
    )

    run_dbt_analytics = BashOperator(
        task_id="run_dbt_analytics",
        bash_command="cd dbt/nyc_taxi_project && dbt run --select analytics",
    )

    download >> load_raw >> quality >> run_dbt_staging >> run_dbt_analytics
```

---

## dbt/nyc_taxi_project/dbt_project.yml

```yaml
name: nyc_taxi_project
version: '1.0.0'
profile: nyc_taxi_project
model-paths: ["models"]
clean-targets: ["target", "dbt_packages"]

models:
  nyc_taxi_project:
    staging:
      +schema: staging
      +materialized: view
    analytics:
      +schema: analytics
      +materialized: table
```

---

## dbt/nyc_taxi_project/profiles.yml

```yaml
nyc_taxi_project:
  target: dev
  outputs:
    dev:
      type: postgres
      host: localhost
      user: postgres
      password: postgres
      port: 5432
      dbname: nyc_taxi
      schema: public
      threads: 4
```

---

## dbt/nyc_taxi_project/models/staging/stg_yellow_taxi_trips.sql

```sql
with source as (
    select * from raw.yellow_taxi_trips
),

cleaned as (
    select
        vendorid,
        tpep_pickup_datetime as pickup_ts,
        tpep_dropoff_datetime as dropoff_ts,
        passenger_count,
        trip_distance,
        ratecodeid,
        store_and_fwd_flag,
        pulocationid as pickup_location_id,
        dolocationid as dropoff_location_id,
        payment_type,
        fare_amount,
        extra,
        mta_tax,
        tip_amount,
        tolls_amount,
        improvement_surcharge,
        total_amount,
        congestion_surcharge,
        airport_fee,
        source_file,
        load_batch_id,
        ingested_at,
        extract(epoch from (tpep_dropoff_datetime - tpep_pickup_datetime)) / 60.0 as trip_duration_minutes
    from source
    where tpep_pickup_datetime is not null
      and tpep_dropoff_datetime is not null
      and trip_distance >= 0
      and tpep_dropoff_datetime >= tpep_pickup_datetime
)

select * from cleaned
```

---

## dbt/nyc_taxi_project/models/staging/staging.yml

```yaml
version: 2

models:
  - name: stg_yellow_taxi_trips
    columns:
      - name: pickup_ts
        tests:
          - not_null
      - name: dropoff_ts
        tests:
          - not_null
```

---

## dbt/nyc_taxi_project/models/analytics/mart_daily_revenue.sql

```sql
select
    cast(pickup_ts as date) as trip_date,
    count(*) as trip_count,
    sum(total_amount) as total_revenue,
    avg(fare_amount) as avg_fare_amount,
    avg(trip_distance) as avg_trip_distance,
    sum(tip_amount) as total_tip_amount
from staging.stg_yellow_taxi_trips
group by 1
order by 1
```

---

## dbt/nyc_taxi_project/models/analytics/analytics.yml

```yaml
version: 2

models:
  - name: mart_daily_revenue
    columns:
      - name: trip_date
        tests:
          - unique
          - not_null
```

---

## tests/test_smoke.py

```python
def test_smoke() -> None:
    assert 1 == 1
```

---

## Suggested next improvements

1. Parameterize year and month in the DAG instead of hardcoding a single file.
2. Add `if_exists=append` protections plus duplicate batch detection.
3. Run `dbt test` in Airflow after `dbt run`.
4. Move raw files to cloud storage instead of local `data/`.
5. Add Terraform for Azure PostgreSQL, Blob Storage, and Airflow hosting.
6. Add a dashboard layer such as Metabase.
