from pathlib import Path
from uuid import uuid4
from io import StringIO
import os

import pandas as pd
import psycopg2
from dotenv import load_dotenv


def get_pg_config() -> dict:
    load_dotenv()
    return {
        "host": os.getenv("POSTGRES_HOST", "localhost"),
        "port": int(os.getenv("POSTGRES_PORT", "5432")),
        "dbname": os.getenv("POSTGRES_DB", "nyc_taxi"),
        "user": os.getenv("POSTGRES_USER", "postgres"),
        "password": os.getenv("POSTGRES_PASSWORD", "postgres"),
    }


def load_parquet_to_raw(file_path: str) -> None:
    path = Path(file_path)
    print(f"Using file path: {path.resolve()}")
    print(f"File exists: {path.exists()}")

    if not path.exists():
        raise FileNotFoundError(f"Missing file: {file_path}")

    df = pd.read_parquet(path)
    print(f"DataFrame row count before load: {len(df)}")
    print(f"Columns from parquet: {list(df.columns)}")

    df = df.rename(
        columns={
            "VendorID": "vendorid",
            "RatecodeID": "ratecodeid",
            "PULocationID": "pulocationid",
            "DOLocationID": "dolocationid",
            "Airport_fee": "airport_fee",
        }
    )

    expected_columns = [
        "vendorid",
        "tpep_pickup_datetime",
        "tpep_dropoff_datetime",
        "passenger_count",
        "trip_distance",
        "ratecodeid",
        "store_and_fwd_flag",
        "pulocationid",
        "dolocationid",
        "payment_type",
        "fare_amount",
        "extra",
        "mta_tax",
        "tip_amount",
        "tolls_amount",
        "improvement_surcharge",
        "total_amount",
        "congestion_surcharge",
        "airport_fee",
    ]

    df = df[expected_columns].copy()
    df["source_file"] = path.name
    df["load_batch_id"] = str(uuid4())
    df["ingested_at"] = pd.Timestamp.utcnow()

    print(f"DataFrame row count after metadata columns: {len(df)}")

    csv_buffer = StringIO()
    df.to_csv(csv_buffer, index=False, header=False)
    csv_buffer.seek(0)

    batch_id = df["load_batch_id"].iloc[0]
    year_month = path.stem.replace("yellow_tripdata_", "")

    conn = psycopg2.connect(**get_pg_config())
    try:
        with conn:
            with conn.cursor() as cur:
                cur.copy_expert(
                    """
                    COPY raw.yellow_taxi_trips (
                        vendorid,
                        tpep_pickup_datetime,
                        tpep_dropoff_datetime,
                        passenger_count,
                        trip_distance,
                        ratecodeid,
                        store_and_fwd_flag,
                        pulocationid,
                        dolocationid,
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
                        ingested_at
                    )
                    FROM STDIN WITH (FORMAT CSV)
                    """,
                    csv_buffer,
                )

                cur.execute(
                    """
                    INSERT INTO raw.ingestion_log (
                        batch_id, dataset_name, source_file, year_month, row_count, status
                    ) VALUES (%s, %s, %s, %s, %s, %s)
                    """,
                    (
                        batch_id,
                        "yellow_taxi_trips",
                        path.name,
                        year_month,
                        len(df),
                        "SUCCESS",
                    ),
                )

        print("Load complete")
    finally:
        conn.close()


if __name__ == "__main__":
    load_parquet_to_raw("data/yellow_tripdata_2025-01.parquet")