from sqlalchemy import create_engine, text
from ingestion.utils import get_postgres_uri


def run_basic_checks() -> None:
    engine = create_engine(get_postgres_uri())

    checks = {
        "raw_row_count": """
            select count(*) from raw.yellow_taxi_trips
        """,
        "null_pickup_ts": """
            select count(*) from raw.yellow_taxi_trips
            where tpep_pickup_datetime is null
        """,
        "null_dropoff_ts": """
            select count(*) from raw.yellow_taxi_trips
            where tpep_dropoff_datetime is null
        """,
        "negative_trip_distance": """
            select count(*) from raw.yellow_taxi_trips
            where trip_distance < 0
        """,
        "negative_total_amount": """
            select count(*) from raw.yellow_taxi_trips
            where total_amount < 0
        """,
        "pickup_after_dropoff": """
            select count(*) from raw.yellow_taxi_trips
            where tpep_pickup_datetime > tpep_dropoff_datetime
        """,
    }

    with engine.begin() as conn:
        results = {
            name: conn.execute(text(sql)).scalar()
            for name, sql in checks.items()
        }

    total_rows = results["raw_row_count"]

    if total_rows == 0:
        raise ValueError("raw.yellow_taxi_trips is empty")

    print("\n=== RAW DATA QUALITY SUMMARY ===")
    print(f"Total rows: {total_rows:,}")

    hard_failures = []
    warnings = []

    if results["null_pickup_ts"] > 0:
        hard_failures.append(
            f"Null pickup timestamps: {results['null_pickup_ts']:,}"
        )

    if results["null_dropoff_ts"] > 0:
        hard_failures.append(
            f"Null dropoff timestamps: {results['null_dropoff_ts']:,}"
        )

    if results["negative_trip_distance"] > 0:
        hard_failures.append(
            f"Negative trip distance rows: {results['negative_trip_distance']:,}"
        )

    negative_total_amount = results["negative_total_amount"]
    negative_total_pct = negative_total_amount / total_rows

    if negative_total_pct > 0.001:
        warnings.append(
            f"Negative total_amount rows: {negative_total_amount:,} ({negative_total_pct:.4%})"
        )
    elif negative_total_amount > 0:
        warnings.append(
            f"Negative total_amount rows: {negative_total_amount:,} ({negative_total_pct:.4%})"
        )

    bad_time_order = results["pickup_after_dropoff"]
    bad_time_order_pct = bad_time_order / total_rows

    if bad_time_order_pct > 0.001:
        warnings.append(
            f"Pickup after dropoff rows: {bad_time_order:,} ({bad_time_order_pct:.4%})"
        )
    elif bad_time_order > 0:
        warnings.append(
            f"Pickup after dropoff rows: {bad_time_order:,} ({bad_time_order_pct:.4%})"
        )

    print("\nChecks:")
    for key, value in results.items():
        if key == "raw_row_count":
            continue
        pct = value / total_rows if total_rows else 0
        print(f"- {key}: {value:,} ({pct:.4%})")

    if warnings:
        print("\nWarnings:")
        for warning in warnings:
            print(f"- {warning}")

    if hard_failures:
        print("\nFailures:")
        for failure in hard_failures:
            print(f"- {failure}")
        raise ValueError("Raw data quality checks failed")

    print("\nRaw data quality checks passed.")


if __name__ == "__main__":
    run_basic_checks()