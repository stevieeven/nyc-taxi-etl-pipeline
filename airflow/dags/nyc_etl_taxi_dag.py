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