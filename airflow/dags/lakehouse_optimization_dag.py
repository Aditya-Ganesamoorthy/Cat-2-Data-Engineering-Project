"""
Airflow DAG: Lakehouse Table Optimization, Compaction & Safe Vacuum (Week 4).
Orchestrates automated Delta Lake table maintenance:
1. Catalog & physical partition synchronization
2. File compaction (OPTIMIZE / bin-packing small streaming Parquet files)
3. Safe snapshot retention cleanup (VACUUM with 168-hour retention window)
4. Trino serving layer catalog synchronization & query healthcheck
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator


PROJECT_DIR = "/opt/airflow/project"

default_args = {
    "owner": "data-engineering",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="lakehouse_delta_optimization",
    default_args=default_args,
    description="Automated Delta Lake compaction, safe vacuum, and Trino catalog synchronization",
    start_date=datetime(2026, 1, 1),
    schedule="0 2 * * *",  # Daily at 2:00 AM UTC
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "delta", "optimize", "vacuum", "trino", "week-4"],
) as dag:

    sync_partitions = BashOperator(
        task_id="sync_partitions",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/lakehouse_catalog.py --partitions"
        ),
    )

    compact_staging = BashOperator(
        task_id="compact_staging_events",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/lakehouse_optimizer.py --table staging"
        ),
    )

    compact_quarantine = BashOperator(
        task_id="compact_quarantine_events",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/lakehouse_optimizer.py --table quarantine"
        ),
    )

    safe_vacuum = BashOperator(
        task_id="safe_vacuum_cleanup",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/lakehouse_optimizer.py --table all --retention-hours 168"
        ),
    )

    verify_trino_serving = BashOperator(
        task_id="verify_trino_serving",
        env={
            "TRINO_HOST": "trino",
            "TRINO_PORT": "8080",
            "TRINO_USER": "airflow",
            "TRINO_CATALOG": "delta",
            "TRINO_SCHEMA": "movie_lakehouse",
        },
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/trino_client.py"
        ),
    )

    # Workflow dependencies
    sync_partitions >> [compact_staging, compact_quarantine] >> safe_vacuum >> verify_trino_serving
