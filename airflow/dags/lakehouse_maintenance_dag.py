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
    dag_id="lakehouse_catalog_maintenance",
    default_args=default_args,
    description="Synchronize Lakehouse Table Catalog, discover partitions, and audit ACID WAL commits",
    start_date=datetime(2026, 1, 1),
    schedule="*/30 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["lakehouse", "iceberg", "delta", "catalog", "wal", "week-2"],
) as dag:

    init_catalog = BashOperator(
        task_id="init_catalog",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/lakehouse_catalog.py --init"
        ),
    )

    discover_partitions = BashOperator(
        task_id="discover_partitions",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/lakehouse_catalog.py --partitions"
        ),
    )

    export_trino_ddl = BashOperator(
        task_id="export_trino_ddl",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/lakehouse_catalog.py --export-trino"
        ),
    )

    audit_table_history = BashOperator(
        task_id="audit_table_history",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/lakehouse_catalog.py --history movie_lakehouse.staging_events"
        ),
    )

    init_catalog >> discover_partitions >> export_trino_ddl >> audit_table_history
