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
    dag_id="movie_data_pipeline",
    default_args=default_args,
    description="Incremental TMDB movie data pipeline using Kafka, SQLite and EDA",
    start_date=datetime(2026, 1, 1),
    schedule="0 * * * *",
    catchup=False,
    max_active_runs=1,
    tags=["tmdb", "kafka", "sqlite", "etl"],
) as dag:

    incremental_extract = BashOperator(
        task_id="incremental_extract",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/incremental_extract.py"
        ),
    )

    stage_movies = BashOperator(
        task_id="stage_movies",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/staging.py"
        ),
    )

    validate_movies = BashOperator(
        task_id="validate_movies",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/validation.py"
        ),
    )

    transform_movies = BashOperator(
        task_id="transform_movies",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python pipeline/transform.py"
        ),
    )

    publish_to_kafka = BashOperator(
        task_id="publish_to_kafka",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "KAFKA_BOOTSTRAP_SERVERS=kafka:9092 "
            "python pipeline/producer.py"
        ),
    )

    consume_from_kafka = BashOperator(
        task_id="consume_from_kafka",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "KAFKA_BOOTSTRAP_SERVERS=kafka:9092 "
            "python pipeline/consumer.py"
        ),
        execution_timeout=timedelta(minutes=5),
    )

    load_to_sqlite = BashOperator(
        task_id="load_to_sqlite",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python database/movie_database.py"
        ),
    )

    run_eda = BashOperator(
        task_id="run_eda",
        bash_command=(
            f"cd {PROJECT_DIR} && "
            "python analytics/eda.py"
        ),
    )

    (
        incremental_extract
        >> stage_movies
        >> validate_movies
        >> transform_movies
        >> publish_to_kafka
        >> consume_from_kafka
        >> load_to_sqlite
        >> run_eda
    )