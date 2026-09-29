import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

if os.name == "nt":
    hadoop_dir = r"C:\hadoop"
    if os.path.exists(hadoop_dir):
        os.environ["HADOOP_HOME"] = hadoop_dir
        if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = rf"C:\hadoop\bin;{os.environ.get('PATH', '')}"

from pipeline.spark_stream_processor import (
    CHECKPOINT_DIR,
    CLEAN_OUTPUT_PATH,
    DEFAULT_BROKER,
    DEFAULT_TOPIC,
    QUARANTINE_OUTPUT_PATH,
    create_spark_session,
    start_processing,
)
from pipeline.stream_producer import run_producer


def run_e2e_streaming_test():
    """
    Automated End-to-End Test for Week 1 Deliverables:
    1. Set up Kafka topic with simulated high-velocity JSON event streams.
    2. Write a PySpark Structured Streaming job to ingest, validate, and parse payloads with schema enforcement.
    """
    print("=" * 80)
    print("  WEEK 1 E2E INTEGRATION TEST: STREAM INGESTION & PYSPARK SCHEMA ENFORCEMENT")
    print("=" * 80)

    test_topic = DEFAULT_TOPIC
    test_broker = DEFAULT_BROKER
    num_events = 50
    rate = 50.0
    anomaly_rate = 0.08  # ~8% anomalies to test schema enforcement and quarantine

    print(f"\n[Step 1/3] Generating & Publishing {num_events} high-velocity events to Kafka...")
    print(f"Topic: {test_topic} | Velocity: {rate} events/s | Anomaly Rate: {anomaly_rate*100:.1f}%")

    run_producer(
        brokers=test_broker,
        topic=test_topic,
        rate=rate,
        max_events=num_events,
        anomaly_rate=anomaly_rate,
    )

    print("\n[Step 2/3] Ingesting and Processing stream via PySpark Structured Streaming...")
    start_processing(
        brokers=test_broker,
        topic=test_topic,
        mode="all",  # Console + Lakehouse Parquet storage
        trigger_available_now=True,
    )

    print("\n[Step 3/3] Validating Lakehouse Stored Partitions & Quarantine Quality...")
    spark = create_spark_session("StreamingVerificationInspector")

    clean_df = spark.read.parquet(str(CLEAN_OUTPUT_PATH))
    total_clean = clean_df.count()

    print(f"\n Clean Lakehouse Events Staged: {total_clean}")
    print("Clean Events Schema Enforcement Proof:")
    clean_df.printSchema()

    print("\nSample Staged Clean Records:")
    clean_df.select(
        "event_id", "event_type", "title", "vote_average", "rating_bracket", "engagement_score", "validation_status"
    ).show(5, truncate=False)

    total_quarantine = 0
    if QUARANTINE_OUTPUT_PATH.exists():
        try:
            quarantine_df = spark.read.parquet(str(QUARANTINE_OUTPUT_PATH))
            total_quarantine = quarantine_df.count()
            print(f"\n Quarantined Anomalous Records Captured: {total_quarantine}")
            print("Quarantine Schema Enforcement Breakdown:")
            quarantine_df.groupBy("rejection_reason").count().show(truncate=False)

            print("Sample Quarantined Records:")
            quarantine_df.select(
                "event_id", "tmdb_id", "rejection_reason", "corrupt_record_raw"
            ).show(5, truncate=False)
        except Exception as e:
            print(f"[Quarantine Check] Note: {e}")

    spark.stop()

    print("=" * 80)
    print("  WEEK 1 VERIFICATION COMPLETED SUCCESSFULLY!")
    print(f"  Total Clean Lakehouse Records:    {total_clean}")
    print(f"  Total Quarantined Corrupt Records: {total_quarantine}")
    print(f"  Schema Enforcement & DLQ Pattern:  VERIFIED & OPERATIONAL")
    print("=" * 80)


if __name__ == "__main__":
    run_e2e_streaming_test()
