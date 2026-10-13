"""
End-to-End Automated Verification Suite for Phase-2 Week-2:
Lakehouse Storage Integration with Genuine Delta Lake & Real Catalog.

Verifies:
  1. Real Delta tables are created and registered in the DeltaCatalog.
  2. Kafka producer sends valid and invalid stream events with delivery confirmation,
     and newly generated event IDs are tracked.
  3. Clean and invalid events are stored separately via streaming sinks,
     and the generated event IDs appear in their respective tables.
  4. Physical partitioning is correct (event_date for both staging and quarantine).
  5. Native Delta transaction history works via DESCRIBE HISTORY.
  6. Catalog registration and table data persist across a fresh Spark application start
     WITHOUT calling init_catalog() before checking tables.
  7. Actual stored records are validated (no mock metadata).
"""

import json
import os
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

# Ensure Spark driver and worker processes use correct python executable and IP
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

if os.name == "nt" and os.path.exists(r"C:\hadoop"):
    os.environ["HADOOP_HOME"] = r"C:\hadoop"
    if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
        os.environ["PATH"] = rf"C:\hadoop\bin;{os.environ.get('PATH', '')}"

from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip
from confluent_kafka import Producer

from pipeline.lakehouse_catalog import (
    CLEAN_EVENTS_TABLE_PATH,
    QUARANTINE_EVENTS_TABLE_PATH,
    WAREHOUSE_DIR,
    LakehouseCatalogManager,
    configure_default_lakehouse_catalog,
    get_catalog_spark_session,
)
from pipeline.spark_stream_processor import start_processing, DEFAULT_BROKER, DEFAULT_TOPIC


def produce_test_events(num_events: int = 30, brokers: str = DEFAULT_BROKER, topic: str = DEFAULT_TOPIC):
    """
    Produces a known mix of valid and invalid movie streaming events to Kafka.
    Tracks all generated event IDs and verifies delivery reports.
    Returns: (success: bool, valid_event_ids: list, invalid_event_ids_with_id: list)
    """
    conf = {"bootstrap.servers": brokers, "client.id": "week2-delta-test-producer"}
    try:
        producer = Producer(conf)
    except Exception as e:
        print(f"[!] Warning: Could not connect to Kafka producer: {e}")
        return False, [], []

    print(f"[*] Generating {num_events} test events to Kafka topic '{topic}'...")
    valid_event_ids = []
    invalid_event_ids_with_id = []
    delivery_failures = []

    def delivery_callback(err, msg):
        if err is not None:
            delivery_failures.append(str(err))

    now_iso = datetime.now(timezone.utc).isoformat()

    sample_movies = [
        {"tmdb_id": 155, "title": "The Dark Knight", "vote_average": 9.0, "popularity": 120.5},
        {"tmdb_id": 278, "title": "The Shawshank Redemption", "vote_average": 9.3, "popularity": 140.2},
        {"tmdb_id": 238, "title": "The Godfather", "vote_average": 9.2, "popularity": 110.0},
        {"tmdb_id": 680, "title": "Pulp Fiction", "vote_average": 8.9, "popularity": 95.3},
    ]

    for i in range(num_events):
        event_id = str(uuid.uuid4())
        if i % 5 == 0:
            # Out of bounds vote (invalid)
            payload = {
                "event_id": event_id,
                "event_type": "rating_submitted",
                "event_timestamp": now_iso,
                "tmdb_id": 155,
                "title": "Invalid Vote Movie",
                "vote_average": 14.5,
                "popularity": 10.0,
            }
            raw = json.dumps(payload)
            invalid_event_ids_with_id.append(event_id)
        elif i % 5 == 1:
            # Missing TMDB ID (invalid)
            payload = {
                "event_id": event_id,
                "event_type": "movie_view",
                "event_timestamp": now_iso,
                "tmdb_id": None,
                "title": "No TMDB ID Movie",
                "vote_average": 7.0,
                "popularity": 5.0,
            }
            raw = json.dumps(payload)
            invalid_event_ids_with_id.append(event_id)
        elif i % 5 == 2:
            # Corrupt JSON syntax (quarantined by permissive parser)
            raw = f'{{"event_id": "{event_id}", "event_type": "corrupt", "malformed_json: missing_quotes'
        else:
            # Valid event
            m = sample_movies[i % len(sample_movies)]
            payload = {
                "event_id": event_id,
                "event_type": "movie_view",
                "event_timestamp": now_iso,
                "user_id": f"user_{i}",
                "session_id": str(uuid.uuid4()),
                "device": "web",
                "country": "US",
                "tmdb_id": m["tmdb_id"],
                "title": m["title"],
                "genres": "Drama, Crime",
                "popularity": m["popularity"],
                "vote_average": m["vote_average"],
                "vote_count": 1000,
                "release_date": "2008-07-18",
                "view_duration_seconds": 120,
                "user_rating": m["vote_average"],
            }
            raw = json.dumps(payload)
            valid_event_ids.append(event_id)

        producer.produce(
            topic,
            key=event_id.encode("utf-8"),
            value=raw.encode("utf-8"),
            on_delivery=delivery_callback,
        )

    remaining = producer.flush(timeout=10)
    success = (remaining == 0) and (len(delivery_failures) == 0)
    print(
        f"[*] Kafka Delivery Report: {num_events} sent, {len(valid_event_ids)} valid, "
        f"{len(invalid_event_ids_with_id)} tagged invalid. Failures: {len(delivery_failures)}."
    )
    return success, valid_event_ids, invalid_event_ids_with_id


def run_full_week2_verification():
    print("=" * 85)
    print(" PHASE-2 WEEK-2: GENUINE DELTA LAKE & REAL CATALOG VERIFICATION SUITE")
    print("=" * 85)

    test_results = {}

    # -------------------------------------------------------------------------
    # TEST 1: Real Delta Lake Catalog Initialization & Registration
    # -------------------------------------------------------------------------
    print("\n[Test 1/5] Verifying Delta Lake Catalog Registration (spark_catalog)...")
    spark = get_catalog_spark_session("Week2TestCatalog")
    catalog_mgr = configure_default_lakehouse_catalog(spark)

    tables = catalog_mgr.list_tables()
    print(f"  Catalog Name:      {catalog_mgr.catalog_name}")
    print(f"  Registered Tables: {tables}")

    has_staging = "staging_events" in tables
    has_quarantine = "quarantine_events" in tables

    # Check table provider is native Delta
    staging_desc = spark.sql("DESCRIBE TABLE EXTENDED movie_lakehouse.staging_events").collect()
    provider_row = [r for r in staging_desc if r["col_name"] == "Provider"]
    provider = provider_row[0]["data_type"] if provider_row else "unknown"

    if has_staging and has_quarantine and provider.lower() == "delta":
        print(f"  [PASS] Tables registered with native Provider: {provider}")
        test_results["Catalog Registration"] = "PASS"
    else:
        print(f"  [FAIL] Catalog verification failed. Tables: {tables}, Provider: {provider}")
        test_results["Catalog Registration"] = "FAIL"

    spark.stop()

    # -------------------------------------------------------------------------
    # TEST 2: Kafka Producer Delivery & Delta Streaming Sinks
    # -------------------------------------------------------------------------
    print("\n[Test 2/5] Running Stream Ingestion & Delta Streaming Sinks...")
    producer_ok, valid_ids, invalid_ids = produce_test_events(num_events=30)
    if not producer_ok:
        print("  [FAIL] Kafka message delivery failed or timed out.")
        test_results["Streaming Delta Sinks"] = "FAIL"
        return False
    else:
        print(f"  [PASS] Kafka producer delivery confirmed for all messages.")

    time.sleep(2)

    try:
        start_processing(
            mode="all",
            trigger_available_now=True,
            table_format="delta",
        )
        print("  [PASS] Streaming micro-batch completed successfully into Delta sinks.")
        test_results["Streaming Delta Sinks"] = "PASS"
    except Exception as e:
        print(f"  [FAIL] Streaming execution failed: {e}")
        test_results["Streaming Delta Sinks"] = "FAIL"

    # -------------------------------------------------------------------------
    # TEST 3: Separation of Clean vs Invalid Events, Event ID Check & Partitioning
    # -------------------------------------------------------------------------
    print("\n[Test 3/5] Verifying Clean vs Quarantine Separation, Event IDs & Partitions...")
    spark = get_catalog_spark_session("Week2DataValidator")

    staging_df = spark.table("movie_lakehouse.staging_events")
    quarantine_df = spark.table("movie_lakehouse.quarantine_events")

    staging_count = staging_df.count()
    quarantine_count = quarantine_df.count()

    print(f"  Total Records in movie_lakehouse.staging_events:    {staging_count}")
    print(f"  Total Records in movie_lakehouse.quarantine_events: {quarantine_count}")

    # Verify newly generated valid event IDs landed in staging_events
    matching_valid = staging_df.filter(staging_df.event_id.isin(valid_ids)).count()
    print(f"  Verified newly generated valid event IDs in staging: {matching_valid}/{len(valid_ids)}")

    # Verify newly generated invalid event IDs landed in quarantine_events
    matching_invalid = quarantine_df.filter(quarantine_df.event_id.isin(invalid_ids)).count()
    print(f"  Verified newly generated invalid event IDs in quarantine: {matching_invalid}/{len(invalid_ids)}")

    # Check rejection_reason column is preserved
    has_rejection_col = "rejection_reason" in quarantine_df.columns
    print(f"  Quarantine preserves 'rejection_reason' column: {has_rejection_col}")

    # Check physical partitions on disk
    staging_partitions = list(CLEAN_EVENTS_TABLE_PATH.glob("event_date=*"))
    quarantine_partitions = list(QUARANTINE_EVENTS_TABLE_PATH.glob("event_date=*"))

    print(f"  Physical Staging Partitions:    {[p.name for p in staging_partitions]}")
    print(f"  Physical Quarantine Partitions: {[p.name for p in quarantine_partitions]}")

    if (
        matching_valid > 0
        and matching_invalid > 0
        and has_rejection_col
        and len(staging_partitions) > 0
        and len(quarantine_partitions) > 0
    ):
        print("  [PASS] Clean and quarantine records stored separately with event_date partitioning & verified IDs.")
        test_results["Separation & Partitioning"] = "PASS"
    else:
        print("  [FAIL] Separation, partitioning, or event ID matching validation failed.")
        test_results["Separation & Partitioning"] = "FAIL"

    # -------------------------------------------------------------------------
    # TEST 4: Native Delta Transaction History (DESCRIBE HISTORY)
    # -------------------------------------------------------------------------
    print("\n[Test 4/5] Verifying Native Delta Transaction History (WAL)...")
    try:
        staging_hist = spark.sql("DESCRIBE HISTORY movie_lakehouse.staging_events").collect()
        quarantine_hist = spark.sql("DESCRIBE HISTORY movie_lakehouse.quarantine_events").collect()

        print(f"  Staging Delta Commit Versions:    {len(staging_hist)} commit(s)")
        print(f"  Quarantine Delta Commit Versions: {len(quarantine_hist)} commit(s)")

        latest_op = staging_hist[0]["operation"] if staging_hist else "NONE"
        print(f"  Latest Staging Operation:         {latest_op}")

        # Check native _delta_log directory
        staging_delta_log = CLEAN_EVENTS_TABLE_PATH / "_delta_log"
        has_delta_log = staging_delta_log.exists() and len(list(staging_delta_log.glob("*.json"))) > 0

        if len(staging_hist) > 0 and len(quarantine_hist) > 0 and has_delta_log:
            print("  [PASS] Native Delta transaction logs and DESCRIBE HISTORY operational.")
            test_results["Delta Transaction History"] = "PASS"
        else:
            print("  [FAIL] Delta transaction history verification failed.")
            test_results["Delta Transaction History"] = "FAIL"
    except Exception as e:
        print(f"  [FAIL] Delta history query failed: {e}")
        test_results["Delta Transaction History"] = "FAIL"

    spark.stop()

    # -------------------------------------------------------------------------
    # TEST 5: Fresh Spark Session Restart & Catalog Persistence
    # (Without calling init_catalog() before checking the tables)
    # -------------------------------------------------------------------------
    print("\n[Test 5/5] Verifying Table Persistence across Fresh Spark Application Start...")
    print("  (Starting fresh Spark session WITHOUT calling init_catalog() or configure_default_lakehouse_catalog)")

    spark_fresh = get_catalog_spark_session("Week2PersistenceCheckFreshSession")

    # Directly check catalog tables from the fresh Spark session
    reloaded_tables = [r.tableName for r in spark_fresh.sql("SHOW TABLES IN movie_lakehouse").collect()]
    print(f"  Fresh Session Registered Tables: {reloaded_tables}")

    has_reloaded_staging = "staging_events" in reloaded_tables
    has_reloaded_quarantine = "quarantine_events" in reloaded_tables

    reloaded_staging_count = spark_fresh.table("movie_lakehouse.staging_events").count()
    reloaded_quarantine_count = spark_fresh.table("movie_lakehouse.quarantine_events").count()

    print(f"  Reloaded Staging Count:    {reloaded_staging_count} (Matches: {reloaded_staging_count == staging_count})")
    print(f"  Reloaded Quarantine Count: {reloaded_quarantine_count} (Matches: {reloaded_quarantine_count == quarantine_count})")

    # Sample query directly from table
    print("\n  Sample Stored Staging Events (Direct Catalog Query):")
    spark_fresh.table("movie_lakehouse.staging_events").select(
        "event_id", "title", "vote_average", "rating_bracket", "event_date"
    ).show(5, truncate=False)

    print("\n  Sample Stored Quarantined Events (Direct Catalog Query):")
    spark_fresh.table("movie_lakehouse.quarantine_events").select(
        "event_id", "rejection_reason", "event_date"
    ).show(5, truncate=False)

    spark_fresh.stop()

    if (
        has_reloaded_staging
        and has_reloaded_quarantine
        and reloaded_staging_count == staging_count
        and reloaded_quarantine_count == quarantine_count
    ):
        print("  [PASS] Catalog tables and data persisted and verified without calling init_catalog().")
        test_results["Persistence Across Restart"] = "PASS"
    else:
        print("  [FAIL] Persistence count or catalog table mismatch after restart.")
        test_results["Persistence Across Restart"] = "FAIL"

    # -------------------------------------------------------------------------
    # SUMMARY
    # -------------------------------------------------------------------------
    print("\n" + "=" * 85)
    print(" WEEK-2 LAKEHOUSE VERIFICATION TEST SUMMARY")
    print("=" * 85)
    all_passed = True
    for test_name, status in test_results.items():
        print(f"  {test_name:<35}: {status}")
        if status != "PASS":
            all_passed = False
    print("=" * 85)
    if all_passed:
        print(" ALL WEEK 2 REQUIREMENTS SUCCESSFULLY VERIFIED (PASS)")
    else:
        print(" SOME WEEK 2 TESTS FAILED - REVIEW LOGS ABOVE")
    print("=" * 85)

    return all_passed


if __name__ == "__main__":
    success = run_full_week2_verification()
    sys.exit(0 if success else 1)
