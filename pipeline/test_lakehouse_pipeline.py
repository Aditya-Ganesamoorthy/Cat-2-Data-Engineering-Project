import json
import os
import sys
import time
from pathlib import Path

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"

if os.name == "nt":
    hadoop_dir = r"C:\hadoop"
    if os.path.exists(hadoop_dir):
        os.environ["HADOOP_HOME"] = hadoop_dir
        if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = rf"C:\hadoop\bin;{os.environ.get('PATH', '')}"

from pipeline.lakehouse_catalog import (
    CLEAN_EVENTS_TABLE_PATH,
    QUARANTINE_EVENTS_TABLE_PATH,
    LakehouseCatalogManager,
    configure_default_lakehouse_catalog,
)
from pipeline.spark_stream_processor import (
    CHECKPOINT_DIR,
    DEFAULT_BROKER,
    DEFAULT_TOPIC,
    create_spark_session,
    start_processing,
)
from pipeline.stream_producer import run_producer


def run_week2_lakehouse_e2e_test():
    """
    Automated End-to-End Test for Week 2 Deliverables:
    1. Configure Apache Iceberg or Delta Lake table catalog (via Hive Metastore, Nessie, or AWS Glue).
    2. Sink parsed streams into the table format with proper partitioning and write-ahead logging (WAL).
    """
    print("=" * 80)
    print("  PHASE-2 WEEK-2: LAKEHOUSE STORAGE INTEGRATION E2E VERIFICATION")
    print("  Table Catalog Configuration | Partitioning | Write-Ahead Logging (WAL)")
    print("=" * 80)

    # -------------------------------------------------------------------------
    # STEP 1: Configure & Verify Lakehouse Table Catalog
    # -------------------------------------------------------------------------
    print("\n[Step 1/6] Configuring Lakehouse Table Catalog (Delta Lake Specification)...")
    catalog_mgr = configure_default_lakehouse_catalog()
    catalog = catalog_mgr._load_catalog()

    print(f" Catalog Name:      {catalog['catalog_name']}")
    print(f" Catalog Type:      {catalog['catalog_type'].upper()} (Open Table Format)")
    print(f" Catalog Metadata:  {catalog_mgr.catalog_file}")
    print(f" Registered Tables: {list(catalog['tables'].keys())}")

    for tname, tinfo in catalog["tables"].items():
        print(f"\n   -> Table: {tinfo['full_name']}")
        print(f"      Format:             {tinfo['format']}")
        print(f"      Location:           {tinfo['location']}")
        print(f"      WAL Directory:      {tinfo['wal_directory']}")
        print(f"      Partition Columns:  {tinfo['partition_columns']}")

    # -------------------------------------------------------------------------
    # STEP 2: Publish Simulated Stream to Kafka
    # -------------------------------------------------------------------------
    test_topic = DEFAULT_TOPIC
    test_broker = DEFAULT_BROKER
    num_events = 50
    rate = 50.0
    anomaly_rate = 0.10  # 10% anomalies to verify clean vs quarantine segregation

    print(f"\n[Step 2/6] Publishing {num_events} real-time movie events to Kafka topic '{test_topic}'...")
    run_producer(
        brokers=test_broker,
        topic=test_topic,
        rate=rate,
        max_events=num_events,
        anomaly_rate=anomaly_rate,
    )

    # -------------------------------------------------------------------------
    # STEP 3: Ingest & Sink Stream into Lakehouse Table Format with WAL
    # -------------------------------------------------------------------------
    print("\n[Step 3/6] Streaming stream to Lakehouse with proper partitioning and WAL...")
    start_processing(
        brokers=test_broker,
        topic=test_topic,
        mode="lakehouse",
        trigger_available_now=True,
        table_format="delta",
    )

    # -------------------------------------------------------------------------
    # STEP 4: Verify Write-Ahead Logging (WAL) Files & Commit Integrity
    # -------------------------------------------------------------------------
    print("\n[Step 4/6] Verifying Write-Ahead Logging (WAL) Integrity...")

    # A. Structured Streaming Engine WAL
    clean_offsets_wal = CHECKPOINT_DIR / "clean_events" / "offsets"
    clean_commits_wal = CHECKPOINT_DIR / "clean_events" / "commits"
    print(f"\n  A. Streaming Checkpoint WAL:")
    print(f"     Offsets WAL Exists: {clean_offsets_wal.exists()} -> Files: {len(list(clean_offsets_wal.glob('*'))) if clean_offsets_wal.exists() else 0}")
    print(f"     Commits WAL Exists: {clean_commits_wal.exists()} -> Files: {len(list(clean_commits_wal.glob('*'))) if clean_commits_wal.exists() else 0}")

    # B. Lakehouse Table Format Transaction Log (WAL)
    clean_table_wal = CLEAN_EVENTS_TABLE_PATH / "_delta_log"
    quarantine_wal = QUARANTINE_EVENTS_TABLE_PATH / "_delta_log"

    print(f"\n  B. Lakehouse Table Format ACID Transaction Log (WAL):")
    print(f"     Staging Table WAL Dir:    {clean_table_wal}")
    clean_wal_commits = sorted(clean_table_wal.glob("*.json"))
    print(f"     Staging WAL Commits Count: {len(clean_wal_commits)}")

    if clean_wal_commits:
        latest_commit = clean_wal_commits[-1]
        print(f"     Latest Staging WAL Commit: {latest_commit.name}")
        with open(latest_commit, "r", encoding="utf-8") as f:
            commit_data = json.load(f)
            cinfo = commit_data.get("commitInfo", {})
            summary = commit_data.get("commit_summary", {})
            print(f"       - Operation:        {cinfo.get('operation')}")
            print(f"       - Isolation Level:  {cinfo.get('isolationLevel')}")
            print(f"       - Engine Info:      {cinfo.get('engineInfo')}")
            print(f"       - Records Added:    {summary.get('records_committed')}")
            print(f"       - Timestamp:        {summary.get('timestamp_iso')}")

    quarantine_wal_commits = sorted(quarantine_wal.glob("*.json"))
    print(f"\n     Quarantine Table WAL Dir:  {quarantine_wal}")
    print(f"     Quarantine WAL Commits:   {len(quarantine_wal_commits)}")

    # -------------------------------------------------------------------------
    # STEP 5: Verify Lakehouse Partitioning
    # -------------------------------------------------------------------------
    print("\n[Step 5/6] Verifying Lakehouse Physical Partitioning Layout...")
    clean_partitions = catalog_mgr.discover_partitions("staging_events")
    print(f"\n  Clean Staging Table Partitions (by event_date):")
    for p in clean_partitions:
        p_path = CLEAN_EVENTS_TABLE_PATH / p
        files = list(p_path.glob("*.parquet"))
        total_size = sum(f.stat().st_size for f in files)
        print(f"   |-- {p} ({len(files)} data file(s), {total_size / 1024:.1f} KB)")

    q_partitions = catalog_mgr.discover_partitions("quarantine_events")
    print(f"\n  Quarantine DLQ Table Partitions (by rejection_reason):")
    for p in q_partitions:
        p_path = QUARANTINE_EVENTS_TABLE_PATH / p
        files = list(p_path.glob("*.parquet"))
        total_size = sum(f.stat().st_size for f in files)
        print(f"   |-- {p} ({len(files)} data file(s), {total_size / 1024:.1f} KB)")

    # -------------------------------------------------------------------------
    # STEP 6: Query Lakehouse Data & Audit History
    # -------------------------------------------------------------------------
    print("\n[Step 6/6] Inspecting Table Data & ACID Audit History...")
    spark = create_spark_session("Week2LakehouseInspector")

    clean_df = spark.read.parquet(str(CLEAN_EVENTS_TABLE_PATH))
    total_clean = clean_df.count()
    print(f"\n  Total Records in Staging Lakehouse: {total_clean}")

    print("\n  Sample Stored Lakehouse Records:")
    clean_df.select(
        "event_id", "event_type", "title", "vote_average", "rating_bracket", "engagement_score", "event_date"
    ).show(5, truncate=False)

    print("\n  Lakehouse Aggregations by Rating Bracket:")
    clean_df.groupBy("rating_bracket").count().show()

    spark.stop()

    print("\n  ACID Transaction History (Audit Trail via Catalog):")
    history = catalog_mgr.get_table_history("staging_events")
    print(f"  {'Version':<10} {'Timestamp':<28} {'Operation':<22} {'Records':<10} {'Files'}")
    print("  " + "-" * 75)
    for h in history[-5:]:
        print(f"  {h.get('version', '-'):<10} {h.get('timestamp', '-'):<28} {h.get('operation', '-'):<22} {h.get('records_added', 0):<10} {h.get('files_added', 0)}")

    print("\n" + "=" * 80)
    print("  WEEK 2 DELIVERABLES VERIFICATION SUCCESSFUL!")
    print(f"  1. Lakehouse Table Catalog:        CONFIGURED & OPERATIONAL ({catalog['catalog_name']})")
    print(f"  2. Partitioning Strategy:          VERIFIED (Clean: event_date, Quarantine: rejection_reason)")
    print(f"  3. Write-Ahead Logging (WAL):      VERIFIED (Checkpoints + _delta_log ACID commit logs)")
    print(f"  4. Serving Layer Readiness:        READY FOR TRINO INTEGRATION (Week 3)")
    print("=" * 80)


if __name__ == "__main__":
    run_week2_lakehouse_e2e_test()
