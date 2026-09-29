"""
Lakehouse Storage Viewer & Query CLI.
Directly inspects, queries, and displays data stored in the Lakehouse
(both clean staging_events and quarantine DLQ tables), including
physical partitions, schema enforcement, WAL transaction logs, and analytics.
"""

import argparse
import json
import os
import sys
from pathlib import Path
from typing import Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

LAKEHOUSE_BASE_DIR = PROJECT_ROOT / "data" / "lakehouse"
STAGING_PATH = LAKEHOUSE_BASE_DIR / "staging_events"
QUARANTINE_PATH = LAKEHOUSE_BASE_DIR / "quarantine"


def load_table_dataset(table_path: Path):
    """
    Loads Parquet files from Lakehouse table using pyarrow dataset
    with automatic partition discovery.
    """
    import pyarrow.dataset as ds
    if not table_path.exists():
        return None
    try:
        dataset = ds.dataset(str(table_path), partitioning="hive", format="parquet")
        return dataset.to_table().to_pandas()
    except Exception as e:
        # Fallback for empty or non-existent
        return None


def get_latest_wal_commit(table_path: Path):
    """
    Reads the latest ACID transaction log commit from _delta_log/
    """
    delta_log_dir = table_path / "_delta_log"
    if not delta_log_dir.exists():
        return None
    commit_files = sorted(delta_log_dir.glob("*.json"))
    if not commit_files:
        return None
    try:
        with open(commit_files[-1], "r", encoding="utf-8") as f:
            raw = json.load(f)
            summary = raw.get("commit_summary", {})
            info = raw.get("commitInfo", {})
            return {
                "filename": commit_files[-1].name,
                "version": summary.get("version", info.get("readVersion", 0)),
                "operation": info.get("operation", "STREAMING WRITE"),
                "timestamp": summary.get("timestamp_iso", str(info.get("timestamp", "N/A"))),
                "records_added": summary.get("records_committed", 0),
            }
    except Exception:
        return None


def display_staging_table(limit: int = 10, date_filter: Optional[str] = None):
    print("\n" + "=" * 95)
    print(" LAKEHOUSE TABLE: movie_lakehouse.staging_events (Clean & Validated Events)")
    print(f" Storage Path:    {STAGING_PATH}")
    print(" Partitioned By:  [event_date]")
    print("=" * 95)

    df = load_table_dataset(STAGING_PATH)
    if df is None or len(df) == 0:
        print("[!] No records found in staging_events Lakehouse table.")
        return

    # Filter by date if specified
    if date_filter and "event_date" in df.columns:
        df = df[df["event_date"].astype(str) == str(date_filter)]
        print(f" Filter Applied:  event_date == {date_filter}")

    total_count = len(df)
    wal_info = get_latest_wal_commit(STAGING_PATH)

    print(f" Total Records:   {total_count:,}")
    if wal_info:
        print(f" Latest WAL Log:  _delta_log/{wal_info['filename']} (Version {wal_info['version']})")
        print(f"   - Operation:    {wal_info['operation']}")
        print(f"   - Timestamp:    {wal_info['timestamp']}")
        print(f"   - Records Added: {wal_info['records_added']}")

    # Discover Partitions
    partitions = [p.name for p in STAGING_PATH.glob("event_date=*") if p.is_dir()]
    if partitions:
        print(f"\n Active Partitions ({len(partitions)}):")
        for p in sorted(partitions):
            p_dir = STAGING_PATH / p
            file_count = len(list(p_dir.glob("*.parquet")))
            total_bytes = sum(f.stat().st_size for f in p_dir.glob("*.parquet")) / 1024
            print(f"   |-- {p:<25} ({file_count} files, {total_bytes:.1f} KB)")

    # Sample Records Table
    print(f"\n Sample Records (showing {min(limit, total_count)} of {total_count:,}):")
    cols_to_show = ["event_id", "event_type", "title", "vote_average", "rating_bracket", "engagement_score", "event_date"]
    avail_cols = [c for c in cols_to_show if c in df.columns]

    sample_df = df[avail_cols].tail(limit)
    
    # Formatted Header
    header = (
        f"{'Event ID':<38} "
        f"{'Type':<17} "
        f"{'Movie Title':<26} "
        f"{'Vote':<6} "
        f"{'Bracket':<12} "
        f"{'Score':<7} "
        f"{'Date'}"
    )
    print("-" * 115)
    print(header)
    print("-" * 115)

    for _, row in sample_df.iterrows():
        eid = str(row.get("event_id", ""))[:36]
        etype = str(row.get("event_type", ""))[:15]
        title = str(row.get("title", ""))[:24]
        vote = f"{float(row.get('vote_average', 0.0)):.1f}" if row.get("vote_average") is not None else "-"
        bracket = str(row.get("rating_bracket", ""))[:11]
        score = f"{float(row.get('engagement_score', 0.0)):.1f}" if row.get("engagement_score") is not None else "-"
        edate = str(row.get("event_date", ""))
        print(f"{eid:<38} {etype:<17} {title:<26} {vote:<6} {bracket:<12} {score:<7} {edate}")
    print("-" * 115)

    # Analytics Aggregations
    if "rating_bracket" in df.columns:
        print("\n Rating Bracket Distribution:")
        counts = df["rating_bracket"].value_counts()
        for bracket, count in counts.items():
            pct = (count / total_count) * 100
            bar = "#" * int(pct / 5)
            print(f"   {bracket:<14}: {count:>5} ({pct:>5.1f}%)  {bar}")

    if "title" in df.columns and "engagement_score" in df.columns:
        print("\n Top 5 Movies by Engagement Score:")
        top_movies = (
            df[df["engagement_score"] > 0]
            .groupby("title")["engagement_score"]
            .agg(["count", "mean", "max"])
            .sort_values(by="mean", ascending=False)
            .head(5)
        )
        if not top_movies.empty:
            print(f"   {'Movie Title':<28} {'Events':<8} {'Avg Score':<11} {'Max Score'}")
            print("   " + "-" * 55)
            for mtitle, row in top_movies.iterrows():
                print(f"   {str(mtitle)[:26]:<28} {int(row['count']):<8} {row['mean']:<11.2f} {row['max']:.1f}")


def display_quarantine_table(limit: int = 10):
    print("\n" + "=" * 95)
    print(" LAKEHOUSE TABLE: movie_lakehouse.quarantine_events (Dead-Letter Queue / DLQ)")
    print(f" Storage Path:    {QUARANTINE_PATH}")
    print(" Partitioned By:  [rejection_reason]")
    print("=" * 95)

    df = load_table_dataset(QUARANTINE_PATH)
    if df is None or len(df) == 0:
        print("[!] No records found in quarantine Lakehouse table.")
        return

    total_count = len(df)
    wal_info = get_latest_wal_commit(QUARANTINE_PATH)

    print(f" Total Quarantined Records: {total_count:,}")
    if wal_info:
        print(f" Latest WAL Log:            _delta_log/{wal_info['filename']} (Version {wal_info['version']})")
        print(f"   - Operation:              {wal_info['operation']}")
        print(f"   - Records Quarantined:    {wal_info['records_added']}")

    # Partitions by rejection reason
    if "rejection_reason" in df.columns:
        print("\n Quarantined Violations by Rejection Reason:")
        counts = df["rejection_reason"].value_counts()
        for reason, count in counts.items():
            pct = (count / total_count) * 100
            print(f"   |-- {str(reason):<38}: {count:>4} record(s) ({pct:.1f}%)")

    # Sample DLQ records
    print(f"\n Sample Quarantined Records (showing {min(limit, total_count)}):")
    sample_df = df.tail(limit)

    print("-" * 115)
    print(f"{'Event ID':<38} {'TMDB ID':<10} {'Rejection Reason':<35} {'Corrupt Payload Snippet'}")
    print("-" * 115)

    for _, row in sample_df.iterrows():
        eid = str(row.get("event_id", "NULL"))[:36]
        if eid == "nan" or not eid:
            eid = "NULL"
        tid = str(row.get("tmdb_id", "NULL"))[:8]
        if tid == "nan":
            tid = "NULL"
        reason = str(row.get("rejection_reason", "UNKNOWN"))[:33]
        corrupt = str(row.get("corrupt_record_raw", ""))[:32].replace("\n", " ")
        if not corrupt or corrupt == "nan":
            corrupt = "-"
        print(f"{eid:<38} {tid:<10} {reason:<35} {corrupt}")
    print("-" * 115)


def main():
    parser = argparse.ArgumentParser(description="Query & Display Lakehouse Storage Tables")
    parser.add_argument(
        "--table",
        choices=["staging", "quarantine", "all"],
        default="all",
        help="Lakehouse table to display (staging, quarantine, or all)",
    )
    parser.add_argument("--limit", type=int, default=10, help="Number of sample records to display (default: 10)")
    parser.add_argument("--date", type=str, default=None, help="Filter staging events by event_date (YYYY-MM-DD)")
    parser.add_argument("--json", action="store_true", help="Output records as raw JSON")

    args = parser.parse_args()

    if args.json:
        if args.table in ["staging", "all"]:
            df = load_table_dataset(STAGING_PATH)
            if df is not None:
                print(df.tail(args.limit).to_json(orient="records", indent=2))
        if args.table in ["quarantine", "all"]:
            df = load_table_dataset(QUARANTINE_PATH)
            if df is not None:
                print(df.tail(args.limit).to_json(orient="records", indent=2))
        return

    if args.table in ["staging", "all"]:
        display_staging_table(limit=args.limit, date_filter=args.date)

    if args.table in ["quarantine", "all"]:
        display_quarantine_table(limit=args.limit)

    print("\n" + "=" * 95)
    print(" LAKEHOUSE DISPLAY COMPLETE")
    print("=" * 95 + "\n")


if __name__ == "__main__":
    main()
