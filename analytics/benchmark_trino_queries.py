"""
Trino Query Benchmarking & Performance Optimization Suite (Week 4).
Runs representative Lakehouse SQL analytical queries against Trino,
evaluates latency before and after Delta Lake table compaction (OPTIMIZE & VACUUM),
and saves an audit report to analytics/reports/week4_optimization_benchmark.md.
"""

import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Tuple

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.trino_client import execute_trino_query, get_trino_connection, TRINO_SCHEMA
from pipeline.lakehouse_optimizer import LakehouseOptimizer, STAGING_PATH, QUARANTINE_PATH

BENCHMARK_REPORT_PATH = PROJECT_ROOT / "analytics" / "reports" / "week4_optimization_benchmark.md"

BENCHMARK_QUERIES = [
    {
        "id": "Q1_TOTAL_SUMMARY",
        "name": "Volume & Validation Rate Summary",
        "description": "Calculates overall counts and ratios across staging and DLQ tables.",
        "sql": f"""
        SELECT 
            (SELECT count(*) FROM delta.{TRINO_SCHEMA}.staging_events) AS valid_events,
            (SELECT count(*) FROM delta.{TRINO_SCHEMA}.quarantine_events) AS quarantined_events,
            round(
                CAST((SELECT count(*) FROM delta.{TRINO_SCHEMA}.staging_events) AS DOUBLE) /
                NULLIF((SELECT count(*) FROM delta.{TRINO_SCHEMA}.staging_events) + (SELECT count(*) FROM delta.{TRINO_SCHEMA}.quarantine_events), 0) * 100.0,
                2
            ) AS validation_rate_pct
        """,
    },
    {
        "id": "Q2_ENGAGEMENT_AGG",
        "name": "Top Movies by Engagement & Ratings",
        "description": "Aggregates view events, average engagement score, and vote ratings per title.",
        "sql": f"""
        SELECT 
            title,
            count(*) AS view_count,
            round(avg(vote_average), 2) AS avg_vote,
            round(avg(engagement_score), 2) AS avg_engagement,
            max(engagement_score) AS max_engagement
        FROM delta.{TRINO_SCHEMA}.staging_events
        GROUP BY title
        ORDER BY avg_engagement DESC, view_count DESC
        """,
    },
    {
        "id": "Q3_RATING_BRACKETS",
        "name": "Rating Bracket Metric Distribution",
        "description": "Groups by curated rating brackets and computes aggregations.",
        "sql": f"""
        SELECT 
            rating_bracket,
            count(*) AS total_records,
            round(avg(vote_average), 2) AS avg_vote,
            round(min(vote_average), 2) AS min_vote,
            round(max(vote_average), 2) AS max_vote,
            round(avg(engagement_score), 2) AS avg_score
        FROM delta.{TRINO_SCHEMA}.staging_events
        GROUP BY rating_bracket
        ORDER BY total_records DESC
        """,
    },
    {
        "id": "Q4_PARTITION_FILTER",
        "name": "Date Partition Pruned Event Scan",
        "description": "Scans partitioned files matching recent event_date and filters by threshold.",
        "sql": f"""
        SELECT 
            event_date,
            count(*) AS daily_views,
            round(avg(engagement_score), 2) AS daily_avg_engagement
        FROM delta.{TRINO_SCHEMA}.staging_events
        WHERE event_date >= CAST('2026-10-13' AS DATE)
        GROUP BY event_date
        ORDER BY event_date DESC
        """,
    },
    {
        "id": "Q5_QUARANTINE_REASONS",
        "name": "Quarantine DLQ Root-Cause Breakdown",
        "description": "Aggregates schema and data validation failure causes from quarantine table.",
        "sql": f"""
        SELECT 
            rejection_reason,
            count(*) AS failure_count,
            round(CAST(count(*) AS DOUBLE) / (SELECT count(*) FROM delta.{TRINO_SCHEMA}.quarantine_events) * 100.0, 2) AS pct_of_total
        FROM delta.{TRINO_SCHEMA}.quarantine_events
        GROUP BY rejection_reason
        ORDER BY failure_count DESC
        """,
    },
]


def run_single_query_benchmark(sql: str, iterations: int = 3) -> float:
    """Executes a query multiple times in Trino and returns average execution time in milliseconds."""
    timings = []
    for _ in range(iterations):
        t0 = time.perf_counter()
        execute_trino_query(sql)
        elapsed_ms = (time.perf_counter() - t0) * 1000
        timings.append(elapsed_ms)
    return round(sum(timings) / len(timings), 2)


def run_full_benchmark() -> Dict[str, Any]:
    print("=" * 85)
    print(" CAT-2 WEEK 4: TRINO QUERY PERFORMANCE BENCHMARK & DELTA OPTIMIZATION")
    print("=" * 85)

    staging_opt = LakehouseOptimizer(STAGING_PATH)
    quarantine_opt = LakehouseOptimizer(QUARANTINE_PATH)

    pre_staging_stats = staging_opt.get_stats()
    pre_quarantine_stats = quarantine_opt.get_stats()

    print(f"[*] Pre-Optimization Staging Files:    {pre_staging_stats['active_file_count']} files ({pre_staging_stats['total_size_kb']} KB)")
    print(f"[*] Pre-Optimization Quarantine Files: {pre_quarantine_stats['active_file_count']} files ({pre_quarantine_stats['total_size_kb']} KB)")

    # 1. Benchmark Before Optimization
    print("\n--- Phase 1: Benchmarking Trino Queries (Before Optimization) ---")
    before_results = {}
    for q in BENCHMARK_QUERIES:
        print(f"  Measuring {q['id']} ({q['name']})...")
        avg_ms = run_single_query_benchmark(q["sql"], iterations=3)
        before_results[q["id"]] = avg_ms
        print(f"    Avg Latency: {avg_ms:.2f} ms")

    # 2. Execute Compaction and Vacuum Optimization
    print("\n--- Phase 2: Executing Delta Lake Table Compaction & Vacuum ---")
    compact_staging = staging_opt.compact()
    compact_quarantine = quarantine_opt.compact()

    vacuum_staging = staging_opt.vacuum(retention_hours=168, dry_run=False)
    vacuum_quarantine = quarantine_opt.vacuum(retention_hours=168, dry_run=False)

    post_staging_stats = staging_opt.get_stats()
    post_quarantine_stats = quarantine_opt.get_stats()

    print(f"  Staging Files:    {pre_staging_stats['active_file_count']} -> {post_staging_stats['active_file_count']} (Compacted in {compact_staging['duration_seconds']}s)")
    print(f"  Quarantine Files: {pre_quarantine_stats['active_file_count']} -> {post_quarantine_stats['active_file_count']} (Compacted in {compact_quarantine['duration_seconds']}s)")

    # 3. Benchmark After Optimization
    print("\n--- Phase 3: Benchmarking Trino Queries (After Optimization) ---")
    after_results = {}
    for q in BENCHMARK_QUERIES:
        print(f"  Measuring {q['id']} ({q['name']})...")
        avg_ms = run_single_query_benchmark(q["sql"], iterations=3)
        after_results[q["id"]] = avg_ms
        print(f"    Avg Latency: {avg_ms:.2f} ms")

    # 4. Generate Benchmark Report
    BENCHMARK_REPORT_PATH.parent.mkdir(parents=True, exist_ok=True)

    report_lines = [
        "# Week 4: Trino Query Performance & Lakehouse Optimization Benchmark Report",
        "",
        f"- **Execution Timestamp**: {datetime.now(timezone.utc).isoformat()} UTC",
        f"- **Query Engine**: Trino 435 (Docker `de-trino` on port 8082)",
        f"- **Storage Format**: Delta Lake 4.4.1 (ACID Parquet + Write-Ahead Log)",
        f"- **Catalog**: Trino `delta_lake` connector (`delta.movie_lakehouse`)",
        "",
        "## 1. Storage & Compaction Metrics",
        "",
        "| Table Name | Initial Files | Compacted Files | Initial Size (KB) | Post-Compaction Size (KB) | Compaction Time (s) | Vacuum Status |",
        "| :--- | :---: | :---: | :---: | :---: | :---: | :---: |",
        f"| `movie_lakehouse.staging_events` | {pre_staging_stats['active_file_count']} | {post_staging_stats['active_file_count']} | {pre_staging_stats['total_size_kb']} | {post_staging_stats['total_size_kb']} | {compact_staging['duration_seconds']} | 168h Safe Retain |",
        f"| `movie_lakehouse.quarantine_events` | {pre_quarantine_stats['active_file_count']} | {post_quarantine_stats['active_file_count']} | {pre_quarantine_stats['total_size_kb']} | {post_quarantine_stats['total_size_kb']} | {compact_quarantine['duration_seconds']} | 168h Safe Retain |",
        "",
        "## 2. Trino Analytical Query Latency Comparison",
        "",
        "| Query ID | Query Name & Workload | Latency Before (ms) | Latency After (ms) | Speedup / Improvement |",
        "| :--- | :--- | :---: | :---: | :---: |",
    ]

    for q in BENCHMARK_QUERIES:
        qid = q["id"]
        b_ms = before_results[qid]
        a_ms = after_results[qid]
        diff_ms = b_ms - a_ms
        pct = round((diff_ms / b_ms) * 100.0, 1) if b_ms > 0 else 0.0
        improvement_str = f"+{pct}% faster ({diff_ms:+.1f} ms)" if diff_ms >= 0 else f"{pct}% ({diff_ms:+.1f} ms)"
        report_lines.append(f"| `{qid}` | {q['name']} | {b_ms:.2f} ms | {a_ms:.2f} ms | **{improvement_str}** |")

    report_lines.extend([
        "",
        "## 3. Query Definitions",
        "",
    ])

    for q in BENCHMARK_QUERIES:
        report_lines.append(f"### `{q['id']}`: {q['name']}")
        report_lines.append(f"*{q['description']}*")
        report_lines.append("```sql")
        report_lines.append(q["sql"].strip())
        report_lines.append("```")
        report_lines.append("")

    report_content = "\n".join(report_lines)
    BENCHMARK_REPORT_PATH.write_text(report_content, encoding="utf-8")
    print(f"\n[+] Benchmark Report generated and saved to: {BENCHMARK_REPORT_PATH}")

    return {
        "before": before_results,
        "after": after_results,
        "pre_staging_stats": pre_staging_stats,
        "post_staging_stats": post_staging_stats,
    }


if __name__ == "__main__":
    run_full_benchmark()
