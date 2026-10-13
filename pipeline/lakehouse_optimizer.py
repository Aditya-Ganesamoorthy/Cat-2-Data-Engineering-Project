"""
Lakehouse Delta Table Optimizer & Maintenance Utility.
Performs safe bin-packing compaction (OPTIMIZE), snapshot retention auditing,
and vacuuming with safety-enforced retention windows for Delta Lake tables.
Supports execution via CLI, Python API, and Airflow DAGs.
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Dict, List, Optional

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from deltalake import DeltaTable

STAGING_PATH = PROJECT_ROOT / "data" / "lakehouse" / "staging_events"
QUARANTINE_PATH = PROJECT_ROOT / "data" / "lakehouse" / "quarantine_events"

DEFAULT_RETENTION_HOURS = 168  # 7-day safe retention period


class LakehouseOptimizer:
    """
    Manages safe Delta Lake maintenance operations:
    - Compaction (bin-packing small Parquet files into optimal sizes)
    - Vacuuming (cleaning up expired snapshot files with retention enforcement)
    - Metadata and file statistics auditing
    """

    def __init__(self, table_path: Path):
        self.table_path = Path(table_path)
        if not self.table_path.exists():
            raise FileNotFoundError(f"Table path does not exist: {self.table_path}")
        self.dt = DeltaTable(str(self.table_path))

    @property
    def table_name(self) -> str:
        return self.table_path.name

    def get_stats(self) -> Dict[str, Any]:
        """Returns file count, total size, and version."""
        from urllib.parse import unquote
        self.dt = DeltaTable(str(self.table_path))
        files = self.dt.file_uris()
        total_bytes = 0
        for u in files:
            p = Path(unquote(u))
            if p.exists():
                total_bytes += p.stat().st_size
        return {
            "table_name": self.table_name,
            "version": self.dt.version(),
            "active_file_count": len(files),
            "total_size_bytes": total_bytes,
            "total_size_kb": round(total_bytes / 1024, 2),
        }

    def compact(self, target_size: Optional[int] = None) -> Dict[str, Any]:
        """
        Executes file compaction (OPTIMIZE).
        Consolidates small stream micro-batch files into larger, query-efficient files.
        """
        before_stats = self.get_stats()
        t0 = time.perf_counter()

        # Run compaction via DeltaTable optimize API
        res = self.dt.optimize.compact(target_size=target_size)
        elapsed_sec = time.perf_counter() - t0

        after_stats = self.get_stats()
        return {
            "table": self.table_name,
            "duration_seconds": round(elapsed_sec, 3),
            "files_before": before_stats["active_file_count"],
            "files_after": after_stats["active_file_count"],
            "new_version": after_stats["version"],
            "metrics": res,
        }

    def vacuum(self, retention_hours: int = DEFAULT_RETENTION_HOURS, dry_run: bool = False) -> Dict[str, Any]:
        """
        Executes safe VACUUM on Delta Lake table.
        Enforces safety threshold (retention_hours >= 168) unless dry_run is specified.
        """
        t0 = time.perf_counter()
        deleted_files = self.dt.vacuum(
            retention_hours=retention_hours,
            enforce_retention_duration=True,
            dry_run=dry_run,
        )
        elapsed_sec = time.perf_counter() - t0

        return {
            "table": self.table_name,
            "retention_hours": retention_hours,
            "dry_run": dry_run,
            "duration_seconds": round(elapsed_sec, 3),
            "files_deleted": len(deleted_files),
            "deleted_file_list": deleted_files,
        }


def optimize_all_lakehouse_tables(retention_hours: int = DEFAULT_RETENTION_HOURS) -> Dict[str, Any]:
    """Runs compaction and vacuum audit on all Lakehouse tables."""
    results = {}
    for path in [STAGING_PATH, QUARANTINE_PATH]:
        opt = LakehouseOptimizer(path)
        print(f"\n[OPTIMIZE] Optimizing table: {opt.table_name}...")
        compact_res = opt.compact()
        print(f"  Files before: {compact_res['files_before']} -> Files after: {compact_res['files_after']}")
        print(f"  Committed version: {compact_res['new_version']} (Elapsed: {compact_res['duration_seconds']}s)")

        print(f"[VACUUM] Executing safe vacuum (retention={retention_hours}h)...")
        vacuum_res = opt.vacuum(retention_hours=retention_hours, dry_run=False)
        print(f"  Vacuum completed in {vacuum_res['duration_seconds']}s (Pruned expired files: {vacuum_res['files_deleted']})")

        results[opt.table_name] = {
            "compaction": compact_res,
            "vacuum": vacuum_res,
        }
    return results


def main():
    parser = argparse.ArgumentParser(description="Delta Lake Table Optimization & Maintenance")
    parser.add_argument("--table", choices=["staging", "quarantine", "all"], default="all", help="Target table")
    parser.add_argument("--retention-hours", type=int, default=DEFAULT_RETENTION_HOURS, help="Safe vacuum retention hours")
    parser.add_argument("--dry-run-vacuum", action="store_true", help="Audit vacuum without file deletion")

    args = parser.parse_args()

    tables = []
    if args.table in ["staging", "all"]:
        tables.append(STAGING_PATH)
    if args.table in ["quarantine", "all"]:
        tables.append(QUARANTINE_PATH)

    for p in tables:
        opt = LakehouseOptimizer(p)
        print("=" * 80)
        print(f" OPTIMIZATION & MAINTENANCE FOR: {opt.table_name}")
        print("=" * 80)
        stats = opt.get_stats()
        print(f" Current State: {stats['active_file_count']} files, {stats['total_size_kb']} KB, Version: {stats['version']}")

        compact_res = opt.compact()
        print(f" Compaction: Files {compact_res['files_before']} -> {compact_res['files_after']} (in {compact_res['duration_seconds']}s)")

        vacuum_res = opt.vacuum(retention_hours=args.retention_hours, dry_run=args.dry_run_vacuum)
        print(f" Vacuum: Pruned {vacuum_res['files_deleted']} files (in {vacuum_res['duration_seconds']}s)")
        print("=" * 80)


if __name__ == "__main__":
    main()
