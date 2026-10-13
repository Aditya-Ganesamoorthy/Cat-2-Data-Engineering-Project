"""
Automated Verification Suite for CAT-2 Week 3 & Week 4.
Verifies:
  1. Trino serving layer connectivity and query results (Week 3)
  2. Delta Lake table compaction (OPTIMIZE) and safe vacuuming (Week 4)
  3. Power BI datasets export integrity (Week 3)
  4. Airflow maintenance and optimization DAG integrity (Week 4)
"""

import os
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.trino_client import execute_trino_df, TRINO_SCHEMA, verify_trino_queries
from pipeline.lakehouse_optimizer import LakehouseOptimizer, STAGING_PATH, QUARANTINE_PATH
from analytics.export_lakehouse_for_powerbi import export_powerbi_datasets, EXPORT_DIR


def test_trino_serving():
    print("\n[Test 1/4] Verifying Trino Serving Layer (delta.movie_lakehouse)...")
    res = verify_trino_queries()
    assert res["staging_count"] > 0, "Staging count in Trino must be > 0"
    assert res["quarantine_count"] > 0, "Quarantine count in Trino must be > 0"
    print("  [PASS] Trino queries returned valid Delta table rows.")
    return True


def test_delta_optimization():
    print("\n[Test 2/4] Verifying Delta Lake Compaction & Vacuum...")
    staging_opt = LakehouseOptimizer(STAGING_PATH)
    compact_res = staging_opt.compact()
    vacuum_res = staging_opt.vacuum(retention_hours=168, dry_run=False)

    assert compact_res["files_after"] > 0, "Compacted files must be > 0"
    assert vacuum_res["retention_hours"] == 168, "Vacuum retention must enforce 168 hours safe window"
    print(f"  [PASS] Compaction and Vacuum executed cleanly (active files: {compact_res['files_after']}).")
    return True


def test_powerbi_exports():
    print("\n[Test 3/4] Verifying Power BI Analytic Exports...")
    export_powerbi_datasets()
    staging_csv = EXPORT_DIR / "cat2_staging_events.csv"
    quarantine_csv = EXPORT_DIR / "cat2_quarantine_events.csv"
    kpi_csv = EXPORT_DIR / "cat2_kpi_summary.csv"

    assert staging_csv.exists() and staging_csv.stat().st_size > 0
    assert quarantine_csv.exists() and quarantine_csv.stat().st_size > 0
    assert kpi_csv.exists() and kpi_csv.stat().st_size > 0
    print("  [PASS] Power BI snapshot datasets verified on disk.")
    return True


def test_airflow_dags():
    print("\n[Test 4/4] Verifying Airflow DAG Files...")
    dag_path = PROJECT_ROOT / "airflow" / "dags" / "lakehouse_optimization_dag.py"
    assert dag_path.exists(), "lakehouse_optimization_dag.py must exist"

    # Compile check
    with open(dag_path, "r", encoding="utf-8") as f:
        code = f.read()
    compile(code, str(dag_path), "exec")
    print("  [PASS] Airflow optimization DAG syntax compiled with zero errors.")
    return True


def run_all_tests():
    print("=" * 85)
    print(" CAT-2 WEEK 3 & WEEK 4 VERIFICATION TEST SUITE")
    print("=" * 85)
    t1 = test_trino_serving()
    t2 = test_delta_optimization()
    t3 = test_powerbi_exports()
    t4 = test_airflow_dags()

    all_pass = t1 and t2 and t3 and t4
    print("\n" + "=" * 85)
    print(f" FINAL STATUS: {'ALL TESTS PASSED (PASS)' if all_pass else 'SOME TESTS FAILED'}")
    print("=" * 85)
    return all_pass


if __name__ == "__main__":
    success = run_all_tests()
    sys.exit(0 if success else 1)
