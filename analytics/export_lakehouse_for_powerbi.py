"""
Exports Lakehouse Data Views from Trino for Power BI Integration.
Generates snapshot CSV datasets for Power BI import if direct Trino connection is not used.
"""

from pathlib import Path
import pandas as pd

import sys
PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

from pipeline.trino_client import execute_trino_df, TRINO_SCHEMA

EXPORT_DIR = PROJECT_ROOT / "data" / "analytics_exports"


def export_powerbi_datasets():
    EXPORT_DIR.mkdir(parents=True, exist_ok=True)

    print("[*] Exporting Trino Lakehouse datasets for Power BI...")

    # 1. Clean Staging Events
    df_staging = execute_trino_df(f"""
        SELECT 
            event_id,
            event_type,
            event_date,
            event_hour,
            user_id,
            device,
            country,
            tmdb_id,
            title,
            genres,
            popularity,
            vote_average,
            vote_count,
            view_duration_seconds,
            user_rating,
            rating_bracket,
            engagement_score,
            validation_status
        FROM delta.{TRINO_SCHEMA}.staging_events
    """)
    staging_file = EXPORT_DIR / "cat2_staging_events.csv"
    df_staging.to_csv(staging_file, index=False)
    print(f"  [+] Saved Staging Events: {staging_file} ({len(df_staging)} rows)")

    # 2. Quarantined Events
    df_quarantine = execute_trino_df(f"""
        SELECT 
            event_id,
            event_type,
            event_date,
            rejection_reason,
            tmdb_id,
            title,
            vote_average,
            corrupt_record_raw
        FROM delta.{TRINO_SCHEMA}.quarantine_events
    """)
    quarantine_file = EXPORT_DIR / "cat2_quarantine_events.csv"
    df_quarantine.to_csv(quarantine_file, index=False)
    print(f"  [+] Saved Quarantine Events: {quarantine_file} ({len(df_quarantine)} rows)")

    # 3. Aggregated KPIs by Rating Bracket & Title
    df_kpi = execute_trino_df(f"""
        SELECT 
            rating_bracket,
            count(*) AS total_views,
            round(avg(vote_average), 2) AS avg_vote,
            round(avg(engagement_score), 2) AS avg_engagement_score,
            round(max(engagement_score), 2) AS peak_engagement_score
        FROM delta.{TRINO_SCHEMA}.staging_events
        GROUP BY rating_bracket
    """)
    kpi_file = EXPORT_DIR / "cat2_kpi_summary.csv"
    df_kpi.to_csv(kpi_file, index=False)
    print(f"  [+] Saved KPI Summary: {kpi_file} ({len(df_kpi)} rows)")

    print("[*] All Power BI exports generated successfully!")


if __name__ == "__main__":
    export_powerbi_datasets()
