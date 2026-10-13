"""
Trino Query & Catalog Integration Client for Lakehouse Serving Layer.
Connects to Trino running on Docker (port 8082), registers the Delta Lake tables,
executes verification queries, runs analytics for Power BI, and provides benchmark utilities.
"""

import os
import sys
import time
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple

import pandas as pd
from trino.dbapi import connect
from trino.auth import BasicAuthentication

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))

TRINO_HOST = os.getenv("TRINO_HOST", "localhost")
TRINO_PORT = int(os.getenv("TRINO_PORT", "8082"))
TRINO_USER = os.getenv("TRINO_USER", "admin")
TRINO_CATALOG = os.getenv("TRINO_CATALOG", "delta")
TRINO_SCHEMA = os.getenv("TRINO_SCHEMA", "movie_lakehouse")


def get_trino_connection(catalog: Optional[str] = TRINO_CATALOG, schema: Optional[str] = TRINO_SCHEMA):
    """Returns a DBAPI connection to Trino."""
    return connect(
        host=TRINO_HOST,
        port=TRINO_PORT,
        user=TRINO_USER,
        catalog=catalog,
        schema=schema,
    )


def execute_trino_query(sql: str, params: Optional[Tuple] = None) -> Tuple[List[str], List[Tuple]]:
    """Executes a SQL query in Trino and returns (column_names, rows)."""
    with get_trino_connection() as conn:
        cur = conn.cursor()
        cur.execute(sql, params)
        rows = cur.fetchall()
        cols = [desc[0] for desc in cur.description] if cur.description else []
        return cols, rows


def execute_trino_df(sql: str) -> pd.DataFrame:
    """Executes SQL in Trino and returns a Pandas DataFrame."""
    cols, rows = execute_trino_query(sql)
    return pd.DataFrame(rows, columns=cols)


def init_trino_lakehouse_tables():
    """
    Registers the Delta Lake tables in Trino:
    1. CREATE SCHEMA IF NOT EXISTS delta.movie_lakehouse
    2. Register staging_events from /data/lakehouse/staging_events
    3. Register quarantine_events from /data/lakehouse/quarantine_events
    """
    with get_trino_connection(catalog="delta", schema=None) as conn:
        cur = conn.cursor()

        print("[Trino] Creating schema 'movie_lakehouse' in catalog 'delta'...")
        cur.execute(f"CREATE SCHEMA IF NOT EXISTS delta.{TRINO_SCHEMA}")
        cur.fetchall()

        # Check existing tables in schema
        cur.execute(f"SHOW TABLES IN delta.{TRINO_SCHEMA}")
        existing_tables = [row[0] for row in cur.fetchall()]
        print(f"[Trino] Existing registered tables: {existing_tables}")

        if "staging_events" not in existing_tables:
            print("[Trino] Registering table 'staging_events' from /data/lakehouse/staging_events...")
            cur.execute(
                f"CALL delta.system.register_table("
                f"schema_name => '{TRINO_SCHEMA}', "
                f"table_name => 'staging_events', "
                f"table_location => '/data/lakehouse/staging_events')"
            )
            cur.fetchall()
            print("[Trino] 'staging_events' registered.")
        else:
            print("[Trino] 'staging_events' is already registered.")

        if "quarantine_events" not in existing_tables:
            print("[Trino] Registering table 'quarantine_events' from /data/lakehouse/quarantine_events...")
            cur.execute(
                f"CALL delta.system.register_table("
                f"schema_name => '{TRINO_SCHEMA}', "
                f"table_name => 'quarantine_events', "
                f"table_location => '/data/lakehouse/quarantine_events')"
            )
            cur.fetchall()
            print("[Trino] 'quarantine_events' registered.")
        else:
            print("[Trino] 'quarantine_events' is already registered.")

    print("[Trino] Lakehouse tables initialization complete!")


def verify_trino_queries() -> Dict[str, Any]:
    """Runs verification queries against staging_events and quarantine_events."""
    results = {}

    print("\n--- Running Trino Verification Queries ---")
    df_staging_count = execute_trino_df(f"SELECT count(*) AS total_staging FROM delta.{TRINO_SCHEMA}.staging_events")
    staging_count = int(df_staging_count["total_staging"].iloc[0])
    results["staging_count"] = staging_count
    print(f"[*] Trino staging_events record count: {staging_count}")

    df_quarantine_count = execute_trino_df(f"SELECT count(*) AS total_quarantine FROM delta.{TRINO_SCHEMA}.quarantine_events")
    quarantine_count = int(df_quarantine_count["total_quarantine"].iloc[0])
    results["quarantine_count"] = quarantine_count
    print(f"[*] Trino quarantine_events record count: {quarantine_count}")

    print("\n--- Trino Sample Staging Events ---")
    df_sample = execute_trino_df(
        f"SELECT event_id, title, vote_average, rating_bracket, engagement_score, event_date "
        f"FROM delta.{TRINO_SCHEMA}.staging_events LIMIT 5"
    )
    print(df_sample.to_string(index=False))

    print("\n--- Trino Quarantine Breakdown by Rejection Reason ---")
    df_rejection = execute_trino_df(
        f"SELECT rejection_reason, count(*) AS failure_count "
        f"FROM delta.{TRINO_SCHEMA}.quarantine_events "
        f"GROUP BY rejection_reason ORDER BY failure_count DESC"
    )
    print(df_rejection.to_string(index=False))

    print("\n--- Trino Rating Bracket Engagement Aggregation ---")
    df_kpi = execute_trino_df(
        f"SELECT rating_bracket, count(*) as event_count, "
        f"round(avg(vote_average), 2) as avg_vote, "
        f"round(avg(engagement_score), 2) as avg_engagement "
        f"FROM delta.{TRINO_SCHEMA}.staging_events "
        f"GROUP BY rating_bracket ORDER BY event_count DESC"
    )
    print(df_kpi.to_string(index=False))

    return results


if __name__ == "__main__":
    init_trino_lakehouse_tables()
    verify_trino_queries()
