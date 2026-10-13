"""
Lakehouse Storage Catalog & Table Format Manager.
Integrates directly with Delta Lake catalog (org.apache.spark.sql.delta.catalog.DeltaCatalog),
providing operational DDL management, schema enforcement, table metadata,
physical partition discovery, and Write-Ahead Log (WAL) transaction history.
"""

import argparse
import os
import sys
from pathlib import Path
from typing import Any, Dict, List, Optional

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

from dotenv import load_dotenv
load_dotenv(PROJECT_ROOT / ".env")

try:
    from pyspark.sql import SparkSession
    from delta import configure_spark_with_delta_pip
    SPARK_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    SparkSession = None
    configure_spark_with_delta_pip = None
    SPARK_AVAILABLE = False

try:
    import deltalake
    DELTALAKE_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    DELTALAKE_AVAILABLE = False

LAKEHOUSE_BASE_DIR = PROJECT_ROOT / "data" / "lakehouse"
WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"
CLEAN_EVENTS_TABLE_PATH = LAKEHOUSE_BASE_DIR / "staging_events"
QUARANTINE_EVENTS_TABLE_PATH = LAKEHOUSE_BASE_DIR / "quarantine_events"

DATABASE_NAME = "movie_lakehouse"


def get_catalog_spark_session(app_name: str = "LakehouseCatalogManager") -> Optional[Any]:
    """
    Creates a SparkSession configured with the genuine Delta Lake catalog
    (DeltaSparkSessionExtension + DeltaCatalog) and persistent Hive/Derby Metastore support.
    Returns None if PySpark or JVM is unavailable.
    """
    if not SPARK_AVAILABLE:
        return None
    try:
        warehouse_dir = WAREHOUSE_DIR.as_posix()
        builder = (
            SparkSession.builder.appName(app_name)
            .master("local[*]")
            .config("spark.driver.host", "127.0.0.1")
            .config("spark.driver.bindAddress", "127.0.0.1")
            .config("spark.sql.shuffle.partitions", "2")
            .config("spark.ui.enabled", "false")
            .config("spark.sql.warehouse.dir", warehouse_dir)
            .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
            .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
            .enableHiveSupport()
        )
        if os.name == "nt" and os.path.exists(r"C:\hadoop"):
            builder = builder.config("spark.hadoop.home.dir", r"C:\hadoop")

        spark = configure_spark_with_delta_pip(builder).getOrCreate()
        spark.sparkContext.setLogLevel("WARN")
        return spark
    except Exception as e:
        print(f"[LakehouseCatalog] Notice: Could not initialize local SparkSession ({e}). Falling back to standalone metadata mode.")
        return None


class LakehouseCatalogManager:
    """
    Operational Delta Lake Table Catalog Manager.
    Uses Spark's native DeltaCatalog when available, or standalone deltalake/filesystem
    metadata inspection in lightweight container environments (such as Airflow workers).
    """

    def __init__(self, spark: Optional[Any] = None):
        self._spark = spark
        self.catalog_name = DATABASE_NAME
        self.clean_path = CLEAN_EVENTS_TABLE_PATH
        self.quarantine_path = QUARANTINE_EVENTS_TABLE_PATH

    @property
    def spark(self) -> Optional[Any]:
        if self._spark is None and SPARK_AVAILABLE:
            self._spark = get_catalog_spark_session()
        return self._spark

    def init_catalog(self):
        """
        Initializes database and registers Delta Lake tables in the catalog.
        - staging_events: partitioned by event_date
        - quarantine_events: partitioned by event_date (with rejection_reason preserved as column)
        """
        self.clean_path.mkdir(parents=True, exist_ok=True)
        self.quarantine_path.mkdir(parents=True, exist_ok=True)
        WAREHOUSE_DIR.mkdir(parents=True, exist_ok=True)

        if self.spark is not None:
            self.spark.sql(f"CREATE DATABASE IF NOT EXISTS {self.catalog_name}")

            staging_ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.staging_events (
                event_id STRING,
                event_type STRING,
                event_time TIMESTAMP,
                event_date DATE,
                event_hour INT,
                user_id STRING,
                session_id STRING,
                device STRING,
                country STRING,
                tmdb_id BIGINT,
                title STRING,
                genres STRING,
                popularity DOUBLE,
                vote_average DOUBLE,
                vote_count INT,
                release_date STRING,
                view_duration_seconds INT,
                user_rating DOUBLE,
                rating_bracket STRING,
                engagement_score DOUBLE,
                validation_status STRING,
                rejection_reason STRING,
                corrupt_record_raw STRING,
                kafka_partition INT,
                kafka_offset BIGINT,
                kafka_timestamp TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (event_date)
            LOCATION '{self.clean_path.as_posix()}'
            """

            quarantine_ddl = f"""
            CREATE TABLE IF NOT EXISTS {self.catalog_name}.quarantine_events (
                event_id STRING,
                event_type STRING,
                event_time TIMESTAMP,
                event_date DATE,
                event_hour INT,
                user_id STRING,
                session_id STRING,
                device STRING,
                country STRING,
                tmdb_id BIGINT,
                title STRING,
                genres STRING,
                popularity DOUBLE,
                vote_average DOUBLE,
                vote_count INT,
                release_date STRING,
                view_duration_seconds INT,
                user_rating DOUBLE,
                rating_bracket STRING,
                engagement_score DOUBLE,
                validation_status STRING,
                rejection_reason STRING,
                corrupt_record_raw STRING,
                kafka_partition INT,
                kafka_offset BIGINT,
                kafka_timestamp TIMESTAMP
            )
            USING DELTA
            PARTITIONED BY (event_date)
            LOCATION '{self.quarantine_path.as_posix()}'
            """

            self.spark.sql(staging_ddl)
            self.spark.sql(quarantine_ddl)
            print(f"[Catalog] Successfully synchronized Hive/Spark metastore catalog for {self.catalog_name}.")
        else:
            print(f"[Catalog] Standalone filesystem catalog initialized at {LAKEHOUSE_BASE_DIR}.")
            print(f"  |-- staging_events: {self.clean_path}")
            print(f"  |-- quarantine_events: {self.quarantine_path}")

    def list_tables(self) -> List[str]:
        """Returns list of registered table names in the catalog."""
        if self.spark is not None:
            tables_df = self.spark.sql(f"SHOW TABLES IN {self.catalog_name}")
            return [row["tableName"] for row in tables_df.collect()]
        
        tables = []
        if (self.clean_path / "_delta_log").exists():
            tables.append("staging_events")
        if (self.quarantine_path / "_delta_log").exists():
            tables.append("quarantine_events")
        return tables

    def describe_table(self, table_name: str):
        """Describes schema and extended metadata of a table."""
        full_name = self._resolve_table_name(table_name)
        if self.spark is not None:
            return self.spark.sql(f"DESCRIBE TABLE EXTENDED {full_name}")

        target_path = self.clean_path if "staging" in table_name.lower() else self.quarantine_path
        if DELTALAKE_AVAILABLE and (target_path / "_delta_log").exists():
            dt = deltalake.DeltaTable(target_path.as_posix())
            meta = dt.metadata()
            print("=" * 80)
            print(f" EXTENDED TABLE SPECIFICATION (deltalake): {table_name}")
            print("=" * 80)
            print(f"Table Location:     {target_path}")
            print(f"Partition Columns:  {meta.partition_columns}")
            print(f"Current Version:    {dt.version()}")
            print("\nSchema:")
            for field in dt.schema().fields:
                print(f"  |-- {field.name}: {field.type}")
        return None

    def get_table_history(self, table_name: str):
        """
        Executes genuine native Delta Lake DESCRIBE HISTORY command.
        """
        full_name = self._resolve_table_name(table_name)
        if self.spark is not None:
            return self.spark.sql(f"DESCRIBE HISTORY {full_name}")

        target_path = self.clean_path if "staging" in table_name.lower() else self.quarantine_path
        if DELTALAKE_AVAILABLE and (target_path / "_delta_log").exists():
            dt = deltalake.DeltaTable(target_path.as_posix())
            return dt.history()
        return []

    def discover_partitions(self, table_name: str) -> List[str]:
        """
        Discovers physical partition directories on disk.
        """
        target_path = self.clean_path if "staging" in table_name.lower() else self.quarantine_path
        if not target_path.exists():
            return []
        partitions = []
        for p in sorted(target_path.glob("event_date=*")):
            if p.is_dir():
                parquet_files = list(p.glob("*.parquet"))
                file_count = len(parquet_files)
                size_kb = sum(f.stat().st_size for f in parquet_files) / 1024
                partitions.append(f"{p.name} ({file_count} files, {size_kb:.1f} KB)")
        return partitions

    def generate_trino_schema_ddl(self) -> str:
        """
        Generates standard Delta Lake Trino / Presto DDL for serving layer.
        """
        return f"""-- ==============================================================================
-- TRINO / PRESTO DELTA LAKE SERVING LAYER DDL SPECIFICATION
-- Database: {self.catalog_name}
-- ==============================================================================

CREATE SCHEMA IF NOT EXISTS delta.{self.catalog_name};

CREATE TABLE IF NOT EXISTS delta.{self.catalog_name}.staging_events (
    event_id VARCHAR,
    event_type VARCHAR,
    event_time TIMESTAMP,
    event_date DATE,
    event_hour INTEGER,
    user_id VARCHAR,
    session_id VARCHAR,
    device VARCHAR,
    country VARCHAR,
    tmdb_id BIGINT,
    title VARCHAR,
    genres VARCHAR,
    popularity DOUBLE,
    vote_average DOUBLE,
    vote_count INTEGER,
    release_date VARCHAR,
    view_duration_seconds INTEGER,
    user_rating DOUBLE,
    rating_bracket VARCHAR,
    engagement_score DOUBLE,
    validation_status VARCHAR,
    rejection_reason VARCHAR,
    corrupt_record_raw VARCHAR,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP
)
WITH (
    location = '{self.clean_path.as_posix()}',
    partitioned_by = ARRAY['event_date']
);

CREATE TABLE IF NOT EXISTS delta.{self.catalog_name}.quarantine_events (
    event_id VARCHAR,
    event_type VARCHAR,
    event_time TIMESTAMP,
    event_date DATE,
    event_hour INTEGER,
    user_id VARCHAR,
    session_id VARCHAR,
    device VARCHAR,
    country VARCHAR,
    tmdb_id BIGINT,
    title VARCHAR,
    genres VARCHAR,
    popularity DOUBLE,
    vote_average DOUBLE,
    vote_count INTEGER,
    release_date VARCHAR,
    view_duration_seconds INTEGER,
    user_rating DOUBLE,
    rating_bracket VARCHAR,
    engagement_score DOUBLE,
    validation_status VARCHAR,
    rejection_reason VARCHAR,
    corrupt_record_raw VARCHAR,
    kafka_partition INTEGER,
    kafka_offset BIGINT,
    kafka_timestamp TIMESTAMP
)
WITH (
    location = '{self.quarantine_path.as_posix()}',
    partitioned_by = ARRAY['event_date']
);
"""

    def _resolve_table_name(self, name: str) -> str:
        if "." in name:
            return name
        return f"{self.catalog_name}.{name}"

    def close(self):
        if self._spark is not None:
            try:
                self._spark.stop()
            except Exception:
                pass
            self._spark = None


def configure_default_lakehouse_catalog(spark: Optional[Any] = None) -> LakehouseCatalogManager:
    """Helper to initialize and return the Lakehouse catalog manager."""
    mgr = LakehouseCatalogManager(spark)
    mgr.init_catalog()
    return mgr


def main():
    parser = argparse.ArgumentParser(description="Delta Lake Operational Table Catalog Manager")
    parser.add_argument("--init", action="store_true", help="Initialize database and Delta tables in catalog")
    parser.add_argument("--list-tables", action="store_true", help="List registered catalog tables")
    parser.add_argument("--describe", type=str, help="Describe table schema, extended properties, and location")
    parser.add_argument("--history", type=str, help="Display native Delta Lake transaction history (DESCRIBE HISTORY)")
    parser.add_argument("--partitions", nargs="?", const="all", default=None, help="Discover physical partition folders")
    parser.add_argument("--export-trino", action="store_true", help="Export Trino DDL for Lakehouse serving layer")
    parser.add_argument("--show", nargs="?", const="all", default=None, help="Display table data and metrics")

    args = parser.parse_args()
    catalog_mgr = LakehouseCatalogManager()

    if args.init:
        catalog_mgr.init_catalog()
        print("=" * 80)
        print(" DELTA LAKE TABLE CATALOG INITIALIZED")
        print(f" Database: {catalog_mgr.catalog_name}")
        print(" Tables:   staging_events, quarantine_events")
        print(" Provider: Delta Lake")
        print("=" * 80)

    if args.list_tables:
        catalog_mgr.init_catalog()
        tables = catalog_mgr.list_tables()
        print("=" * 80)
        print(f" REGISTERED TABLES IN CATALOG: {catalog_mgr.catalog_name}")
        print("=" * 80)
        for t in tables:
            print(f"  |-- {catalog_mgr.catalog_name}.{t}")

    if args.describe:
        desc_df = catalog_mgr.describe_table(args.describe)
        if desc_df is not None:
            print("=" * 80)
            print(f" EXTENDED TABLE SPECIFICATION: {args.describe}")
            print("=" * 80)
            desc_df.show(50, truncate=False)

    if args.history:
        try:
            hist = catalog_mgr.get_table_history(args.history)
            print("=" * 80)
            print(f" DELTA LAKE TRANSACTION HISTORY (DESCRIBE HISTORY): {args.history}")
            print("=" * 80)
            if hasattr(hist, "select"):
                hist.select("version", "timestamp", "operation", "operationParameters").show(20, truncate=False)
            elif isinstance(hist, list):
                print(f"{'Version':<10} {'Timestamp (UTC)':<26} {'Operation':<22} {'Parameters'}")
                print("-" * 80)
                for commit in hist[:20]:
                    v = commit.get("version", "N/A")
                    ts = commit.get("timestamp", "N/A")
                    if isinstance(ts, (int, float)):
                        from datetime import datetime, timezone
                        ts = datetime.fromtimestamp(ts / 1000, tz=timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
                    op = commit.get("operation", "UNKNOWN")
                    params = str(commit.get("operationParameters", ""))
                    if len(params) > 40:
                        params = params[:37] + "..."
                    print(f"{str(v):<10} {str(ts):<26} {str(op):<22} {params}")
        except Exception as e:
            print(f"Error fetching Delta history for '{args.history}': {e}")

    if args.partitions:
        tables = ["staging_events", "quarantine_events"] if args.partitions in ["all", None] else [args.partitions]
        for t in tables:
            parts = catalog_mgr.discover_partitions(t)
            print("=" * 80)
            print(f" DISCOVERED PARTITIONS FOR: {t} ({len(parts)} partitions)")
            print("=" * 80)
            for p in parts:
                print(f"  |-- {p}")

    if args.export_trino:
        print(catalog_mgr.generate_trino_schema_ddl())

    if args.show:
        try:
            from pipeline.show_lakehouse import display_staging_table, display_quarantine_table
            target = args.show.lower()
            if target in ["staging", "staging_events", "all"]:
                display_staging_table(limit=10)
            if target in ["quarantine", "quarantine_events", "all"]:
                display_quarantine_table(limit=10)
        except Exception as e:
            print(f"Error displaying lakehouse table: {e}")

    catalog_mgr.close()


if __name__ == "__main__":
    main()

