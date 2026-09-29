import argparse
import datetime
import json
import os
import sys
import uuid
from pathlib import Path
from typing import Any, Dict, List, Optional

from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(PROJECT_ROOT))
load_dotenv(PROJECT_ROOT / ".env")

LAKEHOUSE_BASE_DIR = PROJECT_ROOT / "data" / "lakehouse"
CATALOG_METADATA_DIR = LAKEHOUSE_BASE_DIR / "_catalog_metadata"
CLEAN_EVENTS_TABLE_PATH = LAKEHOUSE_BASE_DIR / "staging_events"
QUARANTINE_EVENTS_TABLE_PATH = LAKEHOUSE_BASE_DIR / "quarantine"
WAREHOUSE_DIR = PROJECT_ROOT / "data" / "warehouse"

DEFAULT_CATALOG_TYPE = os.getenv("LAKEHOUSE_CATALOG_TYPE", "delta").lower()
HIVE_METASTORE_URI = os.getenv("HIVE_METASTORE_URI", "thrift://localhost:9083")
NESSIE_URI = os.getenv("NESSIE_URI", "http://localhost:19120/api/v1")


def get_staging_events_schema() -> Dict[str, Any]:
    """
    Returns canonical schema definition for Lakehouse Clean Staging Events table.
    """
    return {
        "fields": [
            {"name": "event_id", "type": "string", "nullable": False, "metadata": {"comment": "Unique event UUID"}},
            {"name": "event_type", "type": "string", "nullable": False, "metadata": {"comment": "Stream event category"}},
            {"name": "event_time", "type": "timestamp", "nullable": False, "metadata": {"comment": "Event generation timestamp"}},
            {"name": "event_date", "type": "date", "nullable": False, "metadata": {"comment": "Partition key (YYYY-MM-DD)"}},
            {"name": "event_hour", "type": "integer", "nullable": True, "metadata": {"comment": "Event hour (0-23)"}},
            {"name": "user_id", "type": "string", "nullable": True, "metadata": {"comment": "User identifier"}},
            {"name": "session_id", "type": "string", "nullable": True, "metadata": {"comment": "User session UUID"}},
            {"name": "device", "type": "string", "nullable": True, "metadata": {"comment": "Client device"}},
            {"name": "country", "type": "string", "nullable": True, "metadata": {"comment": "Country code (ISO 3166-1)"}},
            {"name": "tmdb_id", "type": "long", "nullable": False, "metadata": {"comment": "TMDB movie reference ID"}},
            {"name": "title", "type": "string", "nullable": True, "metadata": {"comment": "Movie title"}},
            {"name": "genres", "type": "string", "nullable": True, "metadata": {"comment": "Comma-separated genres"}},
            {"name": "popularity", "type": "double", "nullable": True, "metadata": {"comment": "Popularity metric"}},
            {"name": "vote_average", "type": "double", "nullable": True, "metadata": {"comment": "Audience vote average (0-10)"}},
            {"name": "vote_count", "type": "integer", "nullable": True, "metadata": {"comment": "Number of ratings"}},
            {"name": "release_date", "type": "string", "nullable": True, "metadata": {"comment": "Theatrical release date"}},
            {"name": "view_duration_seconds", "type": "integer", "nullable": True, "metadata": {"comment": "Watch duration"}},
            {"name": "user_rating", "type": "double", "nullable": True, "metadata": {"comment": "Individual rating score"}},
            {"name": "rating_bracket", "type": "string", "nullable": True, "metadata": {"comment": "Masterpiece/High/Medium/Low"}},
            {"name": "engagement_score", "type": "double", "nullable": True, "metadata": {"comment": "Duration * Rating score"}},
            {"name": "validation_status", "type": "string", "nullable": False, "metadata": {"comment": "Quality status: VALID"}},
            {"name": "rejection_reason", "type": "string", "nullable": True, "metadata": {"comment": "Validation rule verdict"}},
            {"name": "corrupt_record_raw", "type": "string", "nullable": True, "metadata": {"comment": "Raw corrupt payload (if any)"}},
            {"name": "kafka_partition", "type": "integer", "nullable": True, "metadata": {"comment": "Kafka source partition"}},
            {"name": "kafka_offset", "type": "long", "nullable": True, "metadata": {"comment": "Kafka source offset"}},
            {"name": "kafka_timestamp", "type": "timestamp", "nullable": True, "metadata": {"comment": "Kafka broker timestamp"}},
        ]
    }


def get_quarantine_events_schema() -> Dict[str, Any]:
    """
    Returns canonical schema definition for Lakehouse Quarantine (DLQ) table.
    """
    return {
        "fields": [
            {"name": "event_id", "type": "string", "nullable": True, "metadata": {"comment": "Event ID if parsed"}},
            {"name": "tmdb_id", "type": "long", "nullable": True, "metadata": {"comment": "Movie ID if present"}},
            {"name": "validation_status", "type": "string", "nullable": False, "metadata": {"comment": "Status: QUARANTINE"}},
            {"name": "rejection_reason", "type": "string", "nullable": False, "metadata": {"comment": "Violation rule code"}},
            {"name": "corrupt_record_raw", "type": "string", "nullable": True, "metadata": {"comment": "Original unparseable JSON"}},
            {"name": "event_date", "type": "date", "nullable": True, "metadata": {"comment": "Partition date"}},
            {"name": "kafka_partition", "type": "integer", "nullable": True, "metadata": {"comment": "Source partition"}},
            {"name": "kafka_offset", "type": "long", "nullable": True, "metadata": {"comment": "Source offset"}},
            {"name": "kafka_timestamp", "type": "timestamp", "nullable": True, "metadata": {"comment": "Source timestamp"}},
        ]
    }


class LakehouseCatalogManager:
    """
    Lakehouse Storage Catalog & Table Format Manager.
    Configures and maintains Lakehouse tables (Delta Lake / Iceberg specification),
    Write-Ahead Logging (WAL) transaction logs, schema evolution, and partition catalogs.
    Compatible with Hive Metastore, Nessie, and AWS Glue catalog specifications.
    """

    def __init__(
        self,
        catalog_name: str = "movie_lakehouse",
        catalog_type: str = DEFAULT_CATALOG_TYPE,
        base_dir: Path = LAKEHOUSE_BASE_DIR,
    ):
        self.catalog_name = catalog_name
        self.catalog_type = catalog_type
        self.base_dir = Path(base_dir)
        self.metadata_dir = self.base_dir / "_catalog_metadata"
        self.metadata_dir.mkdir(parents=True, exist_ok=True)
        self.catalog_file = self.metadata_dir / f"{self.catalog_name}_catalog.json"
        self._initialize_catalog()

    def _initialize_catalog(self):
        """Initialize catalog registry if not already present."""
        if not self.catalog_file.exists():
            catalog_state = {
                "catalog_name": self.catalog_name,
                "catalog_type": self.catalog_type,
                "version": "2.0.0",
                "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "updated_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
                "endpoints": {
                    "hive_metastore": HIVE_METASTORE_URI,
                    "nessie": NESSIE_URI,
                    "local_warehouse": str(WAREHOUSE_DIR),
                },
                "tables": {},
            }
            self._save_catalog(catalog_state)

    def _load_catalog(self) -> Dict[str, Any]:
        if not self.catalog_file.exists():
            self._initialize_catalog()
        with open(self.catalog_file, "r", encoding="utf-8") as f:
            return json.load(f)

    def _save_catalog(self, data: Dict[str, Any]):
        data["updated_at"] = datetime.datetime.now(datetime.timezone.utc).isoformat()
        with open(self.catalog_file, "w", encoding="utf-8") as f:
            json.dump(data, f, indent=2, default=str)

    def register_table(
        self,
        table_name: str,
        table_path: Path,
        table_format: str,
        partition_columns: List[str],
        schema: Dict[str, Any],
        description: str = "",
    ) -> Dict[str, Any]:
        """
        Register a Lakehouse table in the catalog registry.
        """
        table_path = Path(table_path)
        wal_dir = table_path / "_delta_log"
        wal_dir.mkdir(parents=True, exist_ok=True)

        catalog = self._load_catalog()
        table_entry = {
            "table_name": table_name,
            "full_name": f"{self.catalog_name}.{table_name}",
            "location": str(table_path.resolve()),
            "format": table_format,
            "partition_columns": partition_columns,
            "schema": schema,
            "description": description,
            "wal_directory": str(wal_dir.resolve()),
            "registered_at": datetime.datetime.now(datetime.timezone.utc).isoformat(),
            "properties": {
                "delta.appendOnly": "true",
                "delta.autoOptimize.optimizeWrite": "true",
                "delta.autoOptimize.autoCompact": "true",
                "write.format.default": "parquet",
            },
        }

        catalog["tables"][table_name] = table_entry
        self._save_catalog(catalog)
        return table_entry

    def record_wal_commit(
        self,
        table_name: str,
        files_added: List[Dict[str, Any]],
        num_records: int,
        batch_id: int,
        operation: str = "STREAMING UPDATE",
    ) -> Path:
        """
        Append an atomic Write-Ahead Log (WAL) commit to the Lakehouse transaction log.
        Emulates Delta Lake / Iceberg transaction commit protocols.
        """
        catalog = self._load_catalog()
        if table_name not in catalog["tables"]:
            raise ValueError(f"Table '{table_name}' is not registered in catalog '{self.catalog_name}'")

        table_info = catalog["tables"][table_name]
        wal_dir = Path(table_info["wal_directory"])
        wal_dir.mkdir(parents=True, exist_ok=True)

        # Determine next commit version number
        existing_commits = sorted(wal_dir.glob("*.json"))
        version = len(existing_commits)
        commit_filename = f"{version:020d}.json"
        commit_file_path = wal_dir / commit_filename

        now_utc = datetime.datetime.now(datetime.timezone.utc)
        now_ts = int(now_utc.timestamp() * 1000)

        wal_payload = {
            "commitInfo": {
                "timestamp": now_ts,
                "inCommitTimestamp": now_ts,
                "userId": "streaming_engine",
                "userName": "SparkStructuredStreaming",
                "operation": operation,
                "operationParameters": {
                    "outputMode": "Append",
                    "queryId": str(uuid.uuid4()),
                    "epochId": str(batch_id),
                },
                "readVersion": version - 1 if version > 0 else None,
                "isolationLevel": "Serializable",
                "isBlindAppend": True,
                "engineInfo": "PySpark/4.2.0 Lakehouse-Delta-Writer",
                "txnId": str(uuid.uuid4()),
            },
            "protocol": {
                "minReaderVersion": 1,
                "minWriterVersion": 2,
            },
            "metaData": {
                "id": str(uuid.uuid5(uuid.NAMESPACE_DNS, f"{self.catalog_name}.{table_name}")),
                "format": {"provider": "parquet", "options": {}},
                "schemaString": json.dumps(table_info["schema"]),
                "partitionColumns": table_info["partition_columns"],
                "configuration": table_info.get("properties", {}),
                "createdTime": now_ts,
            },
            "add_actions": [
                {
                    "path": f.get("relative_path", str(f.get("path", ""))),
                    "partitionValues": f.get("partitionValues", {}),
                    "size": f.get("size_bytes", 0),
                    "modificationTime": now_ts,
                    "dataChange": True,
                    "stats": json.dumps({
                        "numRecords": f.get("records", num_records),
                    }),
                }
                for f in files_added
            ],
            "commit_summary": {
                "version": version,
                "records_committed": num_records,
                "files_count": len(files_added),
                "timestamp_iso": now_utc.isoformat(),
            },
        }

        with open(commit_file_path, "w", encoding="utf-8") as f:
            json.dump(wal_payload, f, indent=2)

        return commit_file_path

    def get_table_history(self, table_name: str) -> List[Dict[str, Any]]:
        """
        Read the Write-Ahead Log (WAL) audit trail for a table (equivalent to DESCRIBE HISTORY).
        """
        catalog = self._load_catalog()
        if table_name not in catalog["tables"]:
            raise ValueError(f"Table '{table_name}' not registered in catalog")

        table_info = catalog["tables"][table_name]
        wal_dir = Path(table_info["wal_directory"])
        commits = sorted(wal_dir.glob("*.json"))

        history = []
        for c in commits:
            try:
                with open(c, "r", encoding="utf-8") as f:
                    entry = json.load(f)
                    summary = entry.get("commit_summary", {})
                    info = entry.get("commitInfo", {})
                    history.append({
                        "version": summary.get("version", int(c.stem)),
                        "timestamp": summary.get("timestamp_iso", datetime.datetime.fromtimestamp(info.get("timestamp", 0) / 1000).isoformat()),
                        "operation": info.get("operation", "UNKNOWN"),
                        "records_added": summary.get("records_committed", 0),
                        "files_added": summary.get("files_count", len(entry.get("add_actions", []))),
                        "isolationLevel": info.get("isolationLevel", "Serializable"),
                        "commit_file": c.name,
                    })
            except Exception as e:
                history.append({"commit_file": c.name, "error": str(e)})

        return history

    def discover_partitions(self, table_name: str) -> List[str]:
        """
        Discover active physical partitions for a Lakehouse table.
        """
        catalog = self._load_catalog()
        if table_name not in catalog["tables"]:
            return []

        table_path = Path(catalog["tables"][table_name]["location"])
        if not table_path.exists():
            return []

        partitions = []
        for item in table_path.iterdir():
            if item.is_dir() and "=" in item.name and not item.name.startswith(("_", ".")):
                partitions.append(item.name)
        return sorted(partitions)

    def generate_trino_schema_ddl(self) -> str:
        """
        Generate Trino Lakehouse catalog and table DDL for downstream querying (Week 3/4 serving layer).
        """
        catalog = self._load_catalog()
        ddl_lines = [
            f"-- Trino Serving Layer Catalog DDL for {self.catalog_name}",
            f"CREATE SCHEMA IF NOT EXISTS delta.{self.catalog_name};",
            "",
        ]

        type_mapping = {
            "string": "VARCHAR",
            "long": "BIGINT",
            "integer": "INTEGER",
            "double": "DOUBLE",
            "timestamp": "TIMESTAMP(3)",
            "date": "DATE",
            "boolean": "BOOLEAN",
        }

        for tname, tinfo in catalog["tables"].items():
            loc = tinfo["location"].replace("\\", "/")
            fields = tinfo["schema"]["fields"]
            cols = []
            for f in fields:
                tt = type_mapping.get(f["type"], "VARCHAR")
                cols.append(f"    {f['name']} {tt}")

            cols_str = ",\n".join(cols)
            partition_props = ""
            if tinfo.get("partition_columns"):
                part_cols = ", ".join(f"'{c}'" for c in tinfo["partition_columns"])
                partition_props = f",\n    partitioned_by = ARRAY[{part_cols}]"

            ddl_lines.append(f"CREATE TABLE IF NOT EXISTS delta.{self.catalog_name}.{tname} (")
            ddl_lines.append(cols_str)
            ddl_lines.append(")")
            ddl_lines.append("WITH (")
            ddl_lines.append(f"    location = '{loc}'{partition_props}")
            ddl_lines.append(");")
            ddl_lines.append("")

        return "\n".join(ddl_lines)


def configure_default_lakehouse_catalog() -> LakehouseCatalogManager:
    """
    Ensure the standard default Lakehouse tables (staging_events and quarantine)
    are registered in the Lakehouse catalog with proper schema & partitioning.
    """
    manager = LakehouseCatalogManager("movie_lakehouse")

    # 1. Clean Staging Events Table (Partitioned by event_date)
    manager.register_table(
        table_name="staging_events",
        table_path=CLEAN_EVENTS_TABLE_PATH,
        table_format="delta",
        partition_columns=["event_date"],
        schema=get_staging_events_schema(),
        description="Clean, validated real-time movie streaming events partitioned by event_date with WAL",
    )

    # 2. Quarantine Dead-Letter Queue (Partitioned by rejection_reason)
    manager.register_table(
        table_name="quarantine_events",
        table_path=QUARANTINE_EVENTS_TABLE_PATH,
        table_format="delta",
        partition_columns=["rejection_reason"],
        schema=get_quarantine_events_schema(),
        description="Quarantine Dead-Letter Queue (DLQ) for corrupt/invalid payloads with schema enforcement WAL",
    )

    return manager


def main():
    parser = argparse.ArgumentParser(description="Lakehouse Table Catalog Manager (Delta Lake / Iceberg)")
    parser.add_argument("--init", action="store_true", help="Initialize and configure default catalog tables")
    parser.add_argument("--list-tables", action="store_true", help="List registered catalog tables")
    parser.add_argument("--describe", type=str, help="Describe table schema, partitions, and location")
    parser.add_argument("--show", nargs="?", const="all", default=None, help="Query and display Lakehouse table records, statistics, and analytics (staging, quarantine, or all)")
    parser.add_argument("--history", type=str, help="Display Write-Ahead Log (WAL) transaction history for a table")
    parser.add_argument("--partitions", nargs="?", const="all", default=None, help="List discovered physical partition paths for a table (or all)")
    parser.add_argument("--export-trino", action="store_true", help="Export Trino DDL for Lakehouse serving layer")

    args = parser.parse_args()
    catalog_mgr = configure_default_lakehouse_catalog()

    if args.show:
        try:
            from pipeline.show_lakehouse import display_staging_table, display_quarantine_table
            target = args.show.lower()
            if target in ["staging", "staging_events", "movie_lakehouse.staging_events", "all"]:
                display_staging_table(limit=10)
            if target in ["quarantine", "quarantine_events", "movie_lakehouse.quarantine_events", "all"]:
                display_quarantine_table(limit=10)
        except Exception as e:
            print(f"Error displaying lakehouse table: {e}")

    if args.list_tables or args.init:
        catalog = catalog_mgr._load_catalog()
        print("=" * 80)
        print(f" LAKEHOUSE TABLE CATALOG: {catalog['catalog_name']} ({catalog['catalog_type'].upper()})")
        print(f" Catalog Location: {catalog_mgr.catalog_file}")
        print("=" * 80)
        for tname, tinfo in catalog["tables"].items():
            print(f"\n Table: {tinfo['full_name']}")
            print(f"   Format:             {tinfo['format']}")
            print(f"   Location:           {tinfo['location']}")
            print(f"   WAL Directory:      {tinfo['wal_directory']}")
            print(f"   Partition Columns:  {tinfo['partition_columns']}")
            print(f"   Description:        {tinfo['description']}")

    if args.describe:
        catalog = catalog_mgr._load_catalog()
        if args.describe in catalog["tables"]:
            tinfo = catalog["tables"][args.describe]
            print("=" * 80)
            print(f" SCHEMA SPECIFICATION FOR TABLE: {args.describe}")
            print("=" * 80)
            print(f"{'Field Name':<25} {'Type':<15} {'Nullable':<10} {'Description'}")
            print("-" * 80)
            for f in tinfo["schema"]["fields"]:
                comment = f.get("metadata", {}).get("comment", "")
                print(f"{f['name']:<25} {f['type']:<15} {str(f['nullable']):<10} {comment}")
        else:
            print(f"Table '{args.describe}' not found in catalog.")

    if args.history:
        try:
            hist = catalog_mgr.get_table_history(args.history)
            print("=" * 80)
            print(f" WRITE-AHEAD LOG (WAL) COMMIT AUDIT HISTORY: {args.history}")
            print("=" * 80)
            print(f"{'Version':<10} {'Timestamp':<28} {'Operation':<22} {'Records':<10} {'Files'}")
            print("-" * 80)
            for h in hist:
                print(f"{h.get('version', '-'):<10} {h.get('timestamp', '-'):<28} {h.get('operation', '-'):<22} {h.get('records_added', 0):<10} {h.get('files_added', 0)}")
            print("=" * 80)
        except Exception as e:
            print(f"Error fetching history for '{args.history}': {e}")

    if args.partitions:
        catalog = catalog_mgr._load_catalog()
        target = args.partitions
        tables_to_check = []
        if target in ["all", ""]:
            tables_to_check = list(catalog["tables"].keys())
        elif target in catalog["tables"]:
            tables_to_check = [target]
        else:
            match = [k for k in catalog["tables"].keys() if target in k]
            tables_to_check = match if match else [target]

        for tname in tables_to_check:
            try:
                parts = catalog_mgr.discover_partitions(tname)
                print("=" * 80)
                print(f" DISCOVERED PARTITIONS FOR: {tname} ({len(parts)} partitions)")
                print("=" * 80)
                for p in parts:
                    print(f"  |-- {p}")
            except Exception as e:
                print(f"Error discovering partitions for {tname}: {e}")

    if args.export_trino:
        print(catalog_mgr.generate_trino_schema_ddl())


if __name__ == "__main__":
    main()
