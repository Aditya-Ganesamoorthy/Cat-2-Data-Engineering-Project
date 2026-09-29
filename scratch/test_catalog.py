import os
import shutil
from pathlib import Path

os.environ["SPARK_LOCAL_IP"] = "127.0.0.1"
if os.name == "nt":
    hadoop_dir = r"C:\hadoop"
    if os.path.exists(hadoop_dir):
        os.environ["HADOOP_HOME"] = hadoop_dir
        if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = rf"C:\hadoop\bin;{os.environ.get('PATH', '')}"

import pyspark
from pyspark.sql import SparkSession
from delta import configure_spark_with_delta_pip

PROJECT_ROOT = Path(__file__).resolve().parent.parent
test_lakehouse = PROJECT_ROOT / "data" / "test_lakehouse"
test_warehouse = PROJECT_ROOT / "data" / "test_warehouse"

for p in [test_lakehouse, test_warehouse]:
    if p.exists():
        shutil.rmtree(p)

builder = (
    SparkSession.builder.appName("LakehouseCatalogTest")
    .master("local[*]")
    .config("spark.driver.host", "127.0.0.1")
    .config("spark.driver.bindAddress", "127.0.0.1")
    .config("spark.sql.shuffle.partitions", "2")
    .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
    .config("spark.driver.memory", "2g")
    .config("spark.ui.enabled", "false")
    .config("spark.sql.warehouse.dir", str(test_warehouse))
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
)
if os.name == "nt" and os.path.exists(r"C:\hadoop"):
    builder = builder.config("spark.hadoop.home.dir", r"C:\hadoop")

spark = configure_spark_with_delta_pip(builder).getOrCreate()
spark.sparkContext.setLogLevel("WARN")

print("1. Creating Database in Catalog...")
spark.sql("CREATE DATABASE IF NOT EXISTS movie_lakehouse")
print("Databases in Catalog:")
spark.sql("SHOW DATABASES").show()

print("2. Writing sample partitioned Delta Lake table with WAL...")
df = spark.range(5).selectExpr(
    "concat('evt_', id) as event_id",
    "concat('Movie_', id) as title",
    "to_date('2026-09-29') as event_date",
    "round(rand() * 10, 2) as vote_average"
)
table_path = test_lakehouse / "staging_events"
df.write.format("delta").partitionBy("event_date").mode("overwrite").save(str(table_path))

print("3. Registering Delta Table in Catalog...")
spark.sql(f"""
    CREATE TABLE IF NOT EXISTS movie_lakehouse.staging_events
    USING delta
    LOCATION '{str(table_path).replace(chr(92), "/")}'
""")

print("Catalog Tables:")
spark.sql("SHOW TABLES IN movie_lakehouse").show()

print("4. Querying catalog table via Spark SQL:")
res = spark.sql("SELECT * FROM movie_lakehouse.staging_events")
res.show()

print("5. Inspecting Delta Table History (ACID WAL Audit Trail):")
history_df = spark.sql("DESCRIBE HISTORY movie_lakehouse.staging_events")
history_df.select("version", "timestamp", "operation", "operationParameters").show(truncate=False)

spark.stop()
for p in [test_lakehouse, test_warehouse]:
    if p.exists():
        shutil.rmtree(p)
print("CATALOG AND DELTA LAKE TEST COMPLETED SUCCESSFULLY!")
