import argparse
import os
import sys
from pathlib import Path

from dotenv import load_dotenv
from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col,
    coalesce,
    current_timestamp,
    date_format,
    from_json,
    hour,
    lit,
    round as spark_round,
    to_date,
    to_timestamp,
    when,
)
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    LongType,
    StringType,
    StructField,
    StructType,
)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

if os.name == "nt":
    hadoop_dir = r"C:\hadoop"
    if os.path.exists(hadoop_dir):
        os.environ["HADOOP_HOME"] = hadoop_dir
        if r"C:\hadoop\bin" not in os.environ.get("PATH", ""):
            os.environ["PATH"] = rf"C:\hadoop\bin;{os.environ.get('PATH', '')}"

DEFAULT_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
if DEFAULT_BROKER.startswith("kafka:"):
    DEFAULT_BROKER = "localhost:29092"

DEFAULT_TOPIC = os.getenv("KAFKA_STREAM_TOPIC", "movie_stream_events")
LAKEHOUSE_DIR = PROJECT_ROOT / "data" / "lakehouse"
CLEAN_OUTPUT_PATH = LAKEHOUSE_DIR / "staging_events"
QUARANTINE_OUTPUT_PATH = LAKEHOUSE_DIR / "quarantine"
CHECKPOINT_DIR = PROJECT_ROOT / "data" / "checkpoints"


def get_movie_event_schema() -> StructType:
    """
    Define explicit StructType schema for incoming movie events.
    Includes _corrupt_record for PERMISSIVE mode schema enforcement.
    """
    return StructType([
        StructField("event_id", StringType(), True),
        StructField("event_type", StringType(), True),
        StructField("event_timestamp", StringType(), True),
        StructField("user_id", StringType(), True),
        StructField("session_id", StringType(), True),
        StructField("device", StringType(), True),
        StructField("country", StringType(), True),
        StructField("tmdb_id", LongType(), True),
        StructField("title", StringType(), True),
        StructField("genres", StringType(), True),
        StructField("popularity", DoubleType(), True),
        StructField("vote_average", DoubleType(), True),
        StructField("vote_count", IntegerType(), True),
        StructField("release_date", StringType(), True),
        StructField("view_duration_seconds", IntegerType(), True),
        StructField("user_rating", DoubleType(), True),
        StructField("_corrupt_record", StringType(), True),
    ])


def create_spark_session(app_name: str = "MovieStreamProcessor") -> SparkSession:
    """
    Initialize SparkSession configured for Structured Streaming with Kafka.
    """
    builder = (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "2")
        .config("spark.sql.streaming.forceDeleteTempCheckpointLocation", "true")
        .config("spark.driver.memory", "2g")
        .config("spark.ui.enabled", "false")
    )
    if os.name == "nt" and os.path.exists(r"C:\hadoop"):
        builder = builder.config("spark.hadoop.home.dir", r"C:\hadoop")

    spark = builder.getOrCreate()
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_streaming_pipeline(spark: SparkSession, brokers: str, topic: str):
    """
    Build the PySpark Structured Streaming pipeline:
    1. Ingestion from Kafka topic
    2. Schema enforcement and parsing (PERMISSIVE mode with _corrupt_record)
    3. Stream validation & quarantine classification
    4. On-the-fly transformations (watermarking, lakehouse partition columns, metrics)
    """
    schema = get_movie_event_schema()

    # 1. Ingestion: Stream from Kafka
    raw_stream = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", brokers)
        .option("subscribe", topic)
        .option("startingOffsets", "earliest")
        .option("failOnDataLoss", "false")
        .load()
    )

    # 2. Parsing with strict schema enforcement
    parsed_stream = raw_stream.select(
        col("key").cast("string").alias("kafka_key"),
        col("partition").alias("kafka_partition"),
        col("offset").alias("kafka_offset"),
        col("timestamp").alias("kafka_timestamp"),
        from_json(
            col("value").cast("string"),
            schema,
            {"mode": "PERMISSIVE", "columnNameOfCorruptRecord": "_corrupt_record"},
        ).alias("payload"),
        col("value").cast("string").alias("raw_value"),
    )

    # 3. Validation: Classify VALID vs QUARANTINE / INVALID records
    validated_stream = parsed_stream.withColumn(
        "validation_status",
        when(col("payload._corrupt_record").isNotNull(), lit("QUARANTINE"))
        .when(
            col("payload.tmdb_id").isNull()
            | (col("payload.tmdb_id") <= 0)
            | col("payload.event_id").isNull(),
            lit("QUARANTINE"),
        )
        .when(
            (col("payload.vote_average") < 0.0) | (col("payload.vote_average") > 10.0),
            lit("QUARANTINE"),
        )
        .when(col("payload.popularity") < 0.0, lit("QUARANTINE"))
        .when(
            col("payload.view_duration_seconds").isNotNull()
            & (col("payload.view_duration_seconds") < 0),
            lit("QUARANTINE"),
        )
        .otherwise(lit("VALID")),
    ).withColumn(
        "rejection_reason",
        when(col("payload._corrupt_record").isNotNull(), lit("CORRUPT_JSON_SYNTAX"))
        .when(
            col("payload.tmdb_id").isNull() | (col("payload.tmdb_id") <= 0),
            lit("MISSING_OR_INVALID_TMDB_ID"),
        )
        .when(col("payload.event_id").isNull(), lit("MISSING_EVENT_ID"))
        .when(
            (col("payload.vote_average") < 0.0) | (col("payload.vote_average") > 10.0),
            lit("VOTE_AVERAGE_OUT_OF_BOUNDS_0_10"),
        )
        .when(col("payload.popularity") < 0.0, lit("NEGATIVE_POPULARITY"))
        .when(
            col("payload.view_duration_seconds").isNotNull()
            & (col("payload.view_duration_seconds") < 0),
            lit("NEGATIVE_VIEW_DURATION"),
        )
        .otherwise(lit("PASSED_ALL_CHECKS")),
    )

    # 4. Transformations: Derived business metrics and Lakehouse partitioning columns
    transformed_stream = (
        validated_stream.withColumn(
            "event_time",
            coalesce(
                to_timestamp(col("payload.event_timestamp")),
                col("kafka_timestamp"),
                current_timestamp(),
            ),
        )
        .withWatermark("event_time", "10 minutes")
        .withColumn("event_date", to_date(col("event_time")))
        .withColumn("event_hour", hour(col("event_time")))
        .withColumn(
            "rating_bracket",
            when(col("payload.vote_average") >= 8.5, lit("Masterpiece"))
            .when(col("payload.vote_average") >= 7.0, lit("High"))
            .when(col("payload.vote_average") >= 5.0, lit("Medium"))
            .otherwise(lit("Low")),
        )
        .withColumn(
            "engagement_score",
            when(
                col("payload.view_duration_seconds").isNotNull(),
                spark_round(
                    (col("payload.view_duration_seconds") / 60.0)
                    * coalesce(col("payload.vote_average"), lit(5.0)),
                    2,
                ),
            ).otherwise(lit(0.0)),
        )
        .select(
            col("payload.event_id").alias("event_id"),
            col("payload.event_type").alias("event_type"),
            col("event_time"),
            col("event_date"),
            col("event_hour"),
            col("payload.user_id").alias("user_id"),
            col("payload.session_id").alias("session_id"),
            col("payload.device").alias("device"),
            col("payload.country").alias("country"),
            col("payload.tmdb_id").alias("tmdb_id"),
            col("payload.title").alias("title"),
            col("payload.genres").alias("genres"),
            col("payload.popularity").alias("popularity"),
            col("payload.vote_average").alias("vote_average"),
            col("payload.vote_count").alias("vote_count"),
            col("payload.release_date").alias("release_date"),
            col("payload.view_duration_seconds").alias("view_duration_seconds"),
            col("payload.user_rating").alias("user_rating"),
            col("rating_bracket"),
            col("engagement_score"),
            col("validation_status"),
            col("rejection_reason"),
            col("payload._corrupt_record").alias("corrupt_record_raw"),
            col("kafka_partition"),
            col("kafka_offset"),
            col("kafka_timestamp"),
        )
    )

    return transformed_stream


def run_batch_summary(df, epoch_id):
    """
    Micro-batch callback function to display real-time metrics and validation breakdown.
    """
    count = df.count()
    if count == 0:
        return

    valid_count = df.filter(col("validation_status") == "VALID").count()
    quarantine_count = df.filter(col("validation_status") == "QUARANTINE").count()

    print("\n" + "=" * 75)
    print(f"[MICRO-BATCH {epoch_id}] Ingested & Processed: {count} events")
    print(f"  Passed Schema & Validation (VALID):       {valid_count} ({(valid_count/count)*100:.1f}%)")
    print(f"  Quarantined / Corrupt (QUARANTINE):      {quarantine_count} ({(quarantine_count/count)*100:.1f}%)")
    print("=" * 75)

    print("\n--- SAMPLE VALID STREAM RECORDS ---")
    df.filter(col("validation_status") == "VALID").select(
        "event_id", "event_type", "title", "rating_bracket", "engagement_score", "validation_status"
    ).show(5, truncate=False)

    if quarantine_count > 0:
        print("\n--- SAMPLE QUARANTINED RECORDS (SCHEMA ENFORCEMENT) ---")
        df.filter(col("validation_status") == "QUARANTINE").select(
            "event_id", "tmdb_id", "rejection_reason", "corrupt_record_raw"
        ).show(5, truncate=False)


def start_processing(
    brokers: str = DEFAULT_BROKER,
    topic: str = DEFAULT_TOPIC,
    mode: str = "console",
    trigger_available_now: bool = False,
):
    """
    Execute PySpark Structured Streaming job.
    Modes:
      - 'console': Real-time display in terminal with micro-batch statistics.
      - 'lakehouse': Persist clean events to Parquet lakehouse staging + quarantine sink.
      - 'all': Both console monitoring and lakehouse persistence.
    """
    print("=" * 75)
    print("[SPARK STRUCTURED STREAMING] Starting Stream Processor")
    print(f"Brokers:        {brokers}")
    print(f"Topic:          {topic}")
    print(f"Mode:           {mode}")
    print(f"Trigger Mode:   {'AvailableNow (Micro-batch test)' if trigger_available_now else 'Continuous (2s intervals)'}")
    print(f"Lakehouse Path: {CLEAN_OUTPUT_PATH}")
    print(f"Quarantine:     {QUARANTINE_OUTPUT_PATH}")
    print("=" * 75)

    spark = create_spark_session()
    stream_df = build_streaming_pipeline(spark, brokers, topic)

    queries = []

    # Ensure output directories exist
    CLEAN_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    QUARANTINE_OUTPUT_PATH.mkdir(parents=True, exist_ok=True)
    CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

    if mode in ["console", "all"]:
        console_builder = (
            stream_df.writeStream
            .outputMode("append")
            .foreachBatch(run_batch_summary)
            .option("checkpointLocation", str(CHECKPOINT_DIR / "console"))
        )
        if trigger_available_now:
            q_console = console_builder.trigger(availableNow=True).start()
        else:
            q_console = console_builder.trigger(processingTime="2 seconds").start()
        queries.append(q_console)

    if mode in ["lakehouse", "all"]:
        # Clean stream sink (VALID records only)
        clean_df = stream_df.filter(col("validation_status") == "VALID")
        clean_builder = (
            clean_df.writeStream
            .outputMode("append")
            .format("parquet")
            .partitionBy("event_date")
            .option("path", str(CLEAN_OUTPUT_PATH))
            .option("checkpointLocation", str(CHECKPOINT_DIR / "clean_events"))
        )
        if trigger_available_now:
            q_clean = clean_builder.trigger(availableNow=True).start()
        else:
            q_clean = clean_builder.trigger(processingTime="2 seconds").start()
        queries.append(q_clean)

        # Quarantine sink (INVALID / CORRUPT records)
        quarantine_df = stream_df.filter(col("validation_status") == "QUARANTINE")
        quarantine_builder = (
            quarantine_df.writeStream
            .outputMode("append")
            .format("parquet")
            .option("path", str(QUARANTINE_OUTPUT_PATH))
            .option("checkpointLocation", str(CHECKPOINT_DIR / "quarantine"))
        )
        if trigger_available_now:
            q_quarantine = quarantine_builder.trigger(availableNow=True).start()
        else:
            q_quarantine = quarantine_builder.trigger(processingTime="2 seconds").start()
        queries.append(q_quarantine)

    print(f"[Streaming Active] Started {len(queries)} stream writer query(ies).")

    try:
        for q in queries:
            q.awaitTermination()
    except KeyboardInterrupt:
        print("\n[Stopped] Stream processor stopped by user.")
    finally:
        for q in queries:
            if q.isActive:
                q.stop()
        spark.stop()
        print("[Shutdown] SparkSession stopped successfully.")


def main():
    parser = argparse.ArgumentParser(description="PySpark Structured Streaming Ingestion & Validation Job")
    parser.add_argument("--brokers", type=str, default=DEFAULT_BROKER, help=f"Kafka brokers (default: {DEFAULT_BROKER})")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help=f"Kafka topic (default: {DEFAULT_TOPIC})")
    parser.add_argument("--mode", type=str, choices=["console", "lakehouse", "all"], default="console", help="Streaming output mode (default: console)")
    parser.add_argument("--available-now", action="store_true", help="Process all available data in micro-batch and exit")

    args = parser.parse_args()
    start_processing(
        brokers=args.brokers,
        topic=args.topic,
        mode=args.mode,
        trigger_available_now=args.available_now,
    )


if __name__ == "__main__":
    main()
