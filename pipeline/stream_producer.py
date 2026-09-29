import argparse
import json
import os
import random
import sys
import time
import uuid
from datetime import datetime, timezone
from pathlib import Path

from confluent_kafka import Producer
from confluent_kafka.admin import AdminClient, NewTopic
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
load_dotenv(PROJECT_ROOT / ".env")

DEFAULT_BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:29092")
if DEFAULT_BROKER.startswith("kafka:"):
    # When running outside docker container on the host machine, fallback to localhost:29092
    DEFAULT_BROKER = "localhost:29092"

DEFAULT_TOPIC = os.getenv("KAFKA_STREAM_TOPIC", "movie_stream_events")
CATALOG_PATH = PROJECT_ROOT / "data" / "processed" / "new_movies_transformed.json"
FALLBACK_RAW_PATH = PROJECT_ROOT / "data" / "raw" / "raw_movies.json"

EVENT_TYPES = ["movie_view", "rating_submitted", "catalog_update", "watchlist_added"]
DEVICE_TYPES = ["SmartTV", "Mobile_iOS", "Mobile_Android", "Web_Chrome", "Tablet"]
COUNTRIES = ["US", "IN", "GB", "CA", "DE", "FR", "JP", "BR", "AU", "KR"]


def ensure_topic_exists(bootstrap_servers: str, topic_name: str, num_partitions: int = 3, replication_factor: int = 1):
    """
    Ensure the target Kafka topic exists. If missing, create it with specified partitions.
    """
    admin_client = AdminClient({"bootstrap.servers": bootstrap_servers})
    try:
        metadata = admin_client.list_topics(timeout=10.0)
        if topic_name in metadata.topics:
            print(f"[Topic Check] Topic '{topic_name}' already exists.")
            return True

        print(f"[Topic Check] Topic '{topic_name}' not found. Creating with {num_partitions} partitions...")
        new_topic = NewTopic(
            topic=topic_name,
            num_partitions=num_partitions,
            replication_factor=replication_factor,
        )
        futures = admin_client.create_topics([new_topic])
        for topic, future in futures.items():
            future.result(timeout=10.0)
        print(f"[Topic Check] Topic '{topic_name}' created successfully.")
        return True
    except Exception as exc:
        print(f"[Topic Check Warning] Could not verify/create topic via AdminClient: {exc}")
        print("Kafka auto.create.topics may handle creation upon first message.")
        return False


def load_movie_catalog():
    """
    Load movie catalog from processed or raw JSON files, or provide default seed movies.
    """
    for candidate in [CATALOG_PATH, FALLBACK_RAW_PATH]:
        if candidate.exists():
            try:
                with candidate.open("r", encoding="utf-8") as f:
                    data = json.load(f)
                    if isinstance(data, list) and len(data) > 0:
                        print(f"[Catalog] Loaded {len(data)} movies from {candidate.name}")
                        return data
            except Exception as e:
                print(f"[Catalog Warning] Error reading {candidate}: {e}")

    # Fallback seed movies if files are not yet created
    print("[Catalog] Using fallback seed movie catalog.")
    return [
        {
            "tmdb_id": 1487864,
            "title": "Nando Between Two Worlds",
            "overview": "A compelling story caught between family life and crime.",
            "release_date": "2026-08-12",
            "popularity": 25.04,
            "vote_average": 7.6,
            "vote_count": 350,
            "genres": "Drama, Thriller, Crime",
            "original_language": "pt",
        },
        {
            "tmdb_id": 273481,
            "title": "Sicario",
            "overview": "An idealistic FBI agent is enlisted by a government task force.",
            "release_date": "2015-09-17",
            "popularity": 36.74,
            "vote_average": 7.8,
            "vote_count": 9738,
            "genres": "Action, Crime, Thriller",
            "original_language": "en",
        },
        {
            "tmdb_id": 157336,
            "title": "Interstellar",
            "overview": "A team of explorers travel through a wormhole in space.",
            "release_date": "2014-11-05",
            "popularity": 92.51,
            "vote_average": 8.6,
            "vote_count": 32000,
            "genres": "Adventure, Drama, Science Fiction",
            "original_language": "en",
        },
    ]


class MovieEventStreamSimulator:
    """
    High-velocity JSON event stream generator with support for schema enforcement testing.
    """

    def __init__(self, catalog: list, anomaly_rate: float = 0.05):
        self.catalog = catalog
        self.anomaly_rate = anomaly_rate

    def generate_event(self) -> tuple[str, str, bool]:
        """
        Generate a single event tuple: (key, json_payload_string, is_anomaly)
        """
        movie = random.choice(self.catalog)
        event_id = str(uuid.uuid4())
        event_type = random.choice(EVENT_TYPES)
        now_utc = datetime.now(timezone.utc).isoformat()
        user_id = f"usr_{random.randint(10000, 99999)}"
        session_id = str(uuid.uuid4())
        device = random.choice(DEVICE_TYPES)
        country = random.choice(COUNTRIES)

        tmdb_id = movie.get("tmdb_id") or movie.get("id") or 1000
        title = movie.get("title", "Unknown Title")
        genres = movie.get("genres", "Drama")
        popularity = float(movie.get("popularity", 10.0))
        vote_average = float(movie.get("vote_average", 7.0))
        vote_count = int(movie.get("vote_count", 100))
        release_date = movie.get("release_date", "2024-01-01")

        is_anomaly = random.random() < self.anomaly_rate

        if is_anomaly:
            anomaly_type = random.choice(["corrupt_json", "null_id", "out_of_bounds", "invalid_type"])

            if anomaly_type == "corrupt_json":
                # Malformed JSON payload to trigger PySpark's _corrupt_record
                corrupt_payload = f'{{"event_id": "{event_id}", "event_type": "{event_type}", "malformed_json: missing_quotes'
                return str(tmdb_id), corrupt_payload, True

            elif anomaly_type == "null_id":
                # Missing required TMDB ID
                payload = {
                    "event_id": event_id,
                    "event_type": event_type,
                    "event_timestamp": now_utc,
                    "user_id": user_id,
                    "session_id": session_id,
                    "device": device,
                    "country": country,
                    "tmdb_id": None,  # Schema violation: null mandatory ID
                    "title": title,
                    "genres": genres,
                    "popularity": popularity,
                    "vote_average": vote_average,
                    "vote_count": vote_count,
                    "release_date": release_date,
                    "view_duration_seconds": random.randint(10, 7200),
                    "user_rating": None,
                }
                return "null_key", json.dumps(payload), True

            elif anomaly_type == "out_of_bounds":
                # Out-of-bounds vote_average and negative popularity
                payload = {
                    "event_id": event_id,
                    "event_type": event_type,
                    "event_timestamp": now_utc,
                    "user_id": user_id,
                    "session_id": session_id,
                    "device": device,
                    "country": country,
                    "tmdb_id": tmdb_id,
                    "title": title,
                    "genres": genres,
                    "popularity": -99.0,  # Negative popularity violation
                    "vote_average": 45.0,  # > 10.0 vote violation
                    "vote_count": vote_count,
                    "release_date": release_date,
                    "view_duration_seconds": -500,
                    "user_rating": 99.9,
                }
                return str(tmdb_id), json.dumps(payload), True

            elif anomaly_type == "invalid_type":
                # Invalid data types
                payload = {
                    "event_id": event_id,
                    "event_type": "UNKNOWN_UNSUPPORTED_ACTION",
                    "event_timestamp": now_utc,
                    "user_id": user_id,
                    "session_id": session_id,
                    "device": device,
                    "country": country,
                    "tmdb_id": "NOT_AN_INTEGER_ID",
                    "title": title,
                    "genres": genres,
                    "popularity": "INVALID_POPULARITY",
                    "vote_average": vote_average,
                    "vote_count": vote_count,
                    "release_date": release_date,
                    "view_duration_seconds": 120,
                    "user_rating": None,
                }
                return str(tmdb_id), json.dumps(payload), True

        # Valid standard payload
        view_duration = random.randint(30, 7200) if event_type == "movie_view" else None
        user_rating = round(random.uniform(1.0, 10.0), 1) if event_type == "rating_submitted" else None

        payload = {
            "event_id": event_id,
            "event_type": event_type,
            "event_timestamp": now_utc,
            "user_id": user_id,
            "session_id": session_id,
            "device": device,
            "country": country,
            "tmdb_id": tmdb_id,
            "title": title,
            "genres": genres,
            "popularity": round(popularity + random.uniform(-1.0, 2.0), 2),
            "vote_average": round(vote_average, 2),
            "vote_count": vote_count,
            "release_date": release_date,
            "view_duration_seconds": view_duration,
            "user_rating": user_rating,
        }

        return str(tmdb_id), json.dumps(payload), False


def run_producer(
    brokers: str = DEFAULT_BROKER,
    topic: str = DEFAULT_TOPIC,
    rate: float = 25.0,
    max_events: int | None = None,
    anomaly_rate: float = 0.05,
):
    """
    Main producer streaming loop. Emits events at the requested rate (events/second).
    """
    print("=" * 70)
    print("[STREAM PRODUCER] Real-Time High-Velocity Movie Event Simulator")
    print(f"Brokers:        {brokers}")
    print(f"Topic:          {topic}")
    print(f"Target Rate:    {rate} events/second")
    print(f"Max Events:     {'Continuous (Infinite)' if max_events is None else max_events}")
    print(f"Anomaly Rate:   {anomaly_rate * 100:.1f}%")
    print("=" * 70)

    ensure_topic_exists(brokers, topic)
    catalog = load_movie_catalog()
    simulator = MovieEventStreamSimulator(catalog, anomaly_rate=anomaly_rate)

    producer = Producer({
        "bootstrap.servers": brokers,
        "client.id": "movie-stream-producer-v2",
        "linger.ms": 5,
        "batch.num.messages": 100,
        "compression.type": "snappy",
    })

    delivered_count = 0
    failed_count = 0
    anomalies_emitted = 0

    def delivery_callback(err, msg):
        nonlocal delivered_count, failed_count
        if err is not None:
            failed_count += 1
            print(f"[Delivery Error] Key: {msg.key()}: {err}")
        else:
            delivered_count += 1

    produced_count = 0
    start_time = time.monotonic()
    last_report_time = start_time
    delay_between_events = 1.0 / rate if rate > 0 else 0.01

    try:
        while True:
            if max_events is not None and produced_count >= max_events:
                print(f"[Completed] Reached maximum event target of {max_events}.")
                break

            key, value, is_anomaly = simulator.generate_event()
            if is_anomaly:
                anomalies_emitted += 1

            producer.produce(
                topic=topic,
                key=key.encode("utf-8"),
                value=value.encode("utf-8"),
                callback=delivery_callback,
            )

            produced_count += 1
            producer.poll(0)

            # Live throughput stats report every 2 seconds
            now = time.monotonic()
            if now - last_report_time >= 2.0:
                elapsed = now - start_time
                actual_rate = produced_count / elapsed if elapsed > 0 else 0
                print(
                    f"[Stream Stats] Emitted: {produced_count:,} | "
                    f"Delivered: {delivered_count:,} | "
                    f"Anomalies: {anomalies_emitted} | "
                    f"Velocity: {actual_rate:.1f} events/s"
                )
                last_report_time = now

            if delay_between_events > 0:
                time.sleep(delay_between_events)

    except KeyboardInterrupt:
        print("\n[Stopped] Stream simulation interrupted by user.")
    finally:
        print("[Flushing] Waiting for in-flight Kafka messages to flush...")
        producer.flush(timeout=10.0)
        total_time = time.monotonic() - start_time
        avg_rate = produced_count / total_time if total_time > 0 else 0
        print("=" * 70)
        print("[SUMMARY REPORT] Movie Event Stream Producer")
        print(f"Total Produced:   {produced_count}")
        print(f"Total Delivered:  {delivered_count}")
        print(f"Failed Messages:  {failed_count}")
        print(f"Test Anomalies:   {anomalies_emitted}")
        print(f"Total Time:       {total_time:.2f} seconds")
        print(f"Average Rate:     {avg_rate:.2f} events/sec")
        print("=" * 70)


def main():
    parser = argparse.ArgumentParser(description="Simulate real-time high-velocity movie events to Kafka.")
    parser.add_argument("--rate", type=float, default=25.0, help="Target events per second (default: 25.0)")
    parser.add_argument("--events", type=int, default=None, help="Number of events to emit (default: continuous)")
    parser.add_argument("--topic", type=str, default=DEFAULT_TOPIC, help=f"Kafka topic (default: {DEFAULT_TOPIC})")
    parser.add_argument("--brokers", type=str, default=DEFAULT_BROKER, help=f"Kafka broker list (default: {DEFAULT_BROKER})")
    parser.add_argument("--anomaly-rate", type=float, default=0.05, help="Anomaly injection rate for testing (default: 0.05)")

    args = parser.parse_args()
    run_producer(
        brokers=args.brokers,
        topic=args.topic,
        rate=args.rate,
        max_events=args.events,
        anomaly_rate=args.anomaly_rate,
    )


if __name__ == "__main__":
    main()
