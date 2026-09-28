import json
import os
import time
from pathlib import Path

from confluent_kafka import Consumer
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")

OUTPUT_DIR = PROJECT_ROOT / "data" / "staging"
OUTPUT_FILE = OUTPUT_DIR / "kafka_movies.json"

KAFKA_BROKER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "movie_pipeline"
)

KAFKA_GROUP_ID = os.getenv(
    "KAFKA_GROUP_ID",
    "movie-project-consumer"
)

IDLE_TIMEOUT = int(
    os.getenv("KAFKA_IDLE_TIMEOUT", "10")
)


def create_consumer():
    consumer = Consumer({
        "bootstrap.servers": KAFKA_BROKER,
        "group.id": KAFKA_GROUP_ID,
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False
    })

    consumer.subscribe([KAFKA_TOPIC])

    return consumer


def consume_movies():
    consumer = create_consumer()

    movies = []
    seen_ids = set()
    idle_start = time.monotonic()

    print(
        f"Listening to Kafka topic '{KAFKA_TOPIC}'..."
    )

    try:
        while True:
            message = consumer.poll(1.0)

            if message is None:
                if (
                    time.monotonic() - idle_start
                    >= IDLE_TIMEOUT
                ):
                    print(
                        f"No new messages for "
                        f"{IDLE_TIMEOUT} seconds."
                    )
                    break

                continue

            if message.error():
                print(
                    f"Kafka message error: "
                    f"{message.error()}"
                )
                continue

            idle_start = time.monotonic()

            raw_value = message.value()
            if raw_value is None:
                continue

            movie = json.loads(
                raw_value.decode("utf-8")
            )

            movie_id = movie.get("tmdb_id")

            if movie_id is None:
                continue

            if movie_id in seen_ids:
                continue

            seen_ids.add(movie_id)
            movies.append(movie)

            print(
                f"Received: {movie.get('title')} "
                f"(ID: {movie_id})"
            )

            consumer.commit(
                message=message,
                asynchronous=False
            )

    finally:
        consumer.close()

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with OUTPUT_FILE.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            movies,
            file,
            indent=2,
            ensure_ascii=False
        )

    print(f"Movies consumed: {len(movies)}")
    print(f"Output: {OUTPUT_FILE}")

    return movies


if __name__ == "__main__":
    consume_movies()