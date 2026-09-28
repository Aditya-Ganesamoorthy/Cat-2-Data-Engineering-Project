import json
import os
from pathlib import Path

from confluent_kafka import Producer
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "new_movies_transformed.json"
)

KAFKA_BROKER = os.getenv(
    "KAFKA_BOOTSTRAP_SERVERS",
    "localhost:29092"
)

KAFKA_TOPIC = os.getenv(
    "KAFKA_TOPIC",
    "movie_pipeline"
)


def create_producer():
    return Producer({
        "bootstrap.servers": KAFKA_BROKER
    })


def publish_movies():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    with INPUT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        movies = json.load(file)

    if not movies:
        raise RuntimeError(
            "No movies available for publishing."
        )

    producer = create_producer()

    delivered = 0
    failed = 0

    def delivery_report(error, message):
        nonlocal delivered, failed

        if error:
            failed += 1
            print(
                f"Delivery failed for "
                f"{message.key()}: {error}"
            )
        else:
            delivered += 1

    print(
        f"Publishing {len(movies)} movies "
        f"to Kafka topic '{KAFKA_TOPIC}'..."
    )

    for movie in movies:
        movie_id = movie.get("tmdb_id")

        if movie_id is None:
            print("Skipping movie without TMDB ID.")
            continue

        producer.produce(
            topic=KAFKA_TOPIC,
            key=str(movie_id),
            value=json.dumps(movie),
            callback=delivery_report
        )

        producer.poll(0)

    producer.flush()

    print(f"Movies delivered: {delivered}")
    print(f"Movies failed: {failed}")

    if failed > 0:
        raise RuntimeError(
            f"{failed} movie records failed to publish."
        )

    print("Kafka publishing completed successfully.")


if __name__ == "__main__":
    publish_movies()