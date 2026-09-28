import json
import os
import time
from pathlib import Path

import requests
from dotenv import load_dotenv

PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_FILE = RAW_DIR / "raw_movies.json"

load_dotenv(PROJECT_ROOT / ".env")

TMDB_API_KEY = os.getenv("TMDB_API_KEY")

BASE_URL = "https://api.themoviedb.org/3"
POPULAR_ENDPOINT = f"{BASE_URL}/movie/popular"

TARGET_COUNT = 500
REQUEST_TIMEOUT = 20
MAX_RETRIES = 3
MAX_PAGES = 500


def fetch_page(page):
    params = {
        "api_key": TMDB_API_KEY,
        "language": "en-US",
        "page": page
    }

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            response = requests.get(
                POPULAR_ENDPOINT,
                params=params,
                timeout=REQUEST_TIMEOUT
            )

            response.raise_for_status()
            return response.json()

        except requests.RequestException as error:
            print(
                f"Page {page} failed "
                f"(attempt {attempt}/{MAX_RETRIES}): {error}"
            )

            if attempt < MAX_RETRIES:
                time.sleep(3)

    raise RuntimeError(f"Unable to fetch TMDB page {page}")


def extract_movies():
    if not TMDB_API_KEY:
        raise RuntimeError("TMDB_API_KEY not found in .env")

    movies = []
    seen_ids = set()
    page = 1

    print(f"Target movie count: {TARGET_COUNT}")
    print("Starting TMDB extraction...")

    while len(movies) < TARGET_COUNT and page <= MAX_PAGES:
        print(f"Fetching TMDB page {page}...")

        data = fetch_page(page)
        results = data.get("results", [])

        if not results:
            print("No more movie results returned by TMDB.")
            break

        new_movies = 0

        for movie in results:
            movie_id = movie.get("id")

            if movie_id is None or movie_id in seen_ids:
                continue

            seen_ids.add(movie_id)
            movies.append(movie)
            new_movies += 1

            if len(movies) >= TARGET_COUNT:
                break

        print(
            f"Added {new_movies} new movies | "
            f"Collected {len(movies)}/{TARGET_COUNT}"
        )

        page += 1
        time.sleep(0.2)

    if len(movies) < TARGET_COUNT:
        raise RuntimeError(
            f"Only {len(movies)} unique movies were extracted. "
            f"{TARGET_COUNT} required."
        )

    movies = movies[:TARGET_COUNT]

    RAW_DIR.mkdir(parents=True, exist_ok=True)

    with OUTPUT_FILE.open("w", encoding="utf-8") as file:
        json.dump(
            movies,
            file,
            indent=2,
            ensure_ascii=False
        )

    print(f"Saved {len(movies)} movies to {OUTPUT_FILE}")

    return movies


if __name__ == "__main__":
    extract_movies()