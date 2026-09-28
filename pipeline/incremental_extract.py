import json
import os
import sqlite3
import time
from pathlib import Path

import requests
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from dotenv import load_dotenv


PROJECT_ROOT = Path(__file__).resolve().parent.parent

load_dotenv(PROJECT_ROOT / ".env")


DATABASE_FILE = PROJECT_ROOT / "database" / "movies.db"
OUTPUT_DIR = PROJECT_ROOT / "data" / "raw"
OUTPUT_FILE = OUTPUT_DIR / "new_movies.json"


API_KEY = os.getenv("TMDB_API_KEY")

API_URL = "https://api.themoviedb.org/3/movie/popular"

TARGET_NEW_MOVIES = int(
    os.getenv("NEW_MOVIE_COUNT", "10")
)


def create_session():
    """
    Create a requests session with automatic retry support.

    Retries help recover from temporary network,
    SSL and server-side failures.
    """

    session = requests.Session()

    retry_strategy = Retry(
        total=5,
        connect=5,
        read=5,
        status=5,

        backoff_factor=2,

        status_forcelist=[
            429,
            500,
            502,
            503,
            504
        ],

        allowed_methods=[
            "GET"
        ],

        raise_on_status=False
    )

    adapter = HTTPAdapter(
        max_retries=retry_strategy
    )

    session.mount(
        "https://",
        adapter
    )

    session.mount(
        "http://",
        adapter
    )

    return session


def get_existing_ids():

    connection = sqlite3.connect(
        DATABASE_FILE
    )

    try:
        rows = connection.execute(
            "SELECT tmdb_id FROM movies"
        ).fetchall()

    finally:
        connection.close()

    return {
        row[0]
        for row in rows
    }


def fetch_new_movies(existing_ids):

    if not API_KEY:
        raise RuntimeError(
            "TMDB_API_KEY is missing from .env"
        )

    new_movies = []

    page = 1

    session = create_session()

    print(
        f"Searching for {TARGET_NEW_MOVIES} new movies..."
    )

    try:

        while len(new_movies) < TARGET_NEW_MOVIES:

            print(
                f"Fetching TMDB page {page}..."
            )

            try:

                response = session.get(
                    API_URL,
                    params={
                        "api_key": API_KEY,
                        "language": "en-US",
                        "page": page
                    },
                    timeout=30
                )

                response.raise_for_status()

            except requests.exceptions.RequestException as error:

                print(
                    f"TMDB request failed on page {page}: "
                    f"{error}"
                )

                raise

            data = response.json()

            results = data.get(
                "results",
                []
            )

            if not results:
                print(
                    "No more TMDB results available."
                )
                break

            for movie in results:

                movie_id = movie.get("id")

                if not movie_id:
                    continue

                if movie_id in existing_ids:
                    continue

                new_movies.append(movie)

                existing_ids.add(
                    movie_id
                )

                print(
                    f"New movie found: "
                    f"{movie.get('title')} "
                    f"(ID: {movie_id})"
                )

                if len(new_movies) >= TARGET_NEW_MOVIES:
                    break

            page += 1

            if page > 500:
                print(
                    "Reached maximum TMDB page limit."
                )
                break

            # Small delay between API requests
            time.sleep(1)

    finally:

        session.close()

    return new_movies


def save_movies(movies):

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

    print(
        f"New movies extracted: {len(movies)}"
    )

    print(
        f"Output: {OUTPUT_FILE}"
    )


def main():

    existing_ids = get_existing_ids()

    print(
        f"Existing movies in SQLite: "
        f"{len(existing_ids)}"
    )

    movies = fetch_new_movies(
        existing_ids
    )

    if not movies:

        raise RuntimeError(
            "No new movies were found."
        )

    save_movies(
        movies
    )


if __name__ == "__main__":
    main()