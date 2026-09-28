import json
import sqlite3
from datetime import datetime
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

DATABASE_FILE = (
    PROJECT_ROOT
    / "database"
    / "movies.db"
)

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "staging"
    / "kafka_movies.json"
)


def create_table(connection):
    connection.execute(
        """
        CREATE TABLE IF NOT EXISTS movies (
            tmdb_id INTEGER PRIMARY KEY,
            title TEXT NOT NULL,
            overview TEXT,
            release_date TEXT,
            popularity REAL,
            vote_average REAL,
            vote_count INTEGER,
            original_language TEXT,
            adult INTEGER,
            genre_ids TEXT,
            poster_path TEXT,
            backdrop_path TEXT,
            created_at TEXT,
            genres TEXT
        )
        """
    )

    columns = connection.execute(
        "PRAGMA table_info(movies)"
    ).fetchall()

    column_names = {
        column[1]
        for column in columns
    }

    if "genres" not in column_names:
        connection.execute(
            "ALTER TABLE movies ADD COLUMN genres TEXT"
        )


def load_movies():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Kafka output not found: {INPUT_FILE}"
        )

    with INPUT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        movies = json.load(file)

    if not isinstance(movies, list):
        raise ValueError(
            "Kafka output must contain a list of movies."
        )

    return movies


def insert_movies(connection, movies):
    inserted = 0
    skipped = 0

    for movie in movies:
        tmdb_id = movie.get("tmdb_id")

        if tmdb_id is None:
            skipped += 1
            continue

        exists = connection.execute(
            """
            SELECT 1
            FROM movies
            WHERE tmdb_id = ?
            """,
            (tmdb_id,)
        ).fetchone()

        if exists:
            skipped += 1
            continue

        connection.execute(
            """
            INSERT INTO movies (
                tmdb_id,
                title,
                overview,
                release_date,
                popularity,
                vote_average,
                vote_count,
                original_language,
                adult,
                genre_ids,
                poster_path,
                backdrop_path,
                created_at,
                genres
            )
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                tmdb_id,
                movie.get("title", ""),
                movie.get("overview", ""),
                movie.get("release_date"),
                movie.get("popularity", 0),
                movie.get("vote_average", 0),
                movie.get("vote_count", 0),
                movie.get("original_language", ""),
                int(bool(movie.get("adult", False))),
                movie.get("genre_ids", "[]"),
                movie.get("poster_path"),
                movie.get("backdrop_path"),
                datetime.now().isoformat(
                    timespec="seconds"
                ),
                movie.get("genres", "")
            )
        )

        inserted += 1

    return inserted, skipped


def main():
    movies = load_movies()

    connection = sqlite3.connect(
        DATABASE_FILE
    )

    try:
        create_table(connection)

        inserted, skipped = insert_movies(
            connection,
            movies
        )

        connection.commit()

        total = connection.execute(
            "SELECT COUNT(*) FROM movies"
        ).fetchone()[0]

    finally:
        connection.close()

    print(f"Records received: {len(movies)}")
    print(f"Records inserted: {inserted}")
    print(f"Records skipped: {skipped}")
    print(f"Total movies in database: {total}")


if __name__ == "__main__":
    main()