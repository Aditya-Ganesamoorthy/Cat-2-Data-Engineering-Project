import json
import sqlite3
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASE_FILE = PROJECT_ROOT / "database" / "movies.db"

GENRE_MAP = {
    28: "Action",
    12: "Adventure",
    16: "Animation",
    35: "Comedy",
    80: "Crime",
    99: "Documentary",
    18: "Drama",
    10751: "Family",
    14: "Fantasy",
    36: "History",
    27: "Horror",
    10402: "Music",
    9648: "Mystery",
    10749: "Romance",
    878: "Science Fiction",
    10770: "TV Movie",
    53: "Thriller",
    10752: "War",
    37: "Western"
}


def convert_genre_ids(genre_ids):
    if not genre_ids:
        return ""

    if isinstance(genre_ids, str):
        try:
            genre_ids = json.loads(genre_ids)
        except json.JSONDecodeError:
            genre_ids = [
                int(value.strip())
                for value in genre_ids.split(",")
                if value.strip().isdigit()
            ]

    genres = [
        GENRE_MAP[genre_id]
        for genre_id in genre_ids
        if genre_id in GENRE_MAP
    ]

    return ", ".join(genres)


def update_database():
    connection = sqlite3.connect(DATABASE_FILE)
    cursor = connection.cursor()

    columns = cursor.execute(
        "PRAGMA table_info(movies)"
    ).fetchall()

    column_names = [
        column[1]
        for column in columns
    ]

    if "genres" not in column_names:
        cursor.execute(
            "ALTER TABLE movies ADD COLUMN genres TEXT"
        )

    rows = cursor.execute(
        "SELECT tmdb_id, genre_ids FROM movies"
    ).fetchall()

    updated = 0

    for tmdb_id, genre_ids in rows:
        genres = convert_genre_ids(genre_ids)

        cursor.execute(
            """
            UPDATE movies
            SET genres = ?
            WHERE tmdb_id = ?
            """,
            (genres, tmdb_id)
        )

        updated += 1

    connection.commit()
    connection.close()

    print(f"Genre records updated: {updated}")


if __name__ == "__main__":
    update_database()