import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FILE = (
    PROJECT_ROOT
    / "data"
    / "processed"
    / "new_movies_validated.json"
)

OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"

OUTPUT_FILE = (
    OUTPUT_DIR
    / "new_movies_transformed.json"
)

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


def convert_genres(genre_ids):
    if not isinstance(genre_ids, list):
        return ""

    genres = [
        GENRE_MAP[genre_id]
        for genre_id in genre_ids
        if genre_id in GENRE_MAP
    ]

    return ", ".join(genres)


def transform_movie(movie):
    return {
        "tmdb_id": movie.get("id"),
        "title": movie.get("title", "").strip(),
        "overview": movie.get("overview", "").strip(),
        "release_date": movie.get("release_date"),
        "popularity": movie.get("popularity", 0),
        "vote_average": movie.get("vote_average", 0),
        "vote_count": movie.get("vote_count", 0),
        "original_language": movie.get(
            "original_language",
            ""
        ),
        "adult": int(
            bool(movie.get("adult", False))
        ),
        "genre_ids": json.dumps(
            movie.get("genre_ids", [])
        ),
        "genres": convert_genres(
            movie.get("genre_ids", [])
        ),
        "poster_path": movie.get("poster_path"),
        "backdrop_path": movie.get(
            "backdrop_path"
        )
    }


def transform_movies():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    with INPUT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        movies = json.load(file)

    transformed_movies = [
        transform_movie(movie)
        for movie in movies
    ]

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with OUTPUT_FILE.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            transformed_movies,
            file,
            indent=2,
            ensure_ascii=False
        )

    print(
        f"Records transformed: "
        f"{len(transformed_movies)}"
    )

    print(
        f"Output: {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    transform_movies()