import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FILE = PROJECT_ROOT / "data" / "staging" / "new_movies.json"
OUTPUT_DIR = PROJECT_ROOT / "data" / "processed"
VALIDATED_FILE = OUTPUT_DIR / "new_movies_validated.json"
INVALID_FILE = OUTPUT_DIR / "new_movies_invalid.json"

REQUIRED_FIELDS = [
    "id",
    "title",
    "overview",
    "release_date",
    "popularity",
    "vote_average",
    "vote_count",
    "original_language",
    "genre_ids"
]


def validate_movie(movie):
    errors = []

    for field in REQUIRED_FIELDS:
        if field not in movie:
            errors.append(
                f"Missing field: {field}"
            )

    if not movie.get("id"):
        errors.append("Invalid TMDB ID")

    if not movie.get("title"):
        errors.append("Missing title")

    if movie.get("popularity") is None:
        errors.append("Missing popularity")

    if movie.get("vote_average") is None:
        errors.append("Missing vote average")

    if movie.get("vote_count") is None:
        errors.append("Missing vote count")

    if not isinstance(
        movie.get("genre_ids"),
        list
    ):
        errors.append("Invalid genre_ids")

    return errors


def validate_movies():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    with INPUT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        movies = json.load(file)

    valid_movies = []
    invalid_movies = []

    for movie in movies:
        errors = validate_movie(movie)

        if errors:
            invalid_movies.append({
                "movie": movie,
                "errors": errors
            })
        else:
            valid_movies.append(movie)

    OUTPUT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    with VALIDATED_FILE.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            valid_movies,
            file,
            indent=2,
            ensure_ascii=False
        )

    with INVALID_FILE.open(
        "w",
        encoding="utf-8"
    ) as file:
        json.dump(
            invalid_movies,
            file,
            indent=2,
            ensure_ascii=False
        )

    print(
        f"Records received: {len(movies)}"
    )
    print(
        f"Valid records: {len(valid_movies)}"
    )
    print(
        f"Invalid records: {len(invalid_movies)}"
    )
    print(
        f"Validated output: {VALIDATED_FILE}"
    )
    print(
        f"Invalid output: {INVALID_FILE}"
    )


if __name__ == "__main__":
    validate_movies()