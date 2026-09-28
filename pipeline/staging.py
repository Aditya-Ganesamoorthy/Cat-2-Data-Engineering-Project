import json
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent

INPUT_FILE = PROJECT_ROOT / "data" / "raw" / "new_movies.json"
OUTPUT_DIR = PROJECT_ROOT / "data" / "staging"
OUTPUT_FILE = OUTPUT_DIR / "new_movies.json"


def stage_movies():
    if not INPUT_FILE.exists():
        raise FileNotFoundError(
            f"Input file not found: {INPUT_FILE}"
        )

    with INPUT_FILE.open(
        "r",
        encoding="utf-8"
    ) as file:
        movies = json.load(file)

    if not isinstance(movies, list):
        raise ValueError(
            "Input movie data must be a list."
        )

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
        f"Records staged: {len(movies)}"
    )

    print(
        f"Output: {OUTPUT_FILE}"
    )


if __name__ == "__main__":
    stage_movies()