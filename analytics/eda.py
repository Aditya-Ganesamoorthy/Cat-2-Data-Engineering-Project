import sqlite3
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
DATABASE_FILE = PROJECT_ROOT / "database" / "movies.db"
REPORT_DIR = PROJECT_ROOT / "analytics" / "reports"


def load_movies():
    if not DATABASE_FILE.exists():
        raise FileNotFoundError(
            f"Database not found: {DATABASE_FILE}"
        )

    connection = sqlite3.connect(DATABASE_FILE)

    try:
        df = pd.read_sql_query(
            "SELECT * FROM movies",
            connection
        )
    finally:
        connection.close()

    return df


def basic_overview(df):
    print(f"Total records: {len(df)}")
    print(f"Total columns: {len(df.columns)}")

    print("\nColumns:")
    for column in df.columns:
        print(f"- {column}")

    print("\nData types:")
    print(df.dtypes)

    print("\nMissing values:")
    print(df.isnull().sum())


def numerical_summary(df):
    numeric_columns = df.select_dtypes(
        include="number"
    ).columns

    if len(numeric_columns) == 0:
        print("\nNo numerical columns found.")
        return

    print("\nNumerical summary:")
    print(
        df[numeric_columns].describe()
    )


def release_year_analysis(df):
    if "release_date" not in df.columns:
        return

    dates = pd.to_datetime(
        df["release_date"],
        errors="coerce"
    )

    years = dates.dt.year.dropna()

    if years.empty:
        print("\nNo valid release dates found.")
        return

    print("\nMovies by release year:")
    print(
        years.value_counts()
        .sort_index()
    )


def rating_analysis(df):
    available_columns = [
        column
        for column in [
            "vote_average",
            "vote_count"
        ]
        if column in df.columns
    ]

    if not available_columns:
        return

    print("\nRating statistics:")

    for column in available_columns:
        print(f"\n{column}:")
        print(df[column].describe())


def popularity_analysis(df):
    if "popularity" not in df.columns:
        return

    columns = [
        column
        for column in [
            "title",
            "popularity",
            "vote_average",
            "vote_count"
        ]
        if column in df.columns
    ]

    print("\nTop 10 most popular movies:")
    print(
        df.nlargest(
            10,
            "popularity"
        )[columns].to_string(index=False)
    )


def genre_analysis(df):
    if "genres" not in df.columns:
        print("\nGenre column not found.")
        return

    genres = (
        df["genres"]
        .dropna()
        .astype(str)
        .str.split(",")
        .explode()
        .str.strip()
    )

    genres = genres[genres != ""]

    if genres.empty:
        print("\nNo genre information found.")
        return

    print("\nTop genres:")
    print(
        genres.value_counts()
        .head(15)
    )


def language_analysis(df):
    if "original_language" not in df.columns:
        return

    print("\nMovies by original language:")
    print(
        df["original_language"]
        .value_counts()
        .head(15)
    )


def correlation_analysis(df):
    numeric_columns = df.select_dtypes(
        include="number"
    ).columns

    if len(numeric_columns) < 2:
        return

    print("\nCorrelation matrix:")
    print(
        df[numeric_columns].corr()
        .round(3)
    )


def create_reports(df):
    REPORT_DIR.mkdir(
        parents=True,
        exist_ok=True
    )

    overview = pd.DataFrame({
        "metric": [
            "total_movies",
            "total_columns"
        ],
        "value": [
            len(df),
            len(df.columns)
        ]
    })

    overview.to_csv(
        REPORT_DIR / "overview.csv",
        index=False
    )

    missing = (
        df.isnull()
        .sum()
        .reset_index()
    )

    missing.columns = [
        "column",
        "missing_count"
    ]

    missing["missing_percentage"] = (
        missing["missing_count"]
        / len(df)
        * 100
    ).round(2)

    missing.to_csv(
        REPORT_DIR / "missing_values.csv",
        index=False
    )

    numeric = df.select_dtypes(
        include="number"
    )

    if not numeric.empty:
        numeric.describe().T.to_csv(
            REPORT_DIR / "numerical_summary.csv"
        )

    if "release_date" in df.columns:
        dates = pd.to_datetime(
            df["release_date"],
            errors="coerce"
        )

        years = dates.dt.year.dropna()

        if not years.empty:
            year_report = (
                years
                .value_counts()
                .sort_index()
                .rename_axis("release_year")
                .reset_index(
                    name="movie_count"
                )
            )

            year_report.to_csv(
                REPORT_DIR / "release_year_analysis.csv",
                index=False
            )

    if "genres" in df.columns:
        genres = (
            df["genres"]
            .dropna()
            .astype(str)
            .str.split(",")
            .explode()
            .str.strip()
        )

        genres = genres[genres != ""]

        if not genres.empty:
            genre_report = (
                genres
                .value_counts()
                .rename_axis("genre")
                .reset_index(
                    name="movie_count"
                )
            )

            genre_report.to_csv(
                REPORT_DIR / "genre_analysis.csv",
                index=False
            )

            genre_movies = []

            for _, row in df.iterrows():
                if not row.get("genres"):
                    continue

                for genre in str(
                    row["genres"]
                ).split(","):
                    genre = genre.strip()

                    if genre:
                        genre_movies.append({
                            "tmdb_id": row["tmdb_id"],
                            "title": row["title"],
                            "genre": genre,
                            "popularity": row["popularity"],
                            "vote_average": row["vote_average"],
                            "vote_count": row["vote_count"]
                        })

            if genre_movies:
                genre_detail = pd.DataFrame(
                    genre_movies
                )

                genre_summary = (
                    genre_detail
                    .groupby("genre")
                    .agg(
                        movie_count=("tmdb_id", "count"),
                        average_popularity=("popularity", "mean"),
                        average_rating=("vote_average", "mean"),
                        average_vote_count=("vote_count", "mean")
                    )
                    .reset_index()
                )

                genre_summary[
                    "average_popularity"
                ] = genre_summary[
                    "average_popularity"
                ].round(2)

                genre_summary[
                    "average_rating"
                ] = genre_summary[
                    "average_rating"
                ].round(2)

                genre_summary[
                    "average_vote_count"
                ] = genre_summary[
                    "average_vote_count"
                ].round(2)

                genre_summary.to_csv(
                    REPORT_DIR / "genre_summary.csv",
                    index=False
                )

    if "original_language" in df.columns:
        language_report = (
            df["original_language"]
            .value_counts()
            .rename_axis("original_language")
            .reset_index(
                name="movie_count"
            )
        )

        language_report.to_csv(
            REPORT_DIR / "language_analysis.csv",
            index=False
        )

    if "popularity" in df.columns:
        columns = [
            column
            for column in [
                "tmdb_id",
                "title",
                "popularity",
                "vote_average",
                "vote_count",
                "release_date"
            ]
            if column in df.columns
        ]

        df.nlargest(
            20,
            "popularity"
        )[columns].to_csv(
            REPORT_DIR / "top_popular_movies.csv",
            index=False
        )

    if "vote_average" in df.columns:
        columns = [
            column
            for column in [
                "tmdb_id",
                "title",
                "vote_average",
                "vote_count",
                "popularity",
                "release_date"
            ]
            if column in df.columns
        ]

        df.nlargest(
            20,
            "vote_average"
        )[columns].to_csv(
            REPORT_DIR / "top_rated_movies.csv",
            index=False
        )

    if (
        "vote_average" in df.columns
        and "vote_count" in df.columns
    ):
        rating_correlation = pd.DataFrame({
            "metric": [
                "rating_vs_vote_count"
            ],
            "correlation": [
                df[
                    [
                        "vote_average",
                        "vote_count"
                    ]
                ]
                .corr()
                .iloc[0, 1]
            ]
        })

        rating_correlation[
            "correlation"
        ] = rating_correlation[
            "correlation"
        ].round(4)

        rating_correlation.to_csv(
            REPORT_DIR / "rating_correlation.csv",
            index=False
        )

    numeric_columns = df.select_dtypes(
        include="number"
    ).columns

    if len(numeric_columns) >= 2:
        correlation = (
            df[numeric_columns]
            .corr()
            .round(4)
        )

        correlation.to_csv(
            REPORT_DIR / "correlation_matrix.csv"
        )


def main():
    print("Loading movie data from SQLite...")

    df = load_movies()

    if df.empty:
        raise RuntimeError(
            "No movie records found in SQLite."
        )

    basic_overview(df)
    numerical_summary(df)
    release_year_analysis(df)
    rating_analysis(df)
    popularity_analysis(df)
    genre_analysis(df)
    language_analysis(df)
    correlation_analysis(df)
    create_reports(df)

    print("\nEDA completed successfully.")
    print(
        f"Reports saved to: {REPORT_DIR}"
    )


if __name__ == "__main__":
    main()