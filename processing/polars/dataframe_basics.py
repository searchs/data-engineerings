"""Basic Polars dataframe transformations migrated from searchs/polars-processings."""

import datetime as dt
from pathlib import Path

import polars as pl


def people_frame() -> pl.DataFrame:
    return pl.DataFrame(
        {
            "name": ["Alice Archer", "Ben Brown", "Chloe Cooper", "Daniel Donovan"],
            "birthdate": [
                dt.date(1997, 1, 10),
                dt.date(1985, 2, 15),
                dt.date(1983, 3, 22),
                dt.date(1981, 4, 30),
            ],
            "weight": [57.9, 72.5, 53.6, 83.1],
            "height": [1.56, 1.77, 1.65, 1.75],
        }
    )


def with_derived_columns(frame: pl.DataFrame) -> pl.DataFrame:
    """Add birth year and BMI columns."""
    return frame.with_columns(
        birth_year=pl.col("birthdate").dt.year(),
        bmi=pl.col("weight") / (pl.col("height") ** 2),
    )


def csv_round_trip(frame: pl.DataFrame, output: Path) -> pl.DataFrame:
    output.parent.mkdir(parents=True, exist_ok=True)
    frame.write_csv(output)
    return pl.read_csv(output, try_parse_dates=True)


if __name__ == "__main__":
    df = people_frame()
    print(with_derived_columns(df))
    print(csv_round_trip(df, Path("data/output.csv")))
