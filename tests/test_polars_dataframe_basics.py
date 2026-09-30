from __future__ import annotations

import importlib.util
from pathlib import Path

import pytest

MODULE_PATH = (
    Path(__file__).resolve().parents[1]
    / "processing"
    / "polars"
    / "dataframe_basics.py"
)

spec = importlib.util.spec_from_file_location("polars_dataframe_basics", MODULE_PATH)
assert spec is not None
assert spec.loader is not None
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


def test_people_frame_has_expected_shape_and_columns() -> None:
    frame = module.people_frame()

    assert frame.shape == (4, 4)
    assert frame.columns == ["name", "birthdate", "weight", "height"]


def test_with_derived_columns_adds_birth_year_and_bmi() -> None:
    result = module.with_derived_columns(module.people_frame())

    assert "birth_year" in result.columns
    assert "bmi" in result.columns
    assert result["birth_year"].to_list() == [1997, 1985, 1983, 1981]
    assert result["bmi"][0] == pytest.approx(57.9 / (1.56**2))


def test_csv_round_trip_preserves_row_count(tmp_path: Path) -> None:
    frame = module.people_frame()
    output = tmp_path / "people.csv"

    restored = module.csv_round_trip(frame, output)

    assert output.exists()
    assert restored.height == frame.height
    assert restored.columns == frame.columns
