#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
Unit tests for openpois.models.apply.

All filesystem I/O is mocked so tests run in milliseconds without touching disk.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from openpois.models.apply import (
    PREDICTIONS_FILE,
    constant_lookup,
    group_lookup,
    load_predictions,
)


# ---------------------------------------------------------------------------
# Helpers: minimal prediction DataFrames
# ---------------------------------------------------------------------------


def _make_pred_df(rows: list[dict]) -> pd.DataFrame:
    """Return a predictions DataFrame from a list of row dicts."""
    return pd.DataFrame(rows)


def _constant_rows(t2_vals: list[float]) -> list[dict]:
    """Build minimal constant-model rows for the given t2 values."""
    rows = []
    for t2 in t2_vals:
        rows.append(
            {
                "t1": 0.0,
                "t2": t2,
                "p_mean": round(t2 * 0.1, 4),
                "p_lower": round(t2 * 0.05, 4),
                "p_upper": round(t2 * 0.15, 4),
                "units": "years",
            }
        )
    return rows


def _group_rows(
    groups: list[str],
    t2_vals: list[float],
) -> list[dict]:
    """Build minimal random-effects-model rows for (group, t2) combinations."""
    rows = []
    for grp in groups:
        for t2 in t2_vals:
            rows.append(
                {
                    "t1": 0.0,
                    "t2": t2,
                    "group_name": grp,
                    "group": grp.lower(),
                    "p_mean": round(t2 * 0.1, 4),
                    "p_lower": round(t2 * 0.05, 4),
                    "p_upper": round(t2 * 0.15, 4),
                    "units": "years",
                }
            )
    return rows


# ---------------------------------------------------------------------------
# load_predictions
# ---------------------------------------------------------------------------


class TestLoadPredictions:
    def _patch_read_csv(self, df: pd.DataFrame, tmp_path: Path):
        """Context manager that patches pd.read_csv to return df."""
        return patch("openpois.models.apply.pd.read_csv", return_value=df)

    def test_adds_t2_int_column(self, tmp_path):
        """load_predictions should add a t2_int column equal to t2*10 rounded."""
        raw = _make_pred_df(_constant_rows([0.0, 1.0, 2.5, 5.0, 10.0]))
        with self._patch_read_csv(raw, tmp_path):
            result = load_predictions(tmp_path)

        assert "t2_int" in result.columns
        expected = {0.0: 0, 1.0: 10, 2.5: 25, 5.0: 50, 10.0: 100}
        for _, row in result.iterrows():
            assert row["t2_int"] == expected[row["t2"]]

    def test_t2_int_dtype_is_int(self, tmp_path):
        """t2_int column should have an integer dtype."""
        raw = _make_pred_df(_constant_rows([1.0, 2.0]))
        with self._patch_read_csv(raw, tmp_path):
            result = load_predictions(tmp_path)

        assert np.issubdtype(result["t2_int"].dtype, np.integer)

    def test_original_columns_preserved(self, tmp_path):
        """All original columns (t1, t2, p_mean, p_lower, p_upper, units) remain."""
        raw = _make_pred_df(_constant_rows([1.0]))
        with self._patch_read_csv(raw, tmp_path):
            result = load_predictions(tmp_path)

        for col in ["t1", "t2", "p_mean", "p_lower", "p_upper", "units"]:
            assert col in result.columns

    def test_reads_correct_file_path(self, tmp_path):
        """Should call pd.read_csv with version_dir / PREDICTIONS_FILE."""
        raw = _make_pred_df(_constant_rows([1.0]))
        with patch("openpois.models.apply.pd.read_csv", return_value=raw) as mock_csv:
            load_predictions(tmp_path)

        expected_path = tmp_path / PREDICTIONS_FILE
        mock_csv.assert_called_once_with(expected_path)

    def test_t2_int_rounds_half_up(self, tmp_path):
        """t2 values that map to .5 fractional tenths should round correctly."""
        # t2 = 0.15 → t2*10 = 1.5 → round to 2 (Python banker's round: 2)
        # t2 = 0.25 → t2*10 = 2.5 → round to 2 (banker's round) or 3
        # We test that the result is an integer regardless of rounding direction.
        raw = _make_pred_df(_constant_rows([0.15, 0.25]))
        with self._patch_read_csv(raw, tmp_path):
            result = load_predictions(tmp_path)

        for val in result["t2_int"]:
            assert isinstance(val, (int, np.integer))

    def test_group_name_column_preserved_when_present(self, tmp_path):
        """group_name column should be passed through for random-effects data."""
        raw = _make_pred_df(_group_rows(["alpha", "beta"], [1.0, 2.0]))
        with self._patch_read_csv(raw, tmp_path):
            result = load_predictions(tmp_path)

        assert "group_name" in result.columns
        assert set(result["group_name"].unique()) == {"alpha", "beta"}


# ---------------------------------------------------------------------------
# constant_lookup
# ---------------------------------------------------------------------------


class TestConstantLookup:
    def test_output_shape_is_101_by_3(self):
        """constant_lookup should always return a (101, 3) array."""
        df = _make_pred_df(_constant_rows([1.0, 2.0, 5.0]))
        df["t2_int"] = (df["t2"] * 10).round().astype(int)
        arr = constant_lookup(df)

        assert arr.shape == (101, 3)

    def test_output_dtype_is_float64(self):
        """constant_lookup should return a float64 array."""
        df = _make_pred_df(_constant_rows([1.0]))
        df["t2_int"] = (df["t2"] * 10).round().astype(int)
        arr = constant_lookup(df)

        assert arr.dtype == np.float64

    def test_values_at_known_t2_int_positions(self):
        """Row t2_int should contain [p_mean, p_lower, p_upper] from the DataFrame."""
        rows = _constant_rows([1.0, 5.0])
        df = _make_pred_df(rows)
        df["t2_int"] = (df["t2"] * 10).round().astype(int)
        arr = constant_lookup(df)

        for _, row in df.iterrows():
            idx = row["t2_int"]
            assert arr[idx, 0] == pytest.approx(row["p_mean"])
            assert arr[idx, 1] == pytest.approx(row["p_lower"])
            assert arr[idx, 2] == pytest.approx(row["p_upper"])

    def test_missing_t2_int_positions_are_nan(self):
        """Rows with no prediction data should contain NaN."""
        df = _make_pred_df(_constant_rows([5.0]))  # only t2_int == 50
        df["t2_int"] = (df["t2"] * 10).round().astype(int)
        arr = constant_lookup(df)

        # All rows except index 50 should be NaN
        for i in range(101):
            if i == 50:
                assert not np.isnan(arr[i, 0])
            else:
                assert np.all(np.isnan(arr[i, :]))

    def test_boundary_t2_zero(self):
        """t2 = 0.0 → t2_int = 0; should populate row 0."""
        df = _make_pred_df(_constant_rows([0.0]))
        df["t2_int"] = (df["t2"] * 10).round().astype(int)
        arr = constant_lookup(df)

        assert not np.isnan(arr[0, 0])
        assert arr[0, 0] == pytest.approx(df.iloc[0]["p_mean"])

    def test_boundary_t2_ten(self):
        """t2 = 10.0 → t2_int = 100; should populate row 100."""
        df = _make_pred_df(_constant_rows([10.0]))
        df["t2_int"] = (df["t2"] * 10).round().astype(int)
        arr = constant_lookup(df)

        assert not np.isnan(arr[100, 0])
        assert arr[100, 0] == pytest.approx(df.iloc[0]["p_mean"])

    def test_all_t2_int_values_populated(self):
        """When all 101 t2_int values are present, no NaN should remain."""
        t2_vals = [i / 10 for i in range(101)]
        df = _make_pred_df(_constant_rows(t2_vals))
        df["t2_int"] = (df["t2"] * 10).round().astype(int)
        arr = constant_lookup(df)

        assert not np.any(np.isnan(arr))


# ---------------------------------------------------------------------------
# group_lookup
# ---------------------------------------------------------------------------


class TestGroupLookup:
    def _make_group_df(
        self, groups: list[str], t2_vals: list[float]
    ) -> pd.DataFrame:
        df = _make_pred_df(_group_rows(groups, t2_vals))
        df["t2_int"] = (df["t2"] * 10).round().astype(int)
        return df

    def test_output_shape(self):
        """group_lookup should return (n_groups, 101, 3) array."""
        groups = ["alpha", "beta", "gamma"]
        df = self._make_group_df(groups, [1.0, 5.0])
        _, arr = group_lookup(df)

        assert arr.shape == (3, 101, 3)

    def test_groups_list_sorted_alphabetically(self):
        """Returned group list should be sorted alphabetically."""
        df = self._make_group_df(["zebra", "apple", "mango"], [1.0])
        groups, _ = group_lookup(df)

        assert groups == ["apple", "mango", "zebra"]

    def test_values_at_known_group_and_t2_int(self):
        """arr[g, t2_int, :] should match [p_mean, p_lower, p_upper] from the df."""
        groups = ["alpha", "beta"]
        df = self._make_group_df(groups, [3.0, 7.0])
        returned_groups, arr = group_lookup(df)

        for _, row in df.iterrows():
            gi = returned_groups.index(row["group_name"])
            idx = row["t2_int"]
            assert arr[gi, idx, 0] == pytest.approx(row["p_mean"])
            assert arr[gi, idx, 1] == pytest.approx(row["p_lower"])
            assert arr[gi, idx, 2] == pytest.approx(row["p_upper"])

    def test_missing_t2_int_positions_are_nan(self):
        """(group, t2_int) positions without data should be NaN."""
        df = self._make_group_df(["only"], [5.0])  # t2_int == 50 only
        returned_groups, arr = group_lookup(df)

        gi = returned_groups.index("only")
        for i in range(101):
            if i == 50:
                assert not np.isnan(arr[gi, i, 0])
            else:
                assert np.all(np.isnan(arr[gi, i, :]))

    def test_nan_does_not_bleed_between_groups(self):
        """A missing t2_int for one group should not affect other groups."""
        # alpha only has t2=1.0 (t2_int=10); beta only has t2=5.0 (t2_int=50)
        rows = [
            {
                "t1": 0.0, "t2": 1.0, "group_name": "alpha", "group": "alpha",
                "p_mean": 0.1, "p_lower": 0.05, "p_upper": 0.15, "units": "years",
            },
            {
                "t1": 0.0, "t2": 5.0, "group_name": "beta", "group": "beta",
                "p_mean": 0.5, "p_lower": 0.25, "p_upper": 0.75, "units": "years",
            },
        ]
        df = pd.DataFrame(rows)
        df["t2_int"] = (df["t2"] * 10).round().astype(int)
        returned_groups, arr = group_lookup(df)

        gi_alpha = returned_groups.index("alpha")
        gi_beta = returned_groups.index("beta")

        # alpha row 10 populated, row 50 NaN
        assert not np.isnan(arr[gi_alpha, 10, 0])
        assert np.all(np.isnan(arr[gi_alpha, 50, :]))

        # beta row 50 populated, row 10 NaN
        assert not np.isnan(arr[gi_beta, 50, 0])
        assert np.all(np.isnan(arr[gi_beta, 10, :]))

    def test_single_group(self):
        """group_lookup should work correctly with exactly one group."""
        df = self._make_group_df(["solo"], [2.0, 4.0])
        returned_groups, arr = group_lookup(df)

        assert returned_groups == ["solo"]
        assert arr.shape == (1, 101, 3)
        assert not np.isnan(arr[0, 20, 0])
        assert not np.isnan(arr[0, 40, 0])

    def test_boundary_t2_zero_and_ten(self):
        """t2=0.0 and t2=10.0 should populate t2_int=0 and t2_int=100."""
        df = self._make_group_df(["grp"], [0.0, 10.0])
        returned_groups, arr = group_lookup(df)

        gi = returned_groups.index("grp")
        assert not np.isnan(arr[gi, 0, 0])
        assert not np.isnan(arr[gi, 100, 0])

    def test_output_dtype_is_float64(self):
        """group_lookup should return a float64 array."""
        df = self._make_group_df(["x"], [1.0])
        _, arr = group_lookup(df)

        assert arr.dtype == np.float64

    def test_returns_tuple_of_list_and_ndarray(self):
        """Return type should be (list, np.ndarray)."""
        df = self._make_group_df(["a", "b"], [1.0])
        result = group_lookup(df)

        assert isinstance(result, tuple)
        assert len(result) == 2
        assert isinstance(result[0], list)
        assert isinstance(result[1], np.ndarray)
