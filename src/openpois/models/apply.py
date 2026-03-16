#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
Utilities for applying fitted change-rate model predictions to a POI snapshot.

Provides functions to load saved predictions and build fast numpy lookup arrays
for both constant and random-effects model variants.
"""
from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd


PREDICTIONS_FILE = "predictions.csv"


def load_predictions(version_dir: Path) -> pd.DataFrame:
    """
    Load predictions.csv from a model version directory.

    Adds a ``t2_int`` column (t2 * 10, rounded to int) as an integer lookup key.

    Args:
        version_dir: Path to the versioned model output directory containing
            ``predictions.csv``.

    Returns:
        DataFrame with columns t1, t2, p_mean, p_lower, p_upper, units, t2_int,
        and optionally group / group_name for random-effects models.
    """
    df = pd.read_csv(version_dir / PREDICTIONS_FILE)
    df["t2_int"] = (df["t2"] * 10).round().astype(int)
    return df


def constant_lookup(pred_df: pd.DataFrame) -> np.ndarray:
    """
    Build a (101, 3) float64 lookup array for a constant model.

    Row index = t2_int (0–100); columns = [p_mean, p_lower, p_upper].

    Args:
        pred_df: Predictions DataFrame returned by ``load_predictions``.

    Returns:
        Array of shape (101, 3).
    """
    arr = np.full((101, 3), np.nan)
    idx = pred_df["t2_int"].to_numpy()
    arr[idx] = pred_df[["p_mean", "p_lower", "p_upper"]].to_numpy()
    return arr


def group_lookup(pred_df: pd.DataFrame) -> tuple[list[str], np.ndarray]:
    """
    Build a (n_groups, 101, 3) float64 lookup array for a random-effects model.

    First axis = group index (sorted alphabetically); second = t2_int (0–100);
    third = [p_mean, p_lower, p_upper].

    Args:
        pred_df: Predictions DataFrame returned by ``load_predictions``.
            Must contain a ``group_name`` column.

    Returns:
        Tuple of (ordered_group_names, array of shape (n_groups, 101, 3)).
    """
    groups = sorted(pred_df["group_name"].unique())
    n_groups = len(groups)
    arr = np.full((n_groups, 101, 3), np.nan)
    for ci, col in enumerate(["p_mean", "p_lower", "p_upper"]):
        pivot = (
            pred_df
            .pivot(index = "group_name", columns = "t2_int", values = col)
            .reindex(index = groups, columns = range(101))
        )
        arr[:, :, ci] = pivot.to_numpy()
    return groups, arr
