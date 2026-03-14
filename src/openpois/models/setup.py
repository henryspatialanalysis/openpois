"""
Prepare environment for OpenPOI models.
"""

import pandas as pd
import torch
import torch_continuum as tc


def pytorch_setup(optimize_level: str = 'fast', verbose: bool = False) -> str:
    """
    Set up PyTorch and torch_continuum for OpenPOI models.

    Args:
        optimize_level: Optimization level for torch_continuum.
            'safe': No precision impact
            'fast': Minor optimization (default).
            'max': High optimization, best for LLMs or large transformers.

    Returns:
        str: Device name.
    """
    device = "cuda" if torch.cuda.is_available() else "cpu"
    if verbose:
        print("Running on", device)
    torch.set_default_device(device)
    torch.set_default_dtype(torch.float64)
    tc.optimize(optimize_level, verbose = verbose)
    return device


def prepare_data_for_model(
    data: pd.DataFrame,
    group_key: str | None = None,
    group_values: list[str] | None = None,
    min_value_count: int | None = None,
    t1_col: str = 'last_tag_timestamp',
    t2_col: str = 'obs_timestamp',
) -> pd.DataFrame:
    """
    Prepare an observations DataFrame for model fitting.

    Optionally filters to rows belonging to IDs that have the group_key
    present, subsets to specific group values, and drops groups below a
    minimum observation count. Converts timestamp columns to datetime and
    computes tag_days / tag_years elapsed columns. Drops rows with missing
    tag_years or changed, and rows with tag_years <= 1e-6.

    Args:
        data: Observations DataFrame as returned by format_observations.
        group_key: Column name of the grouping variable. If None, no group
            filtering is applied.
        group_values: If provided, only rows with group_key in this list are
            kept.
        min_value_count: If provided, groups with fewer than this many
            observations are dropped.
        t1_col: Name of the start-time timestamp column.
        t2_col: Name of the end-time timestamp column.

    Returns:
        Filtered DataFrame with additional tag_days and tag_years columns.

    Raises:
        ValueError: If t1_col or t2_col are not present in data.
    """
    if group_key is not None:
        keep_ids = data.dropna(subset = [group_key]).id.unique().tolist()  # noqa: F841
        data = data.query('id in @keep_ids')
    # If group values were set, subset to those observations
    if (group_key is not None) and (group_values is not None):
        data = (
            data
            .dropna(subset = group_key)
            .query(f'{group_key} in @group_values')
        )
    if (group_key is not None) and (min_value_count is not None):
        value_counts = data.value_counts(group_key)
        groups_over_threshold = (  # noqa: F841
            value_counts[value_counts >= min_value_count].index.tolist()
        )
        data = data.query(f'{group_key} in @groups_over_threshold')
    # Prepare timestamps
    if any(col not in data.columns for col in [t1_col, t2_col]):
        raise ValueError("Timestamp columns are missing from the data.")
    for timestamp_col in [t1_col, t2_col]:
        data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    data = data.assign(
        tag_days = (pd.col(t2_col) - pd.col(t1_col)).dt.days,
        tag_years = pd.col('tag_days') / 365,
    )
    data = (
        data
        .dropna(subset = ['tag_years', 'changed'])
        .query('tag_years > 1e-6')
    )
    return data
