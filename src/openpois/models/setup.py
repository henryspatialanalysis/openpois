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
    tc.optimize(optimize_level, verbose = verbose)
    return device


def prepare_data_for_model(
    data: pd.DataFrame,
    group_key: str | None = None,
    group_values: list[str] | None = None,
    t1_col: str = 'last_tag_timestamp',
    t2_col: str = 'obs_timestamp',
) -> pd.DataFrame:
    """
    Prepare data for a model.
    """
    if group_key is not None:
        keep_ids = data.dropna(subset = [group_key]).id.unique().tolist()
        data = data.query('id in @keep_ids')
    # If a group values were set, subset to those observations
    if (group_key is not None) and (group_values is not None):
        keep_ids = data.loc[
            data[group_key].isin(group_values), 'id'
        ].unique().tolist()
        data = data.query('id in @keep_ids')
    # Prepare timestamps
    if any(col not in data.columns for col in [t1_col, t2_col]):
        raise ValueError("Timestamp columns are missing from the data.")
    for timestamp_col in [t1_col, t2_col]:
        data[timestamp_col] = pd.to_datetime(data[timestamp_col])
    data = data.assign(
        tag_days = (pd.col(t2_col) - pd.col(t1_col)).dt.days,
        tag_years = pd.col('tag_days') / 365
    )
    data = (data
        .dropna(subset = ['tag_years', 'changed'])
        .query('tag_years > 1e-6')
    )
    return data
