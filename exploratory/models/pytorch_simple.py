"""
PyTorch model testing

Created February 12, 2026
Purpose: Explore a simple empirical Bayes PyTorch model framework for change data

Reads data prepared in `osm/format_tabular.py`
"""

import numpy as np
import pandas as pd
import torch
from pathlib import Path

from openpois.models.model_fitter import ModelFitter
from openpois.models.setup import pytorch_setup, prepare_data_for_model

# Globals
DATA_VERSION = "20260129"
MODEL_VERSION = "20260212"
DATA_DIR = Path("~/data/openpois").expanduser() / DATA_VERSION
MODEL_DIR = Path("~/data/openpois").expanduser() / MODEL_VERSION
TAG_KEY = "name"
GROUP_KEY = None
GROUP_VALUES = ["park"]
N_DRAWS = 250
SAVE_FULL_MODEL = False


if __name__ == "__main__":
    # Ensure model directory exists
    MODEL_DIR.mkdir(parents = True, exist_ok = True)
    model_suffix = f"_simple_{TAG_KEY}"
    if GROUP_KEY is not None:
        model_suffix += f"_{GROUP_KEY}"
    if GROUP_VALUES is not None:
        model_suffix += f"_{'-'.join(GROUP_VALUES)}"

    # Device setup
    dtype = torch.float64
    device = pytorch_setup()
    def tensor(x: np.ndarray, **kwargs) -> torch.Tensor:
        """Convenience function to create a tensor with default dtype and device."""
        return torch.tensor(x, dtype = dtype, device = device, **kwargs)

    # Data preparation
    observations_df = pd.read_csv(DATA_DIR / f"osm_observations_{TAG_KEY}.csv")
    obs_sub = prepare_data_for_model(
        data = observations_df,
        group_key = GROUP_KEY,
        group_values = GROUP_VALUES,
        t1_col = 'last_tag_timestamp',
        t2_col = 'obs_timestamp',
    )

    # Define model
    # Only parameters need requires_grad = True
    y = tensor(obs_sub['changed'].values)
    t2 = tensor(obs_sub[['tag_years']].values)
    t1 = torch.zeros_like(t2)
    # Estimand: (log) lambda, log of the rate parameter
    def simple_model_fun(params: torch.Tensor) -> torch.Tensor:
        return torch.exp(params)
    starting_params = tensor(np.array([0.0]), requires_grad = True)

    # def pseudo_varying_model_fun(params: torch.Tensor) -> callable:
    #     def f_t(t: torch.Tensor) -> torch.Tensor:
    #         return torch.exp(params).repeat(t.shape).reshape(list(t.shape) + [-1])
    #     return f_t

    simple_model = ModelFitter(
        event_rate_type = 'constant',
        event_rate_fun = simple_model_fun,
        params = starting_params,
        target = y,
        data = {},
        t1 = t1,
        t2 = t2,
        verbose = True
    )

    # Run the model and get predictions
    simple_model.fit()
    simple_model.generate_parameter_draws(n_draws = N_DRAWS)
    fitted_params = simple_model.get_parameter_table().assign(parameter = 'log_lambda')
    predictions = simple_model.predict(
        t2 = tensor(np.arange(11)).reshape(-1, 1)
    ).assign(units = 'years')

    # Save results
    fitted_params.to_csv(MODEL_DIR / f"fitted_params{model_suffix}.csv", index = False)
    predictions.to_csv(MODEL_DIR / f"predictions{model_suffix}.csv", index = False)
    if SAVE_FULL_MODEL:
        torch.save(simple_model, MODEL_DIR / f"fitted_params{model_suffix}.pt")
