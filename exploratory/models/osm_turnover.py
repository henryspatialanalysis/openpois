"""
PyTorch model testing

Created February 12, 2026
Purpose: Explore a simple empirical Bayes PyTorch model framework for change data

Reads data prepared in `osm/format_tabular.py`
"""

import numpy as np
import pandas as pd
import torch
from config_versioned import Config

from openpois.models.osm_models import get_model_class
from openpois.models.model_fitter import ModelFitter
from openpois.models.setup import pytorch_setup, prepare_data_for_model

# Globals
config = Config("~/repos/openpois/config.yaml")

DATA_DIR = config.get_dir_path("osm_data")
MODEL_DIR = config.get_dir_path("model_output")
TAG_KEY = config.get("osm_turnover_model", "tag_key")
GROUP_KEY = config.get("osm_turnover_model", "group_key", fail_if_none = False)
GROUP_VALUES = config.get("osm_turnover_model", "group_values", fail_if_none =False)
N_DRAWS = config.get("osm_turnover_model", "n_draws")
SAVE_FULL_MODEL = config.get("osm_turnover_model", "save_full_model")


if __name__ == "__main__":
    print(f"Running on device: {pytorch_setup()}")
    # Ensure model directory exists
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    model_suffix = f"_simple_{TAG_KEY}"
    if GROUP_KEY is not None:
        model_suffix += f"_{GROUP_KEY}"
    if GROUP_VALUES is not None:
        model_suffix += f"_{'-'.join(GROUP_VALUES)}"

    # Data preparation
    observations_df = pd.read_csv(DATA_DIR / f"osm_observations_{TAG_KEY}.csv")
    obs_sub = prepare_data_for_model(
        data = observations_df,
        group_key = GROUP_KEY,
        group_values = GROUP_VALUES,
        t1_col = 'last_tag_timestamp',
        t2_col = 'obs_timestamp',
    )
    obs_sub['dummy'] = 0.0

    model_type = config.get('osm_turnover_model', 'model_type')
    model = get_model_class(model_type)(
        dataset = obs_sub,
        metadata = {
            't1_col': 'dummy',
            't2_col': 'tag_years',
            'group': GROUP_KEY,
            'var_prior': config.get('osm_turnover_model', 'var_prior')
        }
    )

    model_fitter = ModelFitter(
        event_rate_type = model.event_rate_type,
        event_rate_fun = model.model_fun,
        param_likelihood = model.param_likelihood,
        params = model.parameters,
        target = model.target,
        data = model.model_data,
        t1 = model.t1,
        t2 = model.t2,
        verbose = True
    )

    # Run the model and get parameter summaries
    model_fitter.fit()
    model_fitter.generate_parameter_draws(n_draws = N_DRAWS)
    fitted_params = pd.concat([
        model_fitter.get_parameter_table(),
        model.param_ids,
    ], axis = 1)

    # Predictions are done by group for random effects models
    predict_times = torch.tensor(np.arange(11), dtype = torch.float64).reshape(-1, 1)
    if(model_type == 'random_by_type'):
        n_periods = predict_times.shape[0]
        n_groups = model.model_data['group'].max() + 1
        predict_times = predict_times.repeat(n_groups, 1)
        predict_data = {'group': torch.arange(n_groups).repeat_interleave(n_periods)}
    else:
        predict_data = {}
    predictions = model_fitter.predict(
        t2 = predict_times,
        data = predict_data,
    ).assign(units = 'years')
    for name, vals in predict_data.items():
        predictions[name] = vals.reshape(-1)
    if model_type == 'random_by_type':
        predictions = predictions.merge(
            model.group_lookup.rename(columns = {'group_id': 'group'}),
            on = 'group',
            how = 'left'
        ).sort_values(['group_name', 't2'], ascending = True)
    # Save results
    fitted_params.to_csv(
        MODEL_DIR / f"fitted_params{model_suffix}.csv",
        index = False
    )
    predictions.to_csv(
        MODEL_DIR / f"predictions{model_suffix}.csv",
        index = False
    )
    if SAVE_FULL_MODEL:
        torch.save(
            model_fitter,
            MODEL_DIR / f"fitted_params{model_suffix}.pt"
        )
