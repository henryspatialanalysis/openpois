"""
Fit an empirical Bayes PyTorch model for OSM POI tag change rates.

Reads osm_observations_{tag_key}.csv and fits a Poisson change-rate model
using L-BFGS optimization via PyTorch. The model estimates a per-group change
rate λ (events per year). Predictions give the probability that a tag remains
unchanged after t years for t = 0.0, 0.1, ..., 10.0. Supports constant and
random-effects (by type) model specifications.

Config keys used (config.yaml):
    directories.osm_data                    — input data directory
    directories.model_output                — output directory for results
    osm_turnover_model.tag_key              — tag key to model (e.g. "amenity")
    osm_turnover_model.group_key            — column to group by (null = constant)
    osm_turnover_model.group_values         — subset of group values (null = all)
    osm_turnover_model.min_value_count      — minimum observations to include a group
    osm_turnover_model.model_type           — "constant" or "random_by_type"
    osm_turnover_model.var_prior            — prior variance on log(λ)
    osm_turnover_model.n_draws              — number of posterior parameter draws
    osm_turnover_model.save_full_model      — save param_draws and serialized model

Prerequisites:
    Run osm_data/format_tabular.py first.

Output files (in model_output directory):
    fitted_params.csv   — estimated λ with uncertainty per group
    predictions.csv     — p(unchanged) at t = 0.0..10.0 years per group
    param_draws.csv     — posterior draws (if save_full_model = true)
    fitted_model.pt     — serialized ModelFitter (if save_full_model = true)
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
GROUP_VALUES = config.get("osm_turnover_model", "group_values", fail_if_none = False)
MIN_VALUE_COUNT = config.get(
    "osm_turnover_model", "min_value_count", fail_if_none = False
)
N_DRAWS = config.get("osm_turnover_model", "n_draws")
SAVE_FULL_MODEL = config.get("osm_turnover_model", "save_full_model")


if __name__ == "__main__":
    print(f"Running on device: {pytorch_setup()}")
    # Ensure model directory exists
    MODEL_DIR.mkdir(parents=True, exist_ok=True)
    config.write_self("model_output")

    # Data preparation
    observations_df = pd.read_csv(DATA_DIR / f"osm_observations_{TAG_KEY}.csv")
    obs_sub = prepare_data_for_model(
        data = observations_df,
        group_key = GROUP_KEY,
        group_values = GROUP_VALUES,
        min_value_count = MIN_VALUE_COUNT,
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
    fitted_params = (
        pd.concat([
            model_fitter.get_parameter_table(),
            model.param_ids,
        ], axis = 1)
    )
    if model.group_lookup is not None:
        fitted_params = fitted_params.merge(
            model.group_lookup,
            on = 'group_id',
            how = 'left'
        )

    # Predictions are done by group for random effects models
    predict_times = torch.tensor(
        np.arange(101) / 10,
        dtype = torch.float64
    ).reshape(-1, 1)
    if model_type == 'random_by_type':
        n_periods = predict_times.shape[0]
        n_groups = model.model_data['group'].max() + 1
        predict_times = predict_times.repeat(n_groups, 1)
        predict_data = {'group': torch.arange(n_groups).repeat_interleave(n_periods)}
    else:
        predict_data = {}
    predictions = (
        model_fitter
        .predict(
            t2 = predict_times,
            data = predict_data,
        )
        .assign(units = 'years')
    )
    for name, vals in predict_data.items():
        predictions[name] = vals.reshape(-1)
    if model_type == 'random_by_type':
        predictions = predictions.merge(
            model.group_lookup.rename(columns = {'group_id': 'group'}),
            on = 'group',
            how = 'left'
        ).sort_values(['group_name', 't2'], ascending = True)

    # Save results
    config.write(fitted_params, "model_output", "fitted_params")
    config.write(predictions, "model_output", "predictions")
    if SAVE_FULL_MODEL:
        config.write(
            pd.DataFrame(model_fitter.param_draws),
            "model_output",
            "param_draws"
        )
        torch.save(
            model_fitter,
            config.get_file_path("model_output", "fitted_model")
        )
