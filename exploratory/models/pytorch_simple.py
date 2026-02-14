"""
PyTorch model testing

Created February 12, 2026
Purpose: Explore a simple empirical Bayes PyTorch model framework for change data

Reads data prepared in `osm/format_tabular.py`
"""

import numpy as np
import pandas as pd
import torch
import torchmin
import plotnine as gg
from pathlib import Path

from openpois.models.base_model import BaseModel, EventRate

# Globals
DATA_VERSION = "20260129"
MODEL_VERSION = "20260212"
DATA_DIR = Path("~/data/openpois").expanduser() / DATA_VERSION
MODEL_DIR = Path("~/data/openpois").expanduser() / MODEL_VERSION
TAG_KEY = "name"
GROUP_KEY = "leisure"
GROUP_VALUES = ["park"]

# Load data
observations_df = pd.read_csv(DATA_DIR / f"osm_observations_{TAG_KEY}.csv")

# Ensure model directory exists
MODEL_DIR.mkdir(parents = True, exist_ok = True)
model_suffix = f"_simple_{TAG_KEY}"
if GROUP_KEY is not None:
    model_suffix += f"_{GROUP_KEY}"
if GROUP_VALUES is not None:
    model_suffix += f"_{'-'.join(GROUP_VALUES)}"
# Device setup
DTYPE = torch.float64
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
print("Running on", DEVICE)
torch.set_default_device(DEVICE)


## Input data preparation --------------------------------------------------------------->

# If a group key was set, subset to those observations
if GROUP_KEY is not None:
    keep_ids = observations_df.dropna(subset = [GROUP_KEY]).id.unique().tolist()
    observations_df = observations_df.query('id in @keep_ids')
# If a group values were set, subset to those observations
if GROUP_VALUES is not None:
    keep_ids = observations_df.loc[
        observations_df[GROUP_KEY].isin(GROUP_VALUES), 'id'
    ].unique().tolist()
    observations_df = observations_df.query('id in @keep_ids')

timestamp_cols = ['obs_timestamp', 'last_obs_timestamp', 'last_tag_timestamp']
for timestamp_col in timestamp_cols:
    observations_df[timestamp_col] = pd.to_datetime(observations_df[timestamp_col])
observations_df = observations_df.assign(
    tag_days = (pd.col('obs_timestamp') - pd.col('last_tag_timestamp')).dt.days,
    tag_years = pd.col('tag_days') / 365
)
obs_sub = (observations_df
    .dropna(subset = ['tag_years', 'changed'])
    .query('tag_years > 1e-6')
)


## Define model ------------------------------------------------------------------------->

# Only parameters need requires_grad=True; data tensors must not, or memory explodes
y = torch.tensor(obs_sub['changed'].values, dtype=DTYPE, device=DEVICE)
X = torch.zeros(obs_sub.shape[0], 1, dtype=DTYPE, device=DEVICE)
t1 = torch.zeros(obs_sub.shape[0], 1, dtype=DTYPE, device=DEVICE)
t2 = torch.tensor(obs_sub[['tag_years']].values, dtype=DTYPE, device=DEVICE)
# Estimand: (log) lambda, log of the rate parameter
starting_params = torch.tensor(
    np.array([0.0]),
    dtype = DTYPE,
    device = DEVICE,
    requires_grad = True,
)

def simple_model_fun(params, covariates = None):
    return torch.exp(params)

simple_model = BaseModel(
    event_rate = EventRate(
        type = 'constant',
        fun = simple_model_fun,
    ),
    params = starting_params,
    covariates = X,
    target = y,
    t1 = t1,
    t2 = t2,
    verbose = True
)
simple_model.fit()

m1 = simple_model.get_results().assign(parameter = 'log_lambda')
m2 = (
    m1
    .copy()
    .assign(
        parameter = 'lambda',
        estimate = np.exp(pd.col('estimate')),
        std_err = pd.col('estimate') * pd.col('std_err')
    )
)
model_results = pd.concat([m1, m2])

predictions = simple_model.predict(
    t2 = torch.tensor(np.arange(11), dtype = DTYPE, device = DEVICE),
    covariates = None,
).assign(units = 'years')
predictions.to_csv(MODEL_DIR / f"predictions{model_suffix}.csv", index = False)


## Run model and save results ----------------------------------------------------------->

model_results.to_csv(MODEL_DIR / f"fitted_params{model_suffix}.csv", index = False)
torch.save(simple_model, MODEL_DIR / f"fitted_params{model_suffix}.pt")
