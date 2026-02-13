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

# Globals
DATA_VERSION = "20260129"
MODEL_VERSION = "20260212"
DATA_DIR = Path("~/data/openpois").expanduser() / DATA_VERSION
MODEL_DIR = Path("~/data/openpois").expanduser() / MODEL_VERSION
TAG_KEY = "name"

# Load data
observations_df = pd.read_csv(DATA_DIR / f"osm_observations_{TAG_KEY}.csv")

# Ensure model directory exists
MODEL_DIR.mkdir(parents = True, exist_ok = True)

# Device setup
DTYPE = torch.float64
DEVICE = "cuda" if torch.cuda.is_available() else "cpu"
print("Running on", DEVICE)
torch.set_default_device(DEVICE)


## Input data preparation --------------------------------------------------------------->

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
X = torch.tensor(obs_sub[['tag_years']].values, dtype=DTYPE, device=DEVICE)
y = torch.tensor(obs_sub['changed'].values, dtype=DTYPE, device=DEVICE)

# Estimand: lambda, the rate parameter that is always positive
omega = torch.tensor(
    np.array([0.0]),
    dtype=DTYPE,
    device=DEVICE,
    requires_grad=True,
)

# Small epsilon to avoid log(0) and log(1-p) = -inf -> NaN
def nll_torchmin(params, y, X, DELTA = 1e-6, EPSILON = 1e-7):
    log_lambda = params[0].clamp(-20.0, 20.0)  # keep lambda in [2e-9, 5e8]
    lambda_ = torch.exp(log_lambda)
    # X is (n,1); ensure positive so p is in (0,1)
    x = X.clamp(min = DELTA)
    p = (
        (1.0 - torch.exp(-lambda_ * x))
        .squeeze(-1)
        .clamp(min = EPSILON, max = 1.0 - EPSILON)
    )
    ll = torch.sum(y * torch.log(p) + (1.0 - y) * torch.log(1.0 - p))
    return -ll

model_fit = torchmin.minimize(
    fun = lambda params: nll_torchmin(params = params, y = y, X = X),
    x0 = omega,
    method = 'l-bfgs',
    tol = 1e-5,
    disp = True,
)

# Prepare model results
hessian_ = torch.autograd.functional.hessian(
    lambda params: nll_torchmin(params, y, X),
    model_fit.x
)
se_torch_ = torch.sqrt(torch.linalg.diagonal(torch.linalg.inv(hessian_)))

m1 = pd.DataFrame({
    'parameter': ['log_lambda'],
    'estimate': model_fit.x.data.cpu().numpy(),
    'std_err': se_torch_.data.cpu().numpy(),
})
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


## Run model and save results ----------------------------------------------------------->

model_results.to_csv(MODEL_DIR / f"fitted_params_{TAG_KEY}.csv", index = False)
torch.save(model_fit, MODEL_DIR / f"fitted_params_{TAG_KEY}.pt")