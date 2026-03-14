#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module contains all models for OSM POI turnover rate estimation.

Setup:
   - Some (but not all) models need to understand the shape of the data they are fitting.
     To help build the models, we use objects that extend the ModelFactory class. A
     ModelFactory object takes two arguments:
       1. The dataset that will be fit (pd.dataFrame)
       2. A dictionary of metadata about the dataset (dict)

     The ModelFactory then creates a set of objects that are needed to fit the model:
       1. Model dataset
       2. Model parameters
       3. Parameter identifiers: param names, group IDs for random effects models
       3. A function to calculate the change rate λ of each observation (callable)
          a. For constant change rate models, this function will return a scalar λ.
          b. For time-varying change rate models, this will return another function λ(t).
       4. A function to calculate the parameter likelihood (callable)
       5. Type of model: constant or time-varying change rates (str)
"""

import numpy as np
import pandas as pd
import torch

from abc import ABC, abstractmethod
from functools import partial


class ModelFactory(ABC):
    """
    A factory for creating models for OSM POI turnover rate estimation.
    """
    def __init__(self, dataset: pd.DataFrame, metadata: dict):
        """
        Args:
            dataset: Raw observations DataFrame.
            metadata: Dict of model configuration options (e.g., group key,
                column names, priors).
        """
        self.raw_data = dataset
        self.model_data = {}
        self.target = None
        self.t1 = None
        self.t2 = None
        self.metadata = metadata
        self.parameters = None
        self.param_ids = None
        self.group_lookup = None
        self.model_fun = None
        self.param_likelihood = None
        self.event_rate_type = None
        self.validate_inputs()
        self.build_model()
        self.assign_targets()

    def validate_inputs(self):
        """Validate inputs before building the model. Override to add checks."""
        pass

    @abstractmethod
    def build_model(self):
        """Build model components (model_fun, event_rate_type, parameters, etc.)."""
        pass

    def assign_targets(self):
        """Assign target tensor and time tensors from raw_data."""
        self.target = torch.tensor(self.raw_data['changed'].values)
        t1_col = self.metadata.get('t1_col', 't1')
        self.t1 = torch.tensor(self.raw_data[t1_col].values)
        t2_col = self.metadata.get('t2_col', 't2')
        self.t2 = torch.tensor(self.raw_data[t2_col].values)


# Simplest models: constant change rate, no random effects ------------------------------>

def simple_model(params: torch.Tensor) -> torch.Tensor:
    """
    A simple model for the change rate λ: λ = exp(θ).
    """
    return torch.exp(params)


def pseudo_varying_model(params: torch.Tensor) -> callable:
    """
    A pseudo-varying model for the change rate λ: λ = exp(θ). This model is the same as
    simple_model and should return the same results. It is used to test the time-varying
    model functionality.

    Note that time-varying models always return a function λ(t) rather than a scalar.
    """
    def f_t(t: torch.Tensor) -> torch.Tensor:
        """Return the event rate as a time-constant tensor shaped like t."""
        return torch.exp(params).repeat(t.shape).reshape(list(t.shape) + [-1])
    return f_t


def default_param_likelihood(params: torch.Tensor) -> torch.Tensor:
    """
    A default parameter likelihood for the simple model: no prior on the parameters.
    """
    return torch.tensor(0.0, requires_grad = True)


class ConstantModel(ModelFactory):
    """
    A constant model for the change rate λ: λ = exp(θ).
    """

    def build_model(self):
        """Build a constant-rate model with a single log-lambda parameter."""
        self.model_fun = simple_model
        self.event_rate_type = 'constant'
        self.param_likelihood = default_param_likelihood
        self.parameters = torch.tensor([0.0], requires_grad = True)
        self.param_ids = pd.DataFrame({'param_name': ['log_lambda']})


class PseudoVaryingModel(ModelFactory):
    """
    A pseudo-varying model for the change rate λ: λ = exp(θ).

    This model is the same as simple_model and should return the same results.
    It is used to test the time-varying model functionality.

    Note that time-varying models always return a function λ(t) rather than a
    scalar.
    """

    def build_model(self):
        """Build a pseudo-varying model with a single log-lambda parameter."""
        self.model_fun = pseudo_varying_model
        self.event_rate_type = 'varying'
        self.param_likelihood = default_param_likelihood
        self.parameters = torch.tensor([0.0], requires_grad = True)
        self.param_ids = pd.DataFrame({'param_name': ['log_lambda']})


# Random effects models: change rate constant over time, varies by subgroup ------------->

def random_by_type_model(params: torch.Tensor, group: torch.Tensor) -> torch.Tensor:
    """
    A random effects model for the change rate λ: λ = exp(θ + ε), where ε is a random
    effect by group. The random effects are assumed to be normally distributed with mean
    0 and variance v^2.

    Parameter order:
        - params[0]: log of base lambda, log(λ_0)
        - params[1]: log of variance, log(v^2)
        - params[2:]: random effects, ε_i

    Args:
        params: Parameters to fit.
        data: Data to use in the model.

    Returns:
        The change rate λ for each observation.
    """
    return torch.exp(params[0] + params[2 + group])


def random_by_type_param_likelihood(
    params: torch.Tensor,
    var_prior: tuple[float, float] = (0.0, 1.0)
) -> torch.Tensor:
    """
    Parameter likelihood for the random effects model. The random effects are assumed to
    be normally distributed with mean 0 and variance σ^2.

    Parameter order:
    - params[0]: log of base lambda, log(λ_0). No prior on this parameter.
    - params[1]: log of standard deviation, log(σ). Prior set by N(mean = var_prior[0],
        var = var_prior[1]).
    - params[2:]: random effects, ε_i. Assumed to be distributed N(0, σ^2)

    Returns:
        The parameter log-likelihood.
    """
    # Hyperprior on variance
    ll = torch.distributions.Normal(
        loc = var_prior[0],
        scale = var_prior[1]
    ).log_prob(params[1]).sum()
    # Prior on random effects
    ll += torch.distributions.Normal(
        loc = 0.0,
        scale = torch.exp(params[1])
    ).log_prob(params[2:]).sum()
    return ll


class RandomByTypeModel(ModelFactory):
    """
    A random effects model for the change rate λ: λ = exp(θ) + ε, where ε is a random
    effect by group. The random effects are assumed to be normally distributed with mean
    0 and variance v^2.
    """

    def validate_inputs(self):
        """Validate that metadata contains a 'group' key present in raw_data."""
        if self.metadata is None or 'group' not in self.metadata:
            raise ValueError("Key 'group' is required in metadata")
        if not isinstance(self.raw_data, pd.DataFrame):
            raise ValueError("Raw data must be a pandas DataFrame")
        if self.metadata['group'] not in self.raw_data.columns:
            raise ValueError(
                f"Group key '{self.metadata['group']}' not found in raw data columns: "
                f"{', '.join(self.raw_data.columns.tolist())}"
            )

    def build_model(self):
        """Build a random-effects model with per-group lambdas."""
        self.model_fun = random_by_type_model
        self.event_rate_type = 'constant'
        self.param_likelihood = partial(
            random_by_type_param_likelihood,
            var_prior = self.metadata.get('var_prior', (0.0, 1.0))
        )
        self.raw_data = self.raw_data.dropna(subset = self.metadata['group'])
        self.raw_data['group_id'] = (
            self.raw_data[self.metadata['group']].astype('category').cat.codes
        )
        self.model_data['group'] = torch.tensor(
            self.raw_data['group_id'].values,
            dtype = torch.int64
        )
        self.group_lookup = (
            self.raw_data
            .loc[:, [self.metadata['group'], 'group_id']]
            .rename(columns = {self.metadata['group']: 'group_name'})
            .drop_duplicates()
            .sort_values('group_id', ascending = True)
            .reset_index(drop = True)
        )
        n_groups = self.group_lookup.shape[0]
        self.parameters = torch.tensor(
            [0.0] * (n_groups + 2),
            requires_grad = True
        )
        self.param_ids = pd.DataFrame({
            'param_name': ['log_lambda', 'log_sigma'] + ['epsilon'] * n_groups,
            'group_id': [np.nan, np.nan] + list(range(n_groups))
        })


# Model selector dictionary for importing and selection by model name
MODEL_REGISTRY = {
    "constant": ConstantModel,
    "pseudo_varying": PseudoVaryingModel,
    "random_by_type": RandomByTypeModel,
}


def get_model_class(model_name: str) -> type[ModelFactory]:
    """
    Return a ModelFactory subclass by name from MODEL_REGISTRY.

    Args:
        model_name: Registry key (e.g., 'constant', 'random_by_type').

    Returns:
        The corresponding ModelFactory subclass.

    Raises:
        ValueError: If model_name is not found in MODEL_REGISTRY.
    """
    if model_name not in MODEL_REGISTRY:
        raise ValueError(
            f"Unknown model '{model_name}'. Valid options: "
            f"{', '.join(MODEL_REGISTRY.keys())}"
        )
    return MODEL_REGISTRY[model_name]
