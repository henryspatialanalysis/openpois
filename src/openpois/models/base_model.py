#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module contains the base model for OSM POI change data, which is extended by
particular models that make assumptions about the form of the change process.
"""

import torch
import torchmin
import pandas as pd

class EventRate:
    """
    Stores and calculates event rates (lambda) for a given model of a Poisson process.
    If the event rate is constant, the event rate is a scalar, and change probabilities
    can be calculated directly. If the event rate is time-varying, then change
    probabilities have to be integrated over the time period.
    """

    VALID_TYPES = ['constant', 'varying']

    def __init__(
        self,
        type: str = 'constant',
        fun: callable = None,
        delta: float = 0.02
    ):
        """
        Args:
            type: Type of event rate. Must be one of {self.VALID_TYPES}.
            fun: Function to calculate the event rate. If `type` is "constant", this
                function will return a scalar. If `type` is "varying", this function will
                return a function `f(t)` that gives the event rate at time `t`.
            delta: Time step for a time-varying event rate function. Only used if `type`
                is "varying".
        """
        if type not in self.VALID_TYPES:
            raise ValueError(
                f"Invalid event rate type: {type}. Must be one of {self.VALID_TYPES}"
            )
        self.type = type
        self.fun = fun
        self.delta = delta

    def calculate_change_constant(self, t1: torch.Tensor, t2: torch.Tensor, **kwargs):
        """
        Calculate the change probability for a constant event rate. Given lambda and a
        time period (t1, t2], the change probability is `lambda * (t2 - t1)`
        """
        return self.fun(**kwargs) * (t2 - t1)

    def calculate_change_varying(self, t1: torch.Tensor, t2: torch.Tensor, **kwargs):
        """
        Calculate the change probability for a varying event rate. Given a function `f(t)`
        that gives the event rate at time `t`, the change probability is the approximate
        integral from t1 to t2 of `f(t)`
        """
        f = self.fun(**kwargs)
        t_range = torch.linspace(t1, t2, steps = max(2, int((t2 - t1) / self.delta) + 1))
        return torch.trapz(y = f(t_range), x = t_range)

    def calculate_change(self, t1: torch.Tensor, t2: torch.Tensor, **kwargs):
        """
        Calculate the change probability between two time periods for this event rate.
        """
        if self.type == 'constant':
            return self.calculate_change_constant(t1 = t1, t2 = t2, **kwargs)
        elif self.type == 'varying':
            return self.calculate_change_varying(t1 = t1, t2 = t2, **kwargs)
        else:
            raise ValueError(f"Invalid event rate type: {self.type}")


class BaseModel:
    """
    Base class for OpenPOI change rate models.
    """

    # Small epsilon to avoid log(0) and log(1-p) = -inf -> NaN
    EPSILON = 1e-7
    # Model fit tolerance
    TOLERANCE = 1e-5
    # Optimizer type
    OPTIMIZER = 'l-bfgs'

    def __init__(
        self,
        event_rate: EventRate,
        params: torch.Tensor,
        covariates: torch.Tensor,
        target: torch.Tensor,
        t1: torch.Tensor,
        t2: torch.Tensor,
        verbose: bool = False
    ):
        """
        Args:
            observations: DataFrame of observations.
            event_rate: EventRate object.
            params: Parameters to fit.
            covariates: Covariates to use in the model.
            target: Target variable to predict.
            t1: Start time of the time period.
            t2: End time of the time period.
        """
        self.event_rate = event_rate
        self.starting_params = params
        self.covariates = covariates
        self.target = target
        self.t1 = t1
        self.t2 = t2
        self.verbose = verbose
        self.model_run = False
        self.model_fit = None
        self.fitted_params = None
        self.fitted_probs = None
        self.hessian = None
        self.se = None
        self.params_table = None

    def calculate_change_rates(
        self,
        params: torch.Tensor,
        covariates: torch.Tensor = None,
        t1: torch.Tensor = None,
        t2: torch.Tensor = None,
        **kwargs
    ):
        if t1 is None: t1 = self.t1
        if t2 is None: t2 = self.t2
        if covariates is None: covariates = self.covariates
        return self.event_rate.calculate_change(
            t1 = t1,
            t2 = t2,
            params = params,
            covariates = covariates,
            **kwargs,
        )

    def calculate_probs(self, params: torch.Tensor, **kwargs):
        change_rates = self.calculate_change_rates(params = params, **kwargs)
        probs = (
            (1.0 - torch.exp(-1.0 * change_rates))
            .squeeze(-1)
            .clamp(min = self.EPSILON, max = 1.0 - self.EPSILON)
        )
        return probs

    def calculate_nll(self, params: torch.Tensor, **kwargs):
        probs = self.calculate_probs(params = params, **kwargs)
        ll = torch.sum(
            self.target * torch.log(probs) +
            (1.0 - self.target) * torch.log(1.0 - probs)
        )
        return -1.0 * ll

    def prepare_results(self):
        if not self.model_fit:
            raise ValueError("Run fit() first")
        # Prepare table of fitted parameters
        self.hessian = torch.autograd.functional.hessian(
            self.calculate_nll,
            self.fitted_params,
        )
        self.se = torch.sqrt(torch.linalg.diagonal(torch.linalg.inv(self.hessian)))
        self.params_table = pd.DataFrame({
            'estimate': self.fitted_params.data.cpu().numpy(),
            'std_err': self.se.data.cpu().numpy(),
        })

    def fit(self):
        self.model_fit = torchmin.minimize(
            fun = self.calculate_nll,
            x0 = self.starting_params,
            method = self.OPTIMIZER,
            tol = self.TOLERANCE,
            disp = self.verbose,
        )
        self.fitted_params = self.model_fit.x
        self.fitted_probs = self.calculate_probs(params = self.fitted_params)
        self.model_fit = True
        self.prepare_results()

    def get_results(self):
        return self.params_table

    def predict(
        self,
        t1: torch.Tensor = None,
        t2: torch.Tensor = None,
        covariates: torch.Tensor = None,
        **kwargs
    ):
        if t1 is None and t2 is None and covariates is None:
            t1 = self.t1
            t2 = self.t2
            covariates = self.covariates
        elif t1 is None:
            t1 = torch.zeros_like(t2)
        change_rates = self.calculate_change_rates(
            params = self.fitted_params,
            t1 = t1,
            t2 = t2,
            covariates = covariates,
            **kwargs,
        )
        probs = self.calculate_probs(
            params = self.fitted_params,
            t1 = t1,
            t2 = t2,
            covariates = covariates,
            **kwargs,
        )
        return pd.DataFrame(
            {
                't1': t1.data.cpu().numpy(),
                't2': t2.data.cpu().numpy(),
                'probs': probs.data.cpu().numpy(),
                'change_rates': change_rates.data.cpu().numpy(),
            }
        )
