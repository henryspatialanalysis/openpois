#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module contains a fitter for POI change models.
"""

import torch
import torchmin
import pandas as pd

from openpois.models.event_rate import EventRate


class ModelFitter:
    """
    Fitter for POI change rate models.
    """

    # Small epsilon to avoid log(0) and log(1-p) = -inf -> NaN
    EPSILON = 1e-7
    # Model fit tolerance
    TOLERANCE = 1e-5
    # Optimizer type
    OPTIMIZER = 'l-bfgs'

    def __init__(
        self,
        event_rate_type: str,
        event_rate_fun: callable,
        params: torch.Tensor,
        data: dict[str, torch.Tensor],
        target: torch.Tensor,
        t1: torch.Tensor,
        t2: torch.Tensor,
        param_likelihood: callable | None = None,
        verbose: bool = False
    ):
        """
        Args:
            observations: DataFrame of observations.
            event_rate: EventRate object.
            params: Parameters to fit.
            data: Observations and covariates to use in the model.
            target: Target variable to predict.
            t1: Start time of the time period.
            t2: End time of the time period.
        """
        self.event_rate = EventRate(
            type=event_rate_type,
            fun=event_rate_fun
        )
        self.param_likelihood = param_likelihood
        self.starting_params = params
        self.data = data
        self.target = target
        self.t1 = t1
        self.t2 = t2
        self.verbose = verbose
        self.model_run = False
        self.model_fit = None
        self.fitted_params = None
        self.fitted_probs = None
        self.hessian = None
        self.param_draws = None

    def calculate_change_rates(
        self,
        params: torch.Tensor,
        data: dict[str, torch.Tensor] = {},
        t1: torch.Tensor = None,
        t2: torch.Tensor = None,
        **kwargs
    ):
        if t1 is None:
            t1 = self.t1
        if t2 is None:
            t2 = self.t2
        return self.event_rate.calculate_change(
            t1=t1,
            t2=t2,
            params=params,
            **data,
            **kwargs,
        )

    def calculate_probs(self, params: torch.Tensor, **kwargs):
        change_rates = self.calculate_change_rates(params=params, **kwargs)
        probs = (
            (1.0 - torch.exp(-1.0 * change_rates))
            .squeeze(-1)
            .clamp(min=self.EPSILON, max=1.0 - self.EPSILON)
        )
        return probs

    def calculate_nll(self, params: torch.Tensor, **kwargs):
        probs = self.calculate_probs(params=params, **kwargs)
        ll = torch.sum(
            self.target * torch.log(probs) +
            (1.0 - self.target) * torch.log(1.0 - probs)
        )
        if self.param_likelihood is not None:
            ll += self.param_likelihood(params)
        return -1.0 * ll

    def fit(self):
        self.model_fit = torchmin.minimize(
            fun=self.calculate_nll,
            x0=self.starting_params,
            method=self.OPTIMIZER,
            tol=self.TOLERANCE,
            disp=self.verbose,
        )
        self.fitted_params = self.model_fit.x
        self.fitted_probs = self.calculate_probs(params=self.fitted_params)
        self.model_fit = True

    def generate_parameter_draws(self, n_draws: int, seed: int | None = None):
        """
        Using a Gaussian approximation to the posterior, generate N parameter draws.

        The precision matrix (Hessian of the negative log-likelihood at the MLE) is
        Cholesky-decomposed. Draws are generated as
        theta = theta_hat + L^{-T} z,  z ~ N(0, I),
        so that Cov(theta) = (L L^T)^{-1} = Hessian^{-1}.

        Args:
            n_draws: Number of posterior draws to generate.
            seed: Optional RNG seed for reproducibility.

        Returns:
            Tensor of shape (n_params, n_draws) where each row corresponds to one
            parameter and each column corresponds to a posterior draw.
        """
        if not self.model_fit:
            raise ValueError("Run fit() first")
        if self.hessian is None:
            self.hessian = torch.autograd.functional.hessian(
                self.calculate_nll,
                self.fitted_params,
            )
        if seed is not None:
            torch.manual_seed(seed)
        # Precision = Hessian; Cholesky: precision = L @ L.T (lower triangular L)
        L_prec = torch.linalg.cholesky(self.hessian)
        n_params = self.fitted_params.shape[0]
        z = torch.randn(
            n_draws,
            n_params,
            device=self.hessian.device,
            dtype=self.hessian.dtype
        )
        # theta = mu + (L^{-T} @ z.T).T  so each draw is mu + inv(L.T) @ z_i
        solves = torch.linalg.solve(
            L_prec.T.unsqueeze(1).expand(n_draws, -1, -1),
            z.unsqueeze(-1),
        )
        draws = self.fitted_params.unsqueeze(1) + solves.squeeze(-1).transpose(0, 1)
        self.param_draws = draws

    def get_parameter_draws(self):
        if self.param_draws is None:
            raise ValueError("Run generate_parameter_draws() first")
        return self.param_draws

    def _to_table(self, x):
        """
        Helper function to convert a tensor to a numpy array.
        """
        return x.data.cpu().numpy().reshape(-1)

    def get_parameter_table(
        self,
        ui_width: float = 0.95,
    ):
        """
        Get a table of parameter draws.
        """
        if self.param_draws is None:
            raise ValueError("Run generate_parameter_draws() first")
        if (ui_width < 0.0) or (ui_width > 1.0):
            raise ValueError("ui_width must be between 0 and 1")
        lb = (1.0 - ui_width) / 2
        ub = 1.0 - lb
        return pd.DataFrame(
            {
                'mean': self._to_table(self.param_draws.mean(dim=1)),
                'lower': self._to_table(self.param_draws.quantile(q=lb, dim=1)),
                'upper': self._to_table(self.param_draws.quantile(q=ub, dim=1)),
            }
        )

    def predict(
        self,
        t1: torch.Tensor = None,
        t2: torch.Tensor = None,
        data: dict[str, torch.Tensor] = {},
        ui_width: float = 0.95,
        **kwargs
    ):
        if self.param_draws is None:
            raise ValueError("Run generate_draws() first")
        if (t1 is None) and (t2 is None) and (len(data) == 0):
            t1 = self.t1
            t2 = self.t2
            data = self.data
        elif t1 is None:
            t1 = torch.zeros_like(t2)
        probs = self.calculate_probs(
            params=self.param_draws,
            t1=t1,
            t2=t2,
            **data,
            **kwargs,
        )
        lb = (1.0 - ui_width) / 2
        ub = 1.0 - lb
        return pd.DataFrame(
            {
                't1': self._to_table(t1),
                't2': self._to_table(t2),
                'p_mean': self._to_table(probs.mean(dim=1)),
                'p_lower': self._to_table(probs.quantile(q=lb, dim=1)),
                'p_upper': self._to_table(probs.quantile(q=ub, dim=1)),
            }
        )
