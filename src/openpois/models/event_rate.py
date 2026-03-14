#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module contains the EventRate class, which is used to store and calculate event rates
for a given model of a Poisson process.
"""

from math import ceil

import torch


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
        fun: callable,
        type: str = 'constant',
        delta: float = 0.02,
        max_steps: int = 200
    ):
        """
        Args:
            fun: Function to calculate the event rate. If `type` is "constant",
                this function will return a scalar. If `type` is "varying",
                this function will return a function `f(t)` that gives the
                event rate at time `t`.
            type: Type of event rate. Must be one of {self.VALID_TYPES}.
            delta: Time step for a time-varying event rate function. Only used
                if `type` is "varying".
            max_steps: Maximum number of integration steps for a time-varying
                event rate function.
        """
        if type not in self.VALID_TYPES:
            raise ValueError(
                f"Invalid event rate type: {type}. Must be one of {self.VALID_TYPES}"
            )
        self.type = type
        self.fun = fun
        self.delta = delta
        self.max_steps = max_steps

    def calculate_change_constant(self, t1: torch.Tensor, t2: torch.Tensor, **kwargs):
        """
        Calculate the change probability for a constant event rate.

        Given lambda and a time period (t1, t2], the change probability is
        lambda * (t2 - t1).

        Args:
            t1: Start times tensor.
            t2: End times tensor.
            **kwargs: Additional keyword arguments passed to self.fun.

        Returns:
            Tensor of change probabilities.
        """
        lam = self.fun(**kwargs)
        if lam.dim() == 1:
            lam = lam.unsqueeze(-1)
        return (t2 - t1).reshape(-1, 1) * lam

    def calculate_change_varying(self, t1: torch.Tensor, t2: torch.Tensor, **kwargs):
        """
        Calculate the change probability for a varying event rate.

        Given a function f(t) that gives the event rate at time t, the change
        probability is the approximate integral from t1 to t2 of f(t).

        Args:
            t1: Start times tensor.
            t2: End times tensor.
            **kwargs: Additional keyword arguments passed to self.fun.

        Returns:
            Tensor of change probabilities.
        """
        f = self.fun(**kwargs)
        t1 = t1.reshape(-1)
        t2 = t2.reshape(-1)
        # Generate samples between start and end points
        steps = ceil((t2 - t1).max().item() / self.delta) + 1
        steps = min(steps, self.max_steps)
        sample_grid = torch.linspace(
            start = 0,
            end = 1,
            steps = steps,
            dtype = t2.dtype,
            device = t2.device,
        ).view(1, -1)
        t_samples = (t2 - t1).unsqueeze(1) * sample_grid + t1.unsqueeze(1)
        y = f(t_samples)
        if y.ndim == 3:
            tz = lambda y, t: torch.trapz(y=y, x=t)  # noqa: E731  # pylint: disable=C3001
            return torch.func.vmap(tz, in_dims=(2, None))(y, t_samples).T
        return torch.trapz(y=y, x=t_samples)

    def calculate_change(self, t1: torch.Tensor, t2: torch.Tensor, **kwargs):
        """
        Calculate the change probability between two time periods.

        Dispatches to calculate_change_constant or calculate_change_varying
        depending on self.type.

        Args:
            t1: Start times tensor.
            t2: End times tensor.
            **kwargs: Additional keyword arguments passed to the delegated method.

        Returns:
            Tensor of change probabilities.
        """
        if self.type == 'constant':
            return self.calculate_change_constant(t1=t1, t2=t2, **kwargs)
        if self.type == 'varying':
            return self.calculate_change_varying(t1=t1, t2=t2, **kwargs)
        raise ValueError(f"Invalid event rate type: {self.type}")
