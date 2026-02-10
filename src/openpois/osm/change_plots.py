#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module creates plots showing the stability of various OSM tags over time.
"""

import numpy as np
import pandas as pd
import plotnine as gg
from functools import reduce


def change_plot_reshape_data(
    observations: pd.DataFrame,
    no_change_col: str,
    change_col: str,
    final_observation_col: str,
    day_range: int = 365*10,
) -> pd.DataFrame:
    """
    Reshape data for the change plot. The data comes in with one row per POI-tag, and
    is reshaped by elapsed days since the POI-tag was added. For each elapsed day, there
    are four possibilities:
        1. Confirmed unchanged: The tag was observed unchanged on or *after* this day
        2. Confirmed changed: The tag was last observed changed on or *before* this day
        2. Unsure: The tag was last observed unchanged *before* this day, but has not yet
            been observed changed
        4. Aged out: The maximum time elapsed between when the tag was added and our data
            download is *before* this day, so we should drop it from the plot

    Args:
        observations: DataFrame with observations. Each row is an iteration of a
            tag, with the three columns described below.
        no_change_col: Column name for the days elapsed from when the tag was added to
            when it was last confirmed (observed unchanged).
        change_col: Column name for the days elapsed from when the tag was added to when
            it was changed. For tags that were unchanged, this will be infinity.
        final_observation_col: Column name for the days elapsed from when the tag was
            added to when this data was downloaded.
        day_range: Maximum elapsed time period to plot, in days

    Returns:
        DataFrame where each row is an elapse d
    """
    reshaped = (
        pd.DataFrame({
            'no_change': [
                np.sum(day_i < observations[no_change_col])
                for day_i in range(day_range)
            ],
            'unknown': [
                np.sum(
                    (observations[no_change_col] <= day_i) &
                    (day_i < observations[final_observation_col])
                )
                for day_i in range(day_range)
            ],
            'change': [
                np.sum(
                    (observations[change_col] <= day_i) &
                    (day_i < observations[final_observation_col])
                )
                for day_i in range(day_range)
            ],
            'aged_out': [
                np.sum(observations[final_observation_col] <= day_i)
                for day_i in range(day_range)
            ]
        })
        .assign(
            all = pd.col('no_change') + pd.col('change') + pd.col('unknown'),
            ymin = pd.col('no_change') / pd.col('all'),
            ymax = (pd.col('no_change') + pd.col('unknown')) / pd.col('all'),
            day = np.arange(day_range),
            year = pd.col('day') / 365,
        )
    )
    return reshaped


def change_plot_create(
    observations: pd.DataFrame,
    no_change_col: str = 'no_change',
    change_col: str = 'change',
    final_observation_col: str = 'final_obs',
    title: str = None,
    subtitle: str = None,
    x_label: str = '',
    y_label: str = '',
    day_range: int = 365*10,
) -> gg.ggplot:
    """
    Create a single change plot.

    Args:
        observations: DataFrame with observations. Each row is an iteration of a
            tag, with the three columns described below.
        no_change_col: Column name for the days elapsed from when the tag was added to
            when it was last confirmed (observed unchanged).
        change_col: Column name for the days elapsed from when the tag was added to when
            it was changed. For tags that were unchanged, this will be infinity.
        final_observation_col: Column name for the days elapsed from when the tag was
            added to when this data was downloaded.
        day_range: Maximum elapsed time period to plot, in days

    Returns:
        ggplot object
    """
    year_range = day_range / 365
    reshaped = change_plot_reshape_data(
        observations = observations,
        no_change_col = no_change_col,
        change_col = change_col,
        final_observation_col = final_observation_col,
        day_range = day_range
    )
    fig = (
        gg.ggplot(
            data = reshaped,
            mapping = gg.aes(x = 'year', ymin = 'ymin', ymax = 'ymax')
        ) +
        gg.geom_ribbon(fill = 'blue', alpha = 0.4) +
        gg.geom_line(mapping = gg.aes(y = 'ymin'), color = 'black', alpha = 0.5) +
        gg.geom_line(mapping = gg.aes(y = 'ymax'), color = 'black', alpha = 0.5) +
        gg.labs(
            title = title,
            subtitle = subtitle,
            x = x_label,
            y = y_label,
        ) +
        gg.scale_y_continuous(
            limits = (0, 1.01),
            breaks = np.arange(0, 1.01, 0.25),
            labels = [f"{x*100:.0f}%" for x in np.arange(0, 1.01, 0.25)],
        ) +
        gg.scale_x_continuous(
            limits = (0, year_range + 0.01),
            breaks = np.arange(year_range + 1),
            labels = [f"{x:.0f}" for x in np.arange(year_range + 1)],
        ) +
        gg.theme_bw()
    )
    return fig


def change_multiplot_create(
    observations: pd.DataFrame,
    col: str,
    top_n: int = 9,
    no_change_col: str = 'no_change',
    change_col: str = 'change',
    final_observation_col: str = 'final_obs',
    day_range: int = 365*10,
) -> gg.ggplot:
    """
    Create a multi-panel change plot.

    Args:
        col: Column name for the tag to plot.
        top_n: Number of tags to plot, ordered by number of observations.
        **kwargs: Keyword arguments for change_plot_create.

    Returns:
        ggplot object
    """
    # Drop rows where the tag is missing
    # Get the top occurrences of particular tags
    obs_sub = observations.dropna(subset = [col])
    top_tags = obs_sub[col].value_counts().head(top_n)
    # Create a list of ggplot objects
    fig_list = []
    for tag, _ in top_tags.items():
        obs_sub_tag = obs_sub.query(f"{col} == @tag")
        fig = change_plot_create(
            observations = obs_sub_tag,
            title = tag.title(),
            subtitle = f"N = {obs_sub_tag.shape[0]}",
            no_change_col = no_change_col,
            change_col = change_col,
            final_observation_col = final_observation_col,
            day_range = day_range,
        )
        fig_list.append(fig)
    # Compose the individual plots into a roughly square grid
    n_rows = np.ceil(np.sqrt(len(fig_list)))
    composed_rows = [
        reduce(lambda gg1, gg2: gg1 | gg2, row)
        for row in np.array_split(fig_list, n_rows)
    ]
    composed_fig = reduce(lambda row1, row2: row1 / row2, composed_rows)
    return composed_fig
