"""
Exploratory data viz script for OSM observations.

This script:
1. Reads in the OSM observations from a CSV file.
2. Creates time series plots of the observations, showing how many remain open over time.
"""

import numpy as np
import pandas as pd
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import plotnine as gg

from openpois.osm.change_plots import change_plot_create, change_multiplot_create

# ----------------------------------------------------------------------------------------
# Configuration constants
# ----------------------------------------------------------------------------------------

DATA_VERSION = "20260129"
SAVE_DIR = Path("~/data/openpois").expanduser() / DATA_VERSION
VIZ_DIR = SAVE_DIR / "viz"
OSM_KEYS = ["amenity", "shop", "healthcare", "leisure"]
TAG_KEY = "name"
END_DATE = pd.Timestamp('2025-12-31', tz = 'UTC')

max_days = 365 * 10
VIZ_DIR.mkdir(parents = True, exist_ok = True)

# ----------------------------------------------------------------------------------------
# Plotting functions
# ----------------------------------------------------------------------------------------

def fig_save(
    fig: gg.ggplot, stub: str, width: float = 10, height: float = 6, **kwargs
) -> None:
    """
    Helper function to save a ggplot figure
    """
    fig.save(
        filename = VIZ_DIR / f"{stub}.png",
        width = width,
        height = height,
        units = 'in',
        dpi = 300,
        verbose = False,
        **kwargs
    )
    return None


# ----------------------------------------------------------------------------------------
# Main workflow
# ----------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Read observations
    # Drop the first observation for each POI (when the POI was first added) - the last
    #   observation timestamp will be missing for these rows
    timestamp_cols = ['obs_timestamp', 'last_obs_timestamp', 'last_tag_timestamp']
    observations_df = (pd.read_csv(SAVE_DIR / f"osm_observations_{TAG_KEY}.csv")
        .dropna(subset = timestamp_cols)
    )
    for timestamp_col in timestamp_cols:
        observations_df[timestamp_col] = pd.to_datetime(observations_df[timestamp_col])
    # Add a column that is 1 for the highest value of 'version' within each 'id' grouping
    observations_df['latest_version'] = (
        observations_df.groupby('id')['version'].transform(
            lambda x: x == x.max()
        ).astype(int)
    )
    # Prepare timediffs in days:
    # no_change: Time elapsed until the final confirmation of the previous tag
    # change: Time elapsed from previous tag to changed tag
    # final_obs: Time elapsed from previous tag to data download
    changed_tags = (observations_df
        .query('changed == 1')
        .assign(
            no_change = (
                pd.col('last_obs_timestamp') - pd.col('last_tag_timestamp')
            ).dt.days,
            change = (pd.col('obs_timestamp') - pd.col('last_tag_timestamp')).dt.days,
            final_obs = (END_DATE - pd.col('last_tag_timestamp')).dt.days
        )
    )
    unchanged_tags = (observations_df
        .query('(changed == 0) & (latest_version == 1)')
        .assign(
            no_change = (pd.col('obs_timestamp') - pd.col('last_tag_timestamp')).dt.days,
            change = np.inf,
            final_obs = (END_DATE - pd.col('last_tag_timestamp')).dt.days
        )
    )
    # Format changes
    to_plot_df = pd.concat([changed_tags, unchanged_tags])
    # Create a plot for all tags
    fig = change_plot_create(
        observations = to_plot_df,
        no_change_col = 'no_change',
        change_col = 'change',
        final_observation_col = 'final_obs',
        day_range = max_days,
        title = f"Stability of the `{TAG_KEY}` tag over time",
        x_label = "Years since tag",
        y_label = "Proportion remaining unchanged",
    )
    fig_save(fig, stub = f"osm_changes_{TAG_KEY}_all")

    # Create multi-panel plots for the top tags in each OSM category
    TOP_N_TYPES = 10
    for subtype in OSM_KEYS:
        fig = change_multiplot_create(
            observations = to_plot_df,
            col = subtype,
            top_n = TOP_N_TYPES,
            no_change_col = 'no_change',
            change_col = 'change',
            final_observation_col = 'final_obs',
            title = f"Stability of the `{TAG_KEY}` tag over time by {subtype}",
            subtitle = f"Top {TOP_N_TYPES} {subtype} tags by number of observations",
            x_label = "Years since tag",
            y_label = "Proportion remaining unchanged",
            day_range = max_days,
        )
        fig_save(fig = fig, stub = f"osm_changes_{TAG_KEY}_{subtype}")