"""
Plot OSM tag stability curves from observation data.

Reads osm_observations_{tag_key}.csv and computes Kaplan-Meier-style survival
estimates showing what fraction of tag assignments remain unchanged over time.
Saves two types of PNG figures:
    1. Overall stability curve — all tags pooled into a single panel.
    2. Per-subtype multi-panel curves — top-N values for each key in
       download_keys, shown as separate facets on one figure per key.

Config keys used (config.yaml):
    directories.osm_data           — directory containing input CSV and viz/ output
    download.download_keys         — tag keys used as grouping variables for subplots
    osm_data.tag_key               — the tag being analysed (e.g. "amenity")
    osm_data.timestamp_cols        — columns to parse as timestamps (rows with nulls dropped)
    osm_data.top_n_types           — number of top subtype values per multi-panel figure
    download.osm.end_date          — right-censoring date for still-unchanged tags

Prerequisites:
    Run osm_data/format_tabular.py first.

Output files (in osm_data/viz/):
    osm_changes_{tag_key}_all.png             — overall survival curve
    osm_changes_{tag_key}_{key}.png           — per-subtype facet grid, one per key
"""

import numpy as np
import pandas as pd
from config_versioned import Config

import matplotlib
matplotlib.use("Agg")  # noqa: E402
import plotnine as gg  # noqa: E402

from openpois.osm.change_plots import (  # noqa: E402
    change_plot_create, change_multiplot_create
)

# ----------------------------------------------------------------------------------------
# Configuration constants
# ----------------------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

SAVE_DIR = config.get_dir_path("osm_data")
VIZ_DIR = SAVE_DIR / "viz"
OSM_KEYS = config.get("download", "download_keys")
TAG_KEY = config.get("osm_data", "tag_key")
END_DATE = pd.Timestamp(config.get("download", "osm", "end_date"), tz='UTC')

max_days = 365 * 10
VIZ_DIR.mkdir(parents=True, exist_ok=True)

# ----------------------------------------------------------------------------------------
# Plotting functions
# ----------------------------------------------------------------------------------------


def fig_save(
    fig: gg.ggplot, stub: str, width: float = 10, height: float = 6, **kwargs
) -> None:
    """
    Save a ggplot figure as a PNG file to VIZ_DIR.

    Args:
        fig: The ggplot figure to save.
        stub: Output filename stem (without extension).
        width: Figure width in inches.
        height: Figure height in inches.
        **kwargs: Additional keyword arguments forwarded to fig.save().
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


# ----------------------------------------------------------------------------------------
# Main workflow
# ----------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Read observations
    # Drop the first observation for each POI (when the POI was first added) - the last
    #   observation timestamp will be missing for these rows
    timestamp_cols = config.get("osm_data", "timestamp_cols")
    observations_df = (
        pd.read_csv(SAVE_DIR / f"osm_observations_{TAG_KEY}.csv")
        .dropna(subset = timestamp_cols)
    )
    for timestamp_col in timestamp_cols:
        observations_df[timestamp_col] = pd.to_datetime(observations_df[timestamp_col])
    # Add a column that is 1 for the highest value of 'version' within each 'id' grouping
    observations_df['latest_version'] = (
        observations_df.groupby('id')['version']
        .transform(lambda x: x == x.max())
        .astype(int)
    )
    # Prepare timediffs in days:
    # no_change: Time elapsed until the final confirmation of the previous tag
    # change: Time elapsed from previous tag to changed tag
    # final_obs: Time elapsed from previous tag to data download
    changed_tags = (
        observations_df
        .query('changed == 1')
        .assign(
            no_change=(
                pd.col('last_obs_timestamp') - pd.col('last_tag_timestamp')
            ).dt.days,
            change=(pd.col('obs_timestamp') - pd.col('last_tag_timestamp')).dt.days,
            final_obs=(END_DATE - pd.col('last_tag_timestamp')).dt.days
        )
    )
    unchanged_tags = (
        observations_df
        .query('(changed == 0) & (latest_version == 1)')
        .assign(
            no_change=(pd.col('obs_timestamp') - pd.col('last_tag_timestamp')).dt.days,
            change=np.inf,
            final_obs=(END_DATE - pd.col('last_tag_timestamp')).dt.days
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
    TOP_N_TYPES = config.get("osm_data", "top_n_types")
    for subtype in OSM_KEYS:
        fig = change_multiplot_create(
            observations = to_plot_df,
            col = subtype,
            top_n = TOP_N_TYPES,
            no_change_col = 'no_change',
            change_col = 'change',
            final_observation_col = 'final_obs',
            title = f"Stability of the `{TAG_KEY}` tag over time by {subtype}",
            subtitle = (
                f"Top {TOP_N_TYPES} {subtype} tags by number of observations"
            ),
            x_label = "Years since tag",
            y_label = "Proportion remaining unchanged",
            day_range = max_days,
        )
        fig_save(fig = fig, stub = f"osm_changes_{TAG_KEY}_{subtype}")
