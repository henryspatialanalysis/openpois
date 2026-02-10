"""
Exploratory data viz script for OSM observations.

This script:
1. Reads in the OSM observations from a CSV file.
2. Creates time series plots of the observations, showing how many remain open over time.
"""

import numpy as np
import pandas as pd
from pathlib import Path
import plotnine as gg

# ----------------------------------------------------------------------------------------
# Configuration constants
# ----------------------------------------------------------------------------------------

DATA_VERSION = "20260129"
SAVE_DIR = Path("~/data/openpois").expanduser() / DATA_VERSION
OSM_KEYS = ["amenity", "shop", "healthcare", "leisure"]
TAG_KEY = "name"
END_DATE = pd.Timestamp('2025-12-31', tz = 'UTC')

max_days = 365*10

# ----------------------------------------------------------------------------------------
# Main workflow
# ----------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Read observations
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
    # t1: Time elapsed until the final confirmation of the previous tag
    # t2: Time elapsed from previous tag to changed tag
    changed_tags = (observations_df
        .query('changed == 1')
        .assign(
            t1 = (pd.col('last_obs_timestamp') - pd.col('last_tag_timestamp')).dt.days,
            t2 = (pd.col('obs_timestamp') - pd.col('last_tag_timestamp')).dt.days,
            t3 = np.inf # (END_DATE - pd.col('last_tag_timestamp')).dt.days
        )
    )
    unchanged_tags = (observations_df
        .query('(changed == 0) & (latest_version == 1)')
        .assign(
            t1 = (pd.col('obs_timestamp') - pd.col('last_tag_timestamp')).dt.days,
            t2 = np.inf, # (END_DATE - pd.col('last_tag_timestamp')).dt.days,
            t3 = np.inf
        )
    )
    # Format changes
    to_plot_df = pd.concat([changed_tags, unchanged_tags])
    # Create a plot
    reshaped_df = (
        pd.DataFrame({
            'yes': [np.sum(day_i < to_plot_df['t1']) for day_i in range(max_days)],
            'unknown': [
                np.sum((to_plot_df['t1'] <= day_i) & (day_i < to_plot_df['t2']))
                for day_i in range(max_days)
            ],
            'no': [
                np.sum((to_plot_df['t2'] <= day_i) & (day_i < to_plot_df['t3']))
                for day_i in range(max_days)
            ],
        })
        .assign(
            all = pd.col('yes') + pd.col('no') + pd.col('unknown'),
            ymin = pd.col('yes') / pd.col('all'),
            ymax = (pd.col('yes') + pd.col('unknown')) / pd.col('all'),
            year = np.arange(max_days) / 365,
        )
    )
    fig = (
        gg.ggplot(
            reshaped_df,
            gg.aes(x = 'year', ymin = 'ymin', ymax = 'ymax')) +
        gg.geom_ribbon(fill = 'blue', alpha = 0.4) +
        gg.geom_line(gg.aes(y = 'ymin'), color = 'black', alpha = 0.5) +
        gg.geom_line(gg.aes(y = 'ymax'), color = 'black', alpha = 0.5) +
        gg.labs(
            x = "Years from tag",
            y = "Proportion remaining unchanged",
            title = f"Proportion of `{TAG_KEY}` tags unchanged over time"
        ) +
        gg.scale_y_continuous(
            limits = (0, 1.01),
            breaks = np.arange(0, 1, 0.25),
            labels = [f"{x*100:.0f}%" for x in np.arange(0, 1, 0.25)]
        ) +
        gg.theme_bw()
    )
    fig.save(
        SAVE_DIR / f"osm_observations_{TAG_KEY}.png",
        width = 10,
        height = 6,
        units = 'in',
        dpi = 300,
    )