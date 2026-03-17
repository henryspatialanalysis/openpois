"""
Reformat raw OSM version histories into modelling-ready observations.

Reads osm_versions.csv and osm_changes.csv produced by osm_data/download.py,
then converts them into an observation-per-version format suitable for the
change-rate model. Each observation records the tag value, the timestamps of
the previous tag assignment and the current observation, and a flag for whether
the tag changed.

Config keys used (config.yaml):
    directories.osm_data     — directory containing input and output CSVs
    download.download_keys   — all tag keys collected (passed as keep_keys)
    osm_data.tag_key         — single tag key to model (e.g. "amenity")

Prerequisites:
    Run osm_data/download.py first to produce osm_versions.csv and osm_changes.csv.

Output file (in osm_data directory):
    osm_observations_{tag_key}.csv — one row per version observation with columns:
        id, version, tag_key, last_tag_timestamp, obs_timestamp, changed,
        plus all keep_keys columns for grouping
"""

import pandas as pd
from config_versioned import Config

from openpois.osm.format_observations import format_observations


# ----------------------------------------------------------------------------------------
# Configuration constants
# ----------------------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

SAVE_DIR = config.get_dir_path("osm_data")
OSM_KEYS = config.get("download", "download_keys")
TAG_KEY = config.get("osm_data", "tag_key")


# ----------------------------------------------------------------------------------------
# Main workflow
# ----------------------------------------------------------------------------------------

if __name__ == "__main__":
    # Read files
    changes_df = pd.read_csv(SAVE_DIR / "osm_changes.csv")
    versions_df = pd.read_csv(SAVE_DIR / "osm_versions.csv")

    # Format changes and versions into observations
    observations_df = format_observations(
        changes_df = changes_df,
        versions_df = versions_df,
        tag_key = TAG_KEY,
        keep_keys = OSM_KEYS,
    )

    # Save observations
    out_path = SAVE_DIR / f"osm_observations_{TAG_KEY}.csv"
    observations_df.to_csv(out_path, index=False)
    print(f"Saved {len(observations_df)} observations to {out_path}")
