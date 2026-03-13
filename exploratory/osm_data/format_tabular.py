"""
Exploratory script for reformatting OSM data into a tabular format.

This script:
1. Reads in the OSM versions and changes data from CSV files.
2. Reconfigures POI changesets into 'observations', which are either changes to the
   relevant POI tag or confirmation that the tag is unchanged.
3. Saves the observations to a new CSV file.
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
