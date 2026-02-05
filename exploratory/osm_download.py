"""
Exploratory script for downloading OSM data.

This script uses the openpois.osm_download module to:
1. Collect element IDs across a date range using the Overpass API.
2. Download element histories from the OSM API.
3. Save the results to CSV files.
"""
import datetime
import os
from openpois.osm_download import (
    build_date_range,
    collect_element_ids,
    download_element_histories,
)

# -----------------------------------------------------------------------------
# Configuration constants
# -----------------------------------------------------------------------------

TIMEOUT = 1000
BBOX = {"ymin": 47.41, "xmin": -122.48, "ymax": 47.79, "xmax": -122.16}
START_DATE = datetime.datetime(2016, 1, 1)  # Earliest option is September 13, 2012
END_DATE = datetime.datetime(2025, 12, 31)  # Latest
DATE_INTERVAL = datetime.timedelta(days = 7)
OSM_KEYS = ["amenity", "shop", "healthcare", "leisure"]
SAVE_DIR = "~/data/osm_example_data"

os.makedirs(SAVE_DIR, exist_ok = True)


# -----------------------------------------------------------------------------
# Main workflow
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # Create date range
    date_range = build_date_range(
        start_date = START_DATE,
        end_date = END_DATE,
        interval = DATE_INTERVAL,
    )

    # Collect element IDs from Overpass
    elements_table, succeed_dates, failed_dates = collect_element_ids(
        date_range = date_range,
        bbox = BBOX,
        osm_keys = OSM_KEYS,
        timeout = TIMEOUT,
    )

    # Save elements table
    elements_table.to_csv(
        os.path.join(SAVE_DIR, "osm_elements.csv"),
        index = False,
    )
    print(f"Saved {len(elements_table)} elements to osm_elements.csv")
    print(f"Succeeded on {len(succeed_dates)} dates, failed on {len(failed_dates)}")

    # Download element histories
    versions_df, changes_df, failed_rows = download_element_histories(
        elements_table = elements_table,
        timeout = TIMEOUT,
        progress = True,
    )

    # Save results
    versions_df.to_csv(
        os.path.join(SAVE_DIR, "osm_versions.csv"),
        index=False,
    )
    changes_df.to_csv(
        os.path.join(SAVE_DIR, "osm_changes.csv"),
        index = False,
    )
    print(f"Saved {len(versions_df)} versions and {len(changes_df)} changes")

    print(f"Failed on {len(failed_rows)} elements")
    failed_elements = elements_table.iloc[failed_rows, :]
    failed_elements.to_csv(
        os.path.join(SAVE_DIR, "osm_failed_elements.csv"),
        index = False,
    )