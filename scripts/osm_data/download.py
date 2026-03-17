"""
Download OSM element change histories for a configured bbox and date range.

Uses the Overpass API to collect element IDs for each configured tag key across
a series of snapshot dates, then fetches the full version history of each
element via the OSM API.

Config keys used (config.yaml):
    download.general.bbox            — WGS-84 bbox [xmin, ymin, xmax, ymax]
    download.general.timeout         — request timeout in seconds
    download.osm.start_date          — earliest snapshot date (min: 2012-09-13)
    download.osm.end_date            — latest snapshot date
    download.osm.date_interval_days  — spacing between Overpass queries in days
    download.download_keys           — OSM tag keys to search for (e.g. amenity, shop)
    directories.osm_data             — output directory

Output files (in osm_data directory):
    osm_elements.csv          — element IDs observed at each snapshot date
    osm_versions.csv          — one row per element version with all tag fields
    osm_changes.csv           — one row per version pair flagging tag changes
    osm_failed_elements.csv   — elements whose history could not be retrieved
"""
import datetime
from config_versioned import Config
from openpois.io.osm_history import (
    build_date_range,
    collect_element_ids,
    download_element_histories,
)

# -----------------------------------------------------------------------------
# Configuration constants
# -----------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

TIMEOUT = config.get("download", "general", "timeout")
BBOX = config.get("download", "general", "bbox")
# Earliest option is September 13, 2012
START_DATE = datetime.datetime.combine(
    config.get("download", "osm", "start_date"), datetime.time.min
)
END_DATE = datetime.datetime.combine(
    config.get("download", "osm", "end_date"), datetime.time.min
)  # Latest
DATE_INTERVAL = datetime.timedelta(
    days=config.get("download", "osm", "date_interval_days")
)
OSM_KEYS = config.get("download", "download_keys")
SAVE_DIR = config.get_dir_path("osm_data")

SAVE_DIR.mkdir(parents=True, exist_ok=True)


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
    config.write(elements_table, "osm_data", "osm_elements")
    print(
        f"Succeeded on {len(succeed_dates)} dates, "
        f"failed on {len(failed_dates)}"
    )

    # Download element histories
    versions_df, changes_df, failed_rows = download_element_histories(
        elements_table = elements_table,
        timeout = TIMEOUT,
        progress = True,
    )

    # Save results
    config.write(versions_df, "osm_data", "osm_versions")
    config.write(changes_df, "osm_data", "osm_changes")
    print(f"Saved {len(versions_df)} versions and {len(changes_df)} changes")
    config.write(failed_rows, "osm_data", "osm_failed_elements")
    print(f"Failed on {len(failed_rows)} elements")
