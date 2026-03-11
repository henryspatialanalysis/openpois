"""
Exploratory script for downloading OSM data.

This script uses the openpois.osm_download module to:
1. Collect element IDs across a date range using the Overpass API.
2. Download element histories from the OSM API.
3. Save the results to CSV files.
"""
import datetime
from config_versioned import Config
from openpois.osm.download import (
    build_date_range,
    collect_element_ids,
    download_element_histories,
)

# -----------------------------------------------------------------------------
# Configuration constants
# -----------------------------------------------------------------------------

_cfg = Config("~/repos/openpois/config.yaml")

TIMEOUT = _cfg.get("download", "timeout")
BBOX = _cfg.get("download", "bbox")
# Earliest option is September 13, 2012
START_DATE = datetime.datetime.combine(
    _cfg.get("download", "start_date"), datetime.time.min
)
END_DATE = datetime.datetime.combine(_cfg.get("end_date"), datetime.time.min)  # Latest
DATE_INTERVAL = datetime.timedelta(days=_cfg.get("download", "date_interval_days"))
OSM_KEYS = _cfg.get("osm_keys")
SAVE_DIR = _cfg.get_dir_path("osm_download")

SAVE_DIR.mkdir(parents=True, exist_ok=True)


# -----------------------------------------------------------------------------
# Main workflow
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    # Create date range
    date_range = build_date_range(
        start_date=START_DATE,
        end_date=END_DATE,
        interval=DATE_INTERVAL,
    )

    # Collect element IDs from Overpass
    elements_table, succeed_dates, failed_dates = collect_element_ids(
        date_range=date_range,
        bbox=BBOX,
        osm_keys=OSM_KEYS,
        timeout=TIMEOUT,
    )

    # Save elements table
    _cfg.write(elements_table, "osm_download", "osm_elements")
    print(
        f"Succeeded on {len(succeed_dates)} dates, "
        f"failed on {len(failed_dates)}"
    )

    # Download element histories
    versions_df, changes_df, failed_rows = download_element_histories(
        elements_table=elements_table,
        timeout=TIMEOUT,
        progress=True,
    )

    # Save results
    _cfg.write(versions_df, "osm_download", "osm_versions")
    _cfg.write(changes_df, "osm_download", "osm_changes")
    print(f"Saved {len(versions_df)} versions and {len(changes_df)} changes")
    _cfg.write(failed_rows, "osm_download", "osm_failed_elements")
    print(f"Failed on {len(failed_rows)} elements")
