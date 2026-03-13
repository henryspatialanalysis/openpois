"""
Exploratory script for downloading a current OSM POI snapshot for the US.

This script uses openpois.osm.snapshot to:
1. Download the Geofabrik US PBF extract (~11 GB, skipped if already present).
2. Filter to nodes and ways with matching tag keys using osmium tags-filter.
3. Parse with pyosmium into a GeoDataFrame and save as GeoParquet.

Requires osmium-tool on PATH (conda install -c conda-forge osmium-tool).
"""
from config_versioned import Config
from openpois.osm.snapshot import download_osm_snapshot

# -----------------------------------------------------------------------------
# Configuration constants
# -----------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

PBF_URL = config.get("download", "osm", "pbf_url")
OSM_KEYS = config.get("download", "download_keys")
SAVE_DIR = config.get_dir_path("snapshot_osm")

SAVE_DIR.mkdir(parents=True, exist_ok=True)

RAW_PBF = config.get_file_path("snapshot_osm", "raw_pbf")
FILTERED_PBF = config.get_file_path("snapshot_osm", "filtered_pbf")
OUTPUT_PATH = config.get_file_path("snapshot_osm", "snapshot")


# -----------------------------------------------------------------------------
# Main workflow
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    gdf = download_osm_snapshot(
        pbf_url=PBF_URL,
        raw_pbf_path=RAW_PBF,
        filtered_pbf_path=FILTERED_PBF,
        output_path=OUTPUT_PATH,
        osm_keys=OSM_KEYS,
    )
    print(f"Saved {len(gdf):,} OSM POIs to {OUTPUT_PATH}")
