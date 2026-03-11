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

_cfg = Config("~/repos/openpois/config.yaml")

PBF_URL = _cfg.get("osm_snapshot", "pbf_url")
OSM_KEYS = _cfg.get("osm_keys")
SAVE_DIR = _cfg.get_dir_path("osm_snapshot")

SAVE_DIR.mkdir(parents=True, exist_ok=True)

RAW_PBF = SAVE_DIR / _cfg.get("directories", "osm_snapshot", "files", "raw_pbf")
FILTERED_PBF = SAVE_DIR / _cfg.get(
    "directories", "osm_snapshot", "files", "filtered_pbf"
)
OUTPUT_PATH = SAVE_DIR / _cfg.get("directories", "osm_snapshot", "files", "snapshot")


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
