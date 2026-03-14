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
FILTER_KEYS = config.get("download", "osm", "filter_keys")
EXTRACT_KEYS = config.get("download", "osm", "extract_keys")
OVERWRITE_DOWNLOAD = config.get("download", "osm", "overwrite_download")
OVERWRITE_FILTER = config.get("download", "osm", "overwrite_filter")
SOURCE_LABEL = config.get("download", "osm", "source_label")
KEEP_ALL_KEYS = config.get("download", "osm", "keep_all_keys")
CHUNK_SIZE = config.get("download", "osm", "chunk_size")
MAX_AREA_NODES = config.get("download", "osm", "max_area_nodes", fail_if_none = False)
VERBOSE = config.get("download", "osm", "verbose")
SAVE_DIR = config.get_dir_path("snapshot_osm")
CHUNK_DIR = config.get_dir_path("snapshot_osm")

SAVE_DIR.mkdir(parents=True, exist_ok=True)

RAW_PBF = config.get_file_path("snapshot_osm", "raw_pbf")
FILTERED_PBF = config.get_file_path("snapshot_osm", "filtered_pbf")
OUTPUT_PATH = config.get_file_path("snapshot_osm", "snapshot")


# -----------------------------------------------------------------------------
# Main workflow
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    gdf = download_osm_snapshot(
        pbf_url = PBF_URL,
        raw_pbf_path = RAW_PBF,
        filtered_pbf_path = FILTERED_PBF,
        output_path = OUTPUT_PATH,
        filter_keys = FILTER_KEYS,
        extract_keys = EXTRACT_KEYS,
        overwrite_download = OVERWRITE_DOWNLOAD,
        overwrite_filter = OVERWRITE_FILTER,
        source_label = SOURCE_LABEL,
        keep_all_keys = KEEP_ALL_KEYS,
        chunk_size = CHUNK_SIZE,
        max_area_nodes = MAX_AREA_NODES,
        chunk_dir = CHUNK_DIR,
        verbose = VERBOSE,
    )
    print(f"Saved {len(gdf):,} OSM POIs to {OUTPUT_PATH}")
