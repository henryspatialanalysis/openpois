"""
Download the current US OpenStreetMap POI snapshot as a GeoParquet file.

Downloads the Geofabrik North America PBF extract (~11 GB), uses osmium
tags-filter to extract nodes and ways matching the configured tag keys, then
parses the result with pyosmium into a GeoDataFrame and saves as GeoParquet.
Incremental: skips the PBF download or filter step if output files already
exist (controlled by overwrite_download and overwrite_filter config flags).

Note: osmium is resolved from the conda env bin rather than the shell PATH;
no manual PATH modification is needed.

Config keys used (config.yaml):
    download.osm.pbf_url             — Geofabrik PBF URL (North America extract)
    download.osm.filter_keys         — OSM tag keys to retain (e.g. amenity, shop)
    download.osm.extract_keys        — tag keys to include as output columns
    download.osm.overwrite_download  — re-download PBF even if it already exists
    download.osm.overwrite_filter    — re-run osmium filter even if output exists
    download.osm.source_label        — value written to the "source" column
    download.osm.keep_all_keys       — retain all discovered tag columns in output
    download.osm.chunk_size          — number of elements per pyosmium parse chunk
    download.osm.max_area_nodes      — skip way geometries with more nodes than this
    download.osm.verbose             — print progress during PBF parsing
    directories.snapshot_osm         — output directory; also used for temp PBF files

Output file:
    osm_snapshot.parquet — GeoParquet with ~7.8M US POIs (nodes + area centroids)
        Columns: osm_id, osm_type, name, geometry, last_edited, source,
        plus all extract_keys columns
"""
from config_versioned import Config
from openpois.io.osm_snapshot import download_osm_snapshot

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
