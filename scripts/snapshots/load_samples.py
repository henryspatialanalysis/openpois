"""
Load the first few rows from each POI snapshot for quick schema inspection.

Reads the first N_ROWS rows from each of the three GeoParquet snapshots
(OSM, Overture, Foursquare) without loading the full files into memory, then
writes the rows (minus geometry) to versioned snippet CSVs for fast reference.
Useful for verifying column names and data types after a new download.

Config keys used (config.yaml):
    snapshot_osm.snapshot          — OSM GeoParquet path
    snapshot_overture.snapshot     — Overture GeoParquet path
    snapshot_foursquare.snapshot   — Foursquare GeoParquet path
    directories.testing            — output directory for snippet CSVs

Output files (in testing directory):
    osm_snippet.csv         — first N_ROWS rows of OSM snapshot (no geometry)
    overture_snippet.csv    — first N_ROWS rows of Overture snapshot (no geometry)
    foursquare_snippet.csv  — first N_ROWS rows of Foursquare snapshot (no geometry)
"""

import pyarrow as pa
import pyarrow.parquet as pq
import geopandas as gpd
from config_versioned import Config

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

OSM_PATH = config.get_file_path("snapshot_osm", "snapshot")
OVERTURE_PATH = config.get_file_path("snapshot_overture", "snapshot")
FOURSQUARE_PATH = config.get_file_path("snapshot_foursquare", "snapshot")

N_ROWS = 100

TESTING_DIR = config.get_dir_path("testing")
TESTING_DIR.mkdir(parents = True, exist_ok = True)


# -----------------------------------------------------------------------------
# Helper
# -----------------------------------------------------------------------------

def load_head(path, n = N_ROWS) -> gpd.GeoDataFrame:
    """
    Read the first n rows from a GeoParquet file without loading it all.

    Args:
        path: Path to the GeoParquet file.
        n: Number of rows to read.

    Returns:
        GeoDataFrame with the first n rows.
    """
    pf = pq.ParquetFile(path)
    batch = next(pf.iter_batches(batch_size = n))
    return gpd.GeoDataFrame.from_arrow(pa.Table.from_batches([batch]))


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Loading first {N_ROWS} rows from each snapshot...\n")

    osm = load_head(OSM_PATH)
    print(f"Columns: {list(osm.columns)}\n")
    config.write(
        osm.drop(columns = "geometry"),
        'testing', 'osm_snippet'
    )

    overture = load_head(OVERTURE_PATH)
    print(f"Columns: {list(overture.columns)}\n")
    config.write(
        overture.drop(columns = "geometry"),
        'testing', 'overture_snippet'
    )
    foursquare = load_head(FOURSQUARE_PATH)
    print(f"Columns: {list(foursquare.columns)}\n")
    config.write(
        foursquare.drop(columns = "geometry"),
        'testing', 'foursquare_snippet'
    )
