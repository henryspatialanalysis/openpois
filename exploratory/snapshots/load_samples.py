"""
Load the first few rows from each POI snapshot (OSM, Overture, Foursquare)
as GeoDataFrames for quick inspection.
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


# -----------------------------------------------------------------------------
# Helper
# -----------------------------------------------------------------------------

def load_head(path, n = N_ROWS) -> gpd.GeoDataFrame:
    """Read the first `n` rows from a GeoParquet file without loading it all."""
    pf = pq.ParquetFile(path)
    batch = next(pf.iter_batches(batch_size=n))
    return gpd.GeoDataFrame.from_arrow(pa.Table.from_batches([batch]))


# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Loading first {N_ROWS} rows from each snapshot...\n")

    osm = load_head(OSM_PATH)
    print(f"Columns: {list(osm.columns)}\n")
    config.write(
        osm.drop_geometry(),
        'testing', 'osm_snippet'
    )

    overture = load_head(OVERTURE_PATH)
    print(f"Columns: {list(overture.columns)}\n")
    config.write(
        overture.drop_geometry(),
        'testing', 'overture_snippet'
    )
    foursquare = load_head(FOURSQUARE_PATH)
    print(f"Columns: {list(foursquare.columns)}\n")
    config.write(
        foursquare.drop_geometry(),
        'testing', 'foursquare_snippet'
    )
