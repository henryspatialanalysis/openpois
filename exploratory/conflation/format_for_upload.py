"""
Spatially partition the conflated POI dataset for optimized web map viewport queries.

Reads conflated.parquet, adds a geohash-4 partition column computed from each POI's
centroid, sorts rows within partitions by geohash-6 for spatial locality, and writes
a Hive-style partitioned dataset:

    conflated_partitioned/
        geohash_prefix=9q/
            part-0.parquet
        geohash_prefix=dr/
            part-0.parquet
        ...

Clients can fetch only the geohash cells covering their map viewport, avoiding a full
dataset scan.
"""
import geopandas as gpd
from config_versioned import Config

from openpois.io.geohash_partition import add_geohash_columns, write_partitioned_dataset

# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

INPUT_PATH = config.get_file_path("conflation", "conflated")
OUTPUT_DIR = config.get_file_path("conflation", "partitioned")
OVERWRITE = True

PRECISION_PARTITION = config.get("upload", "geohash_precision_partition")
PRECISION_SORT = config.get("upload", "geohash_precision_sort")

# -----------------------------------------------------------------------------
# Main workflow
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    print(f"Reading conflated dataset from {INPUT_PATH} ...")
    gdf = gpd.read_parquet(INPUT_PATH)
    print(f"Loaded {len(gdf):,} POIs")

    print("Computing geohash columns from centroids ...")
    gdf = add_geohash_columns(
        gdf,
        precision_partition = PRECISION_PARTITION,
        precision_sort = PRECISION_SORT,
    )

    write_partitioned_dataset(gdf, output_dir = OUTPUT_DIR, overwrite = OVERWRITE)

    n_partitions = sum(1 for _ in OUTPUT_DIR.iterdir() if _.is_dir())
    print(f"Done. Wrote {len(gdf):,} rows across {n_partitions} geohash partitions.")
    print(f"Output: {OUTPUT_DIR}")
