"""Utilities for spatially partitioning GeoDataFrames by geohash for web map serving."""
import shutil
import warnings
from pathlib import Path

import geopandas as gpd
import pygeohash


def add_geohash_columns(
    gdf: gpd.GeoDataFrame,
    precision_partition: int,
    precision_sort: int,
) -> gpd.GeoDataFrame:
    """Add geohash_prefix (partition key) and geohash_sort columns from centroids.

    Rows with null or empty geometries are dropped before computing hashes.
    Both columns are derived from the geometry centroid, so Points, Polygons,
    and MultiPolygons are all handled uniformly.
    """
    gdf = gdf[~gdf.geometry.is_empty & gdf.geometry.notna()].copy()
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", "Geometry is in a geographic CRS", UserWarning)
        centroids = gdf.geometry.centroid
    gdf["geohash_prefix"] = [
        pygeohash.encode(g.y, g.x, precision = precision_partition)
        for g in centroids
    ]
    gdf["geohash_sort"] = [
        pygeohash.encode(g.y, g.x, precision = precision_sort)
        for g in centroids
    ]
    return gdf


def write_partitioned_dataset(
    gdf: gpd.GeoDataFrame,
    output_dir,
    overwrite: bool = True,
) -> None:
    """Sort gdf spatially and write as a geohash-partitioned parquet dataset.

    Writes one parquet file per geohash_prefix value into a Hive-style directory
    layout (geohash_prefix=9q/part-0.parquet). Converts and writes one partition
    at a time to avoid duplicating the full dataset in memory.

    The geohash_prefix column becomes the Hive partition directory name and is
    dropped from the stored parquet files. The geohash_sort column is used only
    for row ordering and is also dropped before writing.
    """
    output_dir = Path(output_dir)

    if output_dir.exists():
        if overwrite:
            print(f"Removing existing output: {output_dir}")
            shutil.rmtree(output_dir)
        else:
            raise FileExistsError(
                f"Output directory already exists: {output_dir}. "
                "Pass overwrite=True to replace it."
            )

    gdf = gdf.sort_values(["geohash_prefix", "geohash_sort"]).drop(
        columns = ["geohash_sort"]
    )
    cols = [c for c in gdf.columns if c != "geohash_prefix"]
    n_partitions = gdf["geohash_prefix"].nunique()
    output_dir.mkdir(parents = True, exist_ok = True)

    print(f"Writing {n_partitions} partitions to {output_dir} ...")
    for i, (prefix, group) in enumerate(gdf.groupby("geohash_prefix")):
        partition_dir = output_dir / f"geohash_prefix={prefix}"
        partition_dir.mkdir()
        group[cols].to_parquet(partition_dir / "part-0.parquet")
        if (i + 1) % 100 == 0:
            print(f"  {i + 1}/{n_partitions} partitions written...")
