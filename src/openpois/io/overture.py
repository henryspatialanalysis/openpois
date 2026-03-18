#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module downloads a current/latest Overture Maps Places snapshot for a
given bounding box and set of taxonomy categories.

It is broken into the following functions:
- get_latest_release_date: Finds the most recent Overture release by listing S3.
- build_overture_s3_path: Constructs the S3 glob path for a given release.
- download_overture_snapshot: Queries S3 via DuckDB, filters by bbox and
    taxonomy, and writes a GeoParquet file.

Data source: s3://overturemaps-us-west-2/release/ (public, no auth required).

Category filtering uses the `taxonomy` array field. The first element
(taxonomy[1] in SQL 1-based indexing) is the L0 category. The deprecated
`categories.primary` field must NOT be used; it is removed in June 2026.
"""
from __future__ import annotations

import xml.etree.ElementTree as ET
from pathlib import Path

import duckdb
import geopandas as gpd
import requests


# -----------------------------------------------------------------------------
# Release discovery
# -----------------------------------------------------------------------------


def get_latest_release_date(
    bucket: str,
) -> str:
    """
    Finds the most recent Overture Maps release date by listing the S3 bucket.

    Queries the public S3 HTTP API for prefix listings under the 'release/'
    key and returns the lexicographically largest date string found.

    Args:
        bucket: The S3 bucket name hosting Overture releases.

    Returns:
        Release date string in the format 'YYYY-MM-DD.N' as it appears in S3
        (e.g., '2026-02-18.0').

    Raises:
        requests.HTTPError: If the S3 list request fails.
        ValueError: If no release prefixes are found in the bucket.
    """
    s3_list_url = (
        f"https://{bucket}.s3.amazonaws.com"
        "/?list-type=2&prefix=release%2F&delimiter=%2F"
    )
    resp = requests.get(s3_list_url, timeout = 30)
    resp.raise_for_status()

    root = ET.fromstring(resp.text)
    ns = {"s3": "http://s3.amazonaws.com/doc/2006-03-01/"}
    prefixes = [
        el.text.rstrip("/").removeprefix("release/")
        for el in root.findall(".//s3:CommonPrefixes/s3:Prefix", ns)
    ]

    if not prefixes:
        raise ValueError(
            f"No release prefixes found in s3://{bucket}/release/. "
            "Check that the bucket is accessible."
        )

    return sorted(prefixes)[-1]


def build_overture_s3_path(
    release_date: str,
    bucket: str,
) -> str:
    """
    Returns the S3 glob path for all Places Parquet files in a given release.

    Args:
        release_date: Release identifier as returned by get_latest_release_date
            (e.g., '2026-02-18.0').
        bucket: The S3 bucket name.

    Returns:
        S3 path string suitable for DuckDB read_parquet(), e.g.:
        's3://overturemaps-us-west-2/release/2026-02-18.0/
            theme=places/type=place/*.parquet'
    """
    return (
        f"s3://{bucket}/release/{release_date}"
        "/theme=places/type=place/*.parquet"
    )


# -----------------------------------------------------------------------------
# Download
# -----------------------------------------------------------------------------


def download_overture_snapshot(
    output_path: Path,
    taxonomy_l0_categories: list[str],
    bbox: dict,
    bucket: str,
    s3_region: str,
    release_date: str | None = None,
    source_label: str = "overture",
) -> gpd.GeoDataFrame:
    """
    Downloads filtered Overture Maps Places data and saves it as GeoParquet.

    Uses DuckDB with the httpfs and spatial extensions to query the public
    Overture Maps S3 bucket directly. Applies predicate pushdown via the
    bbox struct columns and filters by taxonomy L0 category.

    The geometry column in the source data is WKB-encoded. This function
    decodes it into a proper GeoPandas geometry column and sets the CRS to
    EPSG:4326 before saving.

    Args:
        output_path: Path to write the output GeoParquet file.
        taxonomy_l0_categories: List of Overture taxonomy L0 values to retain.
            Valid values (from S3 data as of 2026-02-18): 'food_and_drink',
            'shopping', 'arts_and_entertainment', 'sports_and_recreation',
            'health_care', 'services_and_business',
            'travel_and_transportation', 'lifestyle_services', 'education',
            'community_and_government', 'cultural_and_historic', 'lodging',
            'geographic_entities'.
            See: https://docs.overturemaps.org/guides/places/taxonomy/
        bbox: Bounding box dict with keys 'xmin', 'ymin', 'xmax', 'ymax'
            in WGS84 degrees.
        release_date: Overture release identifier (e.g., '2026-02-18.0').
            If None, the latest release is fetched automatically.
        bucket: S3 bucket name hosting Overture releases.
        s3_region: AWS region of the S3 bucket.
        source_label: Value for the output 'source' column.

    Returns:
        GeoDataFrame with schema:
            source (str), overture_id (str), release_date (str),
            taxonomy_l0 (str), taxonomy_l1 (str, nullable),
            taxonomy_l2 (str, nullable),
            overture_name (str, nullable), brand_name (str, nullable,
            from brand.names.primary), confidence (float64, nullable),
            geometry (Point, EPSG:4326)
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    if release_date is None:
        print("Detecting latest Overture release...")
        release_date = get_latest_release_date(bucket = bucket)
        print(f"Using release: {release_date}")

    s3_path = build_overture_s3_path(release_date, bucket = bucket)

    categories_sql = ", ".join(f"'{c}'" for c in taxonomy_l0_categories)

    query = f"""
        SELECT
            '{source_label}' AS source,
            id AS overture_id,
            '{release_date}' AS release_date,
            taxonomy.hierarchy[1] AS taxonomy_l0,
            taxonomy.hierarchy[2] AS taxonomy_l1,
            taxonomy.hierarchy[3] AS taxonomy_l2,
            names.primary AS overture_name,
            brand.names.primary AS brand_name,
            confidence,
            ST_X(geometry) AS longitude,
            ST_Y(geometry) AS latitude
        FROM read_parquet('{s3_path}', hive_partitioning=1)
        WHERE
            bbox.xmin >= {bbox['xmin']}
            AND bbox.xmax <= {bbox['xmax']}
            AND bbox.ymin >= {bbox['ymin']}
            AND bbox.ymax <= {bbox['ymax']}
            AND taxonomy.hierarchy[1] IN ({categories_sql})
    """

    print(f"Querying Overture S3 at {s3_path}...")
    conn = duckdb.connect()
    conn.execute("INSTALL httpfs; LOAD httpfs;")
    conn.execute("INSTALL spatial; LOAD spatial;")
    conn.execute(f"SET s3_region='{s3_region}';")

    df = conn.execute(query).df()
    conn.close()

    print(f"Downloaded {len(df):,} Overture places. Building GeoDataFrame...")
    gdf = gpd.GeoDataFrame(
        df.drop(columns = ["longitude", "latitude"]),
        geometry = gpd.points_from_xy(df["longitude"], df["latitude"]),
        crs = "EPSG:4326",
    )

    gdf.to_parquet(output_path)
    print(f"Saved Overture snapshot to {output_path}")
    return gdf
