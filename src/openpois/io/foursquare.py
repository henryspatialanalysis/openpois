#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module downloads a current/latest Foursquare OS Places snapshot for the
United States via the Foursquare Places Portal Apache Iceberg catalog.

It is broken into the following functions:
- get_fsq_catalog: Authenticates to the Foursquare Iceberg REST catalog.
- get_latest_fsq_release_date: Identifies the most recent snapshot date.
- load_fsq_us_places: Loads US places filtered by L1 category from the catalog.
- download_foursquare_snapshot: End-to-end orchestrator.

Authentication:
    Register at https://places.foursquare.com to obtain a portal token.
    Set the token in the FSQ_PORTAL_TOKEN environment variable before running.

Data license: Apache 2.0. See https://opensource.foursquare.com/places-notice-txt/
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING

import geopandas as gpd

if TYPE_CHECKING:
    from pyiceberg.catalog.rest import RestCatalog


# -----------------------------------------------------------------------------
# Catalog authentication
# -----------------------------------------------------------------------------


def get_fsq_catalog(
    catalog_uri: str,
    catalog_warehouse: str,
    token_env_var: str,
    token: str | None = None,
) -> RestCatalog:
    """
    Initializes and returns a PyIceberg RestCatalog for the Foursquare portal.

    Args:
        catalog_uri: URI of the Foursquare Iceberg REST catalog.
        catalog_warehouse: Warehouse name within the catalog (e.g., 'places').
        token_env_var: Name of the environment variable holding the token.
        token: Foursquare portal API token. If None, reads from the environment
            variable named by token_env_var.

    Returns:
        An authenticated PyIceberg RestCatalog instance.

    Raises:
        EnvironmentError: If token is None and the env var is not set.
        ImportError: If pyiceberg is not installed.
    """
    try:
        from pyiceberg.catalog.rest import RestCatalog  # pylint: disable=C0415
    except ImportError as e:
        raise ImportError(
            "pyiceberg is required for Foursquare downloads. "
            "Install it with: pip install pyiceberg"
        ) from e

    if token is None:
        token = os.environ.get(token_env_var)
    if not token:
        raise EnvironmentError(
            f"Foursquare portal token not found. Set the {token_env_var} "
            "environment variable to your portal token from "
            "https://places.foursquare.com"
        )

    catalog = RestCatalog(
        name = "foursquare",
        uri = catalog_uri,
        warehouse = catalog_warehouse,
        token = token,
    )
    return catalog


# -----------------------------------------------------------------------------
# Release discovery
# -----------------------------------------------------------------------------


def get_latest_fsq_release_date(
    catalog: RestCatalog,
    catalog_namespace: str,
    places_table: str,
) -> str:
    """
    Returns the most recent snapshot date for the Foursquare places table.

    The FSQ OS Places table is a single, unpartitioned snapshot. The snapshot
    date is inferred from the ``last_updated_at`` timestamp in the table's
    partition metadata.

    Args:
        catalog: An authenticated RestCatalog instance.
        catalog_namespace: Iceberg namespace containing the FSQ tables.
        places_table: Name of the places table within the namespace.

    Returns:
        Release date string in 'YYYY-MM-DD' format.

    Raises:
        ValueError: If no partition metadata rows are found.
    """
    table = catalog.load_table(f"{catalog_namespace}.{places_table}")
    partitions = table.inspect.partitions()
    rows = partitions.to_pylist()
    if not rows:
        raise ValueError(
            "No partition metadata found in the Foursquare places table. "
            "Verify that the catalog URI and table namespace are correct."
        )
    # Use the most recent last_updated_at timestamp across all partition rows
    timestamps = [
        row["last_updated_at"]
        for row in rows
        if row.get("last_updated_at") is not None
    ]
    if not timestamps:
        raise ValueError(
            "No 'last_updated_at' values found in Foursquare partition metadata."
        )
    latest = max(timestamps)
    # Format as YYYY-MM-DD
    if hasattr(latest, "strftime"):
        return latest.strftime("%Y-%m-%d")
    return str(latest)[:10]


# -----------------------------------------------------------------------------
# Data loading
# -----------------------------------------------------------------------------


def load_fsq_us_places(
    catalog: RestCatalog,
    release_date: str,
    l1_category_names: list[str],
    catalog_namespace: str,
    places_table: str,
    categories_table: str,
) -> gpd.GeoDataFrame:
    """
    Loads US places from the Foursquare Iceberg catalog, filtered by country
    and L1 category name.

    Reads the places and categories tables, joins on fsq_category_ids to
    resolve the L1 category hierarchy, then filters to rows where any element
    of fsq_category_labels starts with a name in l1_category_names and
    date_closed is null.

    Args:
        catalog: An authenticated RestCatalog instance.
        release_date: Snapshot date label (e.g., '2026-02-12'). Written as
            a metadata column; the table is unpartitioned so no row filter
            is applied on this value.
        l1_category_names: L1 category names to retain (e.g.,
            ['Dining and Drinking', 'Retail']).
        catalog_namespace: Iceberg namespace containing the FSQ tables.
        places_table: Name of the places table within the namespace.
        categories_table: Name of the categories table within the namespace.

    Returns:
        GeoDataFrame with columns:
            source (str), fsq_place_id (str), release_date (str),
            fsq_category_ids (list[str]), fsq_category_labels (list[str]),
            fsq_l1_category (str, nullable), name (str, nullable),
            country (str), geometry (Point, EPSG:4326).
    """
    places_table_obj = catalog.load_table(
        f"{catalog_namespace}.{places_table}"
    )
    cats_table_obj = catalog.load_table(
        f"{catalog_namespace}.{categories_table}"
    )

    # Load categories to build a mapping from category_id to L1 name
    cats_scan = cats_table_obj.scan(
        selected_fields = (
            "category_id",
            "level1_category_name",
            "category_label",
        )
    )
    cats_df = cats_scan.to_pandas()
    # Build a dict from leaf category_id -> L1 category name
    cat_id_to_l1 = dict(
        zip(cats_df["category_id"], cats_df["level1_category_name"])
    )

    # Load US places (table is unpartitioned; filter by country and open status)
    places_scan = places_table_obj.scan(
        row_filter = "country = 'US' AND date_closed IS NULL",
        selected_fields = (
            "fsq_place_id",
            "name",
            "latitude",
            "longitude",
            "country",
            "fsq_category_ids",
            "fsq_category_labels",
        ),
    )
    places_df = places_scan.to_pandas()

    # Resolve L1 category: take the L1 name of the first matching category ID
    def _resolve_l1(cat_ids) -> str | None:
        """Return the first L1 category name matching a category ID, or None."""
        if cat_ids is None or len(cat_ids) == 0:
            return None
        for cid in cat_ids:
            l1 = cat_id_to_l1.get(cid)
            if l1 in l1_category_names:
                return l1
        return None

    places_df["fsq_l1_category"] = places_df["fsq_category_ids"].apply(
        _resolve_l1
    )

    # Filter to rows that match a target L1 category
    places_df = places_df.loc[places_df["fsq_l1_category"].notna()].copy()

    places_df.insert(0, "source", "foursquare")
    places_df.insert(2, "release_date", release_date)

    gdf = gpd.GeoDataFrame(
        places_df.drop(columns = ["latitude", "longitude"]),
        geometry = gpd.points_from_xy(
            places_df["longitude"], places_df["latitude"]
        ),
        crs = "EPSG:4326",
    )
    return gdf


# -----------------------------------------------------------------------------
# Orchestrator
# -----------------------------------------------------------------------------


def download_foursquare_snapshot(
    output_path: Path,
    l1_category_names: list[str],
    catalog_uri: str,
    catalog_warehouse: str,
    catalog_namespace: str,
    places_table: str,
    categories_table: str,
    token_env_var: str,
    release_date: str | None = None,
    token: str | None = None,
) -> gpd.GeoDataFrame:
    """
    End-to-end orchestrator: connect to the Foursquare catalog, load US
    places filtered by L1 category, and save as GeoParquet.

    Reads the portal token from the environment variable named by token_env_var
    if token is not supplied directly.

    Args:
        output_path: Path to write the output GeoParquet file.
        l1_category_names: Foursquare L1 category names to retain.
        catalog_uri: URI of the Foursquare Iceberg REST catalog.
        catalog_warehouse: Warehouse name within the catalog (e.g., 'places').
        catalog_namespace: Iceberg namespace containing the FSQ tables.
        places_table: Name of the places table within the namespace.
        categories_table: Name of the categories table within the namespace.
        token_env_var: Name of the environment variable holding the portal token.
        release_date: Snapshot date to download (e.g., '2026-02-12').
            If None, uses the latest available snapshot.
        token: Foursquare portal API token. If None, reads from the
            environment variable named by token_env_var.

    Returns:
        GeoDataFrame written to output_path.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents=True, exist_ok=True)

    catalog = get_fsq_catalog(
        token = token,
        catalog_uri = catalog_uri,
        catalog_warehouse = catalog_warehouse,
        token_env_var = token_env_var,
    )

    if release_date is None:
        print("Detecting latest Foursquare release...")
        release_date = get_latest_fsq_release_date(
            catalog,
            catalog_namespace = catalog_namespace,
            places_table = places_table,
        )
        print(f"Using release: {release_date}")

    print(f"Loading Foursquare US places for release {release_date}...")
    gdf = load_fsq_us_places(
        catalog = catalog,
        release_date = release_date,
        l1_category_names = l1_category_names,
        catalog_namespace = catalog_namespace,
        places_table = places_table,
        categories_table = categories_table,
    )

    gdf.to_parquet(output_path)
    print(f"Saved {len(gdf):,} Foursquare places to {output_path}")
    return gdf
