#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root.
#   -------------------------------------------------------------
"""
Merge matched and unmatched POIs into a unified conflated dataset.

Produces a GeoDataFrame superset:
  - Matched pairs (OSM + Overture) with blended confidence.
  - Unmatched OSM POIs with their original confidence.
  - Unmatched Overture POIs with downweighted confidence.

For large datasets, ``build_merge_parts`` writes each subset to a
temp parquet file so that source GeoDataFrames can be freed before
the final concat+save in ``save_conflated_from_parts``.
"""
from __future__ import annotations

import gc
import tempfile
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
import shapely


def _pick_geometries(
    osm_geoms: np.ndarray,
    overture_geoms: np.ndarray,
) -> np.ndarray:
    """
    Vectorized geometry selection: prefer higher-level geometry type,
    OSM on ties.
    """
    osm_types = shapely.get_type_id(osm_geoms)
    ov_types = shapely.get_type_id(overture_geoms)
    rank_table = np.ones(8, dtype = np.int8)
    rank_table[0] = 1  # Point
    rank_table[1] = 2  # LineString
    rank_table[3] = 3  # Polygon
    rank_table[6] = 4  # MultiPolygon
    osm_ranks = rank_table[osm_types]
    ov_ranks = rank_table[ov_types]
    use_overture = ov_ranks > osm_ranks
    result = osm_geoms.copy()
    result[use_overture] = overture_geoms[use_overture]
    return result


def _build_matched_gdf(
    osm_gdf: gpd.GeoDataFrame,
    overture_gdf: gpd.GeoDataFrame,
    matches: pd.DataFrame,
    osm_shared_labels: np.ndarray,
    osm_w: float,
    ov_w: float,
) -> gpd.GeoDataFrame:
    """Build GeoDataFrame for matched pairs."""
    oi = matches["osm_idx"].to_numpy()
    vi = matches["overture_idx"].to_numpy()

    osm_conf = osm_gdf["conf_mean"].to_numpy()[oi].astype(float)
    ov_conf_raw = overture_gdf["confidence"].to_numpy()[vi]
    ov_conf = pd.to_numeric(
        ov_conf_raw, errors = "coerce"
    ).astype(float)
    ov_conf = np.where(np.isnan(ov_conf), 0.5, ov_conf)
    osm_higher = osm_conf >= ov_conf

    osm_names = osm_gdf["name"].to_numpy()[oi]
    ov_names = overture_gdf["overture_name"].to_numpy()[vi]
    names = np.where(
        osm_higher,
        osm_names,
        np.where(pd.notna(ov_names), ov_names, osm_names),
    )

    osm_brands = (
        osm_gdf["brand"].to_numpy()[oi]
        if "brand" in osm_gdf.columns
        else np.full(len(oi), None, dtype = object)
    )
    ov_brands = (
        overture_gdf["brand_name"].to_numpy()[vi]
        if "brand_name" in overture_gdf.columns
        else np.full(len(vi), None, dtype = object)
    )
    brands = np.where(
        osm_higher,
        osm_brands,
        np.where(pd.notna(ov_brands), ov_brands, osm_brands),
    )

    merged_conf = osm_conf * osm_w + ov_conf * ov_w

    osm_conf_lower = osm_gdf["conf_lower"].to_numpy()[oi].astype(
        float
    )
    osm_conf_upper = osm_gdf["conf_upper"].to_numpy()[oi].astype(
        float
    )
    conf_lower = osm_conf_lower * osm_w + ov_conf * ov_w
    conf_upper = osm_conf_upper * osm_w + ov_conf * ov_w

    osm_geoms = osm_gdf.geometry.to_numpy()[oi]
    ov_geoms = overture_gdf.geometry.to_numpy()[vi]
    geoms = _pick_geometries(osm_geoms, ov_geoms)

    osm_ids = osm_gdf["osm_id"].to_numpy()[oi]
    ov_ids = overture_gdf["overture_id"].to_numpy()[vi]

    unified_ids = np.array(
        [
            f"matched:{o}_{v}"
            for o, v in zip(osm_ids, ov_ids)
        ],
        dtype = object,
    )

    return gpd.GeoDataFrame(
        {
            "unified_id": unified_ids,
            "source": "matched",
            "osm_id": osm_ids,
            "overture_id": ov_ids,
            "name": names,
            "brand": brands,
            "shared_label": osm_shared_labels[oi],
            "conf_mean": merged_conf,
            "conf_lower": conf_lower,
            "conf_upper": conf_upper,
            "match_score": matches["composite_score"].to_numpy(),
            "match_distance_m": matches["distance_m"].to_numpy(),
            "osm_name": osm_names,
            "overture_name": ov_names,
            "osm_brand": osm_brands,
            "overture_brand": ov_brands,
            "osm_conf_mean": osm_conf,
            "overture_confidence": ov_conf,
        },
        geometry = geoms,
        crs = osm_gdf.crs,
    )


def _build_unmatched_osm_gdf(
    osm_gdf: gpd.GeoDataFrame,
    matched_osm_set: set[int],
    osm_shared_labels: np.ndarray,
) -> gpd.GeoDataFrame:
    """Build GeoDataFrame for unmatched OSM POIs."""
    mask = np.ones(len(osm_gdf), dtype = bool)
    if matched_osm_set:
        mask[np.array(list(matched_osm_set), dtype = np.int64)] = (
            False
        )
    idx = np.where(mask)[0]
    sub = osm_gdf.iloc[idx]

    return gpd.GeoDataFrame(
        {
            "unified_id": np.array(
                [f"osm:{x}" for x in sub["osm_id"].to_numpy()],
                dtype = object,
            ),
            "source": "osm",
            "osm_id": sub["osm_id"].to_numpy(),
            "overture_id": np.full(len(sub), None, dtype = object),
            "name": sub["name"].to_numpy(),
            "brand": (
                sub["brand"].to_numpy()
                if "brand" in sub.columns
                else np.full(len(sub), None, dtype = object)
            ),
            "shared_label": osm_shared_labels[idx],
            "conf_mean": sub["conf_mean"].to_numpy().astype(float),
            "conf_lower": sub["conf_lower"].to_numpy().astype(float),
            "conf_upper": sub["conf_upper"].to_numpy().astype(float),
            "match_score": np.full(len(sub), np.nan),
            "match_distance_m": np.full(len(sub), np.nan),
            "osm_name": sub["name"].to_numpy(),
            "overture_name": np.full(
                len(sub), None, dtype = object
            ),
            "osm_brand": (
                sub["brand"].to_numpy()
                if "brand" in sub.columns
                else np.full(len(sub), None, dtype = object)
            ),
            "overture_brand": np.full(
                len(sub), None, dtype = object
            ),
            "osm_conf_mean": sub["conf_mean"].to_numpy().astype(
                float
            ),
            "overture_confidence": np.full(len(sub), np.nan),
        },
        geometry = sub.geometry.to_numpy(),
        crs = sub.crs,
    )


def _build_unmatched_overture_gdf(
    overture_gdf: gpd.GeoDataFrame,
    matched_ov_set: set[int],
    overture_shared_labels: np.ndarray,
    w: float,
) -> gpd.GeoDataFrame:
    """Build GeoDataFrame for unmatched Overture POIs."""
    mask = np.ones(len(overture_gdf), dtype = bool)
    if matched_ov_set:
        mask[np.array(list(matched_ov_set), dtype = np.int64)] = (
            False
        )
    idx = np.where(mask)[0]
    sub = overture_gdf.iloc[idx]

    ov_conf_raw = sub["confidence"].to_numpy()
    ov_conf = pd.to_numeric(
        ov_conf_raw, errors = "coerce"
    ).astype(float)
    ov_conf = np.where(np.isnan(ov_conf), 0.5, ov_conf)

    return gpd.GeoDataFrame(
        {
            "unified_id": np.array(
                [
                    f"overture:{x}"
                    for x in sub["overture_id"].to_numpy()
                ],
                dtype = object,
            ),
            "source": "overture",
            "osm_id": np.full(len(sub), None, dtype = object),
            "overture_id": sub["overture_id"].to_numpy(),
            "name": sub["overture_name"].to_numpy(),
            "brand": (
                sub["brand_name"].to_numpy()
                if "brand_name" in sub.columns
                else np.full(len(sub), None, dtype = object)
            ),
            "shared_label": overture_shared_labels[idx],
            "conf_mean": ov_conf * w,
            "conf_lower": np.full(len(sub), np.nan),
            "conf_upper": np.full(len(sub), np.nan),
            "match_score": np.full(len(sub), np.nan),
            "match_distance_m": np.full(len(sub), np.nan),
            "osm_name": np.full(len(sub), None, dtype = object),
            "overture_name": sub["overture_name"].to_numpy(),
            "osm_brand": np.full(len(sub), None, dtype = object),
            "overture_brand": (
                sub["brand_name"].to_numpy()
                if "brand_name" in sub.columns
                else np.full(len(sub), None, dtype = object)
            ),
            "osm_conf_mean": np.full(len(sub), np.nan),
            "overture_confidence": ov_conf,
        },
        geometry = sub.geometry.to_numpy(),
        crs = overture_gdf.crs,
    )


# -----------------------------------------------------------------
# In-memory merge (for tests and small datasets)
# -----------------------------------------------------------------


def merge_matched_pois(
    osm_gdf: gpd.GeoDataFrame,
    overture_gdf: gpd.GeoDataFrame,
    matches: pd.DataFrame,
    osm_shared_labels: np.ndarray,
    overture_shared_labels: np.ndarray,
    overture_confidence_weight: float = 0.7,
) -> gpd.GeoDataFrame:
    """
    Build the unified conflated dataset from matches + unmatched.

    This in-memory version is suitable for tests and small datasets.
    For large datasets, use ``build_merge_parts`` +
    ``save_conflated_from_parts`` to avoid holding all parts in
    memory simultaneously.

    Returns:
        Conflated GeoDataFrame with unified schema.
    """
    w = overture_confidence_weight
    osm_w = 1.0 / (1.0 + w)
    ov_w = w / (1.0 + w)

    matched_osm_set = set(matches["osm_idx"].to_numpy())
    matched_ov_set = set(matches["overture_idx"].to_numpy())

    parts = []

    if len(matches) > 0:
        parts.append(
            _build_matched_gdf(
                osm_gdf, overture_gdf, matches,
                osm_shared_labels, osm_w, ov_w,
            )
        )

    parts.append(
        _build_unmatched_osm_gdf(
            osm_gdf, matched_osm_set, osm_shared_labels,
        )
    )

    parts.append(
        _build_unmatched_overture_gdf(
            overture_gdf, matched_ov_set,
            overture_shared_labels, w,
        )
    )

    result = pd.concat(parts, ignore_index = True)
    return gpd.GeoDataFrame(result, crs = osm_gdf.crs)


# -----------------------------------------------------------------
# Disk-backed merge (for large datasets)
# -----------------------------------------------------------------


def build_merge_parts(
    osm_gdf: gpd.GeoDataFrame,
    overture_gdf: gpd.GeoDataFrame,
    matches: pd.DataFrame,
    osm_shared_labels: np.ndarray,
    overture_shared_labels: np.ndarray,
    overture_confidence_weight: float = 0.7,
) -> list[Path]:
    """
    Build each merge subset, write to temp parquet files.

    Each part is written to disk and freed immediately so that the
    caller can delete the source GeoDataFrames before the final
    concat+save step.

    Returns:
        List of temp parquet file paths (matched, osm, overture).
    """
    tmp_dir = Path(tempfile.mkdtemp(prefix = "openpois_merge_"))

    w = overture_confidence_weight
    osm_w = 1.0 / (1.0 + w)
    ov_w = w / (1.0 + w)

    matched_osm_set = set(matches["osm_idx"].to_numpy())
    matched_ov_set = set(matches["overture_idx"].to_numpy())
    part_paths: list[Path] = []

    # Part 1: matched pairs
    if len(matches) > 0:
        print(f"  Building {len(matches):,} matched pairs ...")
        part = _build_matched_gdf(
            osm_gdf, overture_gdf, matches,
            osm_shared_labels, osm_w, ov_w,
        )
        p = tmp_dir / "1_matched.parquet"
        part.to_parquet(p, compression = "zstd")
        part_paths.append(p)
        del part
        gc.collect()

    # Part 2: unmatched OSM
    n_unmatched_osm = len(osm_gdf) - len(matched_osm_set)
    print(
        f"  Building {n_unmatched_osm:,} unmatched OSM POIs ..."
    )
    part = _build_unmatched_osm_gdf(
        osm_gdf, matched_osm_set, osm_shared_labels,
    )
    p = tmp_dir / "2_unmatched_osm.parquet"
    part.to_parquet(p, compression = "zstd")
    part_paths.append(p)
    del part
    gc.collect()

    # Part 3: unmatched Overture
    n_unmatched_ov = len(overture_gdf) - len(matched_ov_set)
    print(
        f"  Building {n_unmatched_ov:,} unmatched Overture POIs ..."
    )
    part = _build_unmatched_overture_gdf(
        overture_gdf, matched_ov_set,
        overture_shared_labels, w,
    )
    p = tmp_dir / "3_unmatched_overture.parquet"
    part.to_parquet(p, compression = "zstd")
    part_paths.append(p)
    del part
    gc.collect()

    return part_paths


def save_conflated_from_parts(
    part_paths: list[Path],
    output_path: Path,
) -> int:
    """
    Concatenate temp parquet parts via PyArrow and save.

    Uses PyArrow's zero-copy ``concat_tables`` to avoid loading
    all data as pandas DataFrames simultaneously. Skips Hilbert
    sorting to stay within memory limits.

    Returns:
        Number of POIs written.
    """
    output_path = Path(output_path)
    output_path.parent.mkdir(parents = True, exist_ok = True)

    print("  Concatenating parts via PyArrow ...")
    tables = [pq.read_table(p) for p in part_paths]
    combined = pa.concat_tables(tables, promote_options = "permissive")
    del tables
    gc.collect()

    n = combined.num_rows
    print(f"  Saving {n:,} POIs to {output_path} ...")
    pq.write_table(
        combined,
        output_path,
        compression = "zstd",
        row_group_size = 50_000,
    )
    del combined
    gc.collect()

    # Clean up temp files
    for p in part_paths:
        p.unlink()
    part_paths[0].parent.rmdir()

    print(f"  Done.")
    return n


def save_conflated(
    gdf: gpd.GeoDataFrame,
    output_path: Path,
) -> None:
    """Hilbert-sort and save as GeoParquet (zstd, 50k row groups)."""
    output_path = Path(output_path)
    output_path.parent.mkdir(parents = True, exist_ok = True)

    print("Sorting by Hilbert curve index ...")
    hilbert_order = gdf.hilbert_distance()
    gdf = gdf.iloc[hilbert_order.argsort()].reset_index(drop = True)

    print(f"Saving conflated dataset to {output_path} ...")
    gdf.to_parquet(
        output_path,
        compression = "zstd",
        row_group_size = 50_000,
    )
    print(f"Done. Saved {len(gdf):,} POIs.")
