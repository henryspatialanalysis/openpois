#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root.
#   -------------------------------------------------------------
"""
Spatial candidate matching and scoring for POI conflation.

1. ``find_spatial_candidates`` — BallTree-based radius search to find
   nearby (OSM, Overture) pairs within category-specific thresholds.
2. ``compute_match_scores`` — multi-component scoring (distance, name,
   type taxonomy, identifiers) for each candidate pair.
3. ``select_best_matches`` — greedy one-to-one assignment above a
   minimum composite score.
"""
from __future__ import annotations

import re
import warnings

import numpy as np
import pandas as pd
import shapely
from rapidfuzz import fuzz
from sklearn.neighbors import BallTree

EARTH_RADIUS_M = 6_371_000.0


# -----------------------------------------------------------------
# Spatial candidate search
# -----------------------------------------------------------------


def _extract_centroids_rad(geom_array) -> np.ndarray:
    """
    Extract (lat_rad, lon_rad) from a geometry array.

    BallTree with ``metric='haversine'`` expects [lat, lon] in
    radians. We suppress the geographic CRS centroid warning since
    we only need approximate centroids for radius search.
    """
    with warnings.catch_warnings():
        warnings.filterwarnings(
            "ignore", "Geometry is in a geographic CRS"
        )
        centroids = shapely.centroid(geom_array)

    x = shapely.get_x(centroids)
    y = shapely.get_y(centroids)
    return np.column_stack([np.deg2rad(y), np.deg2rad(x)])


def find_spatial_candidates(
    osm_geom,
    overture_geom,
    osm_radii_m: np.ndarray,
    max_radius_m: float = 200.0,
    chunk_size: int = 500_000,
) -> pd.DataFrame:
    """
    Find spatially proximate (OSM, Overture) candidate pairs.

    Builds a single BallTree on Overture centroids and queries it
    in chunks of OSM centroids to control memory.

    Args:
        osm_geom: OSM geometry array (GeoSeries.values or similar).
        overture_geom: Overture geometry array.
        osm_radii_m: Per-OSM-POI match radius in meters.
        max_radius_m: Global upper bound on search radius.
        chunk_size: Number of OSM rows to query per batch.

    Returns:
        DataFrame with columns: osm_idx, overture_idx, distance_m.
    """
    overture_coords = _extract_centroids_rad(overture_geom)
    tree = BallTree(overture_coords, metric = "haversine")

    osm_coords = _extract_centroids_rad(osm_geom)
    # Clip radii to max and convert to radians once
    osm_radii_rad = (
        np.minimum(osm_radii_m, max_radius_m) / EARTH_RADIUS_M
    )
    n_osm = len(osm_coords)

    all_osm = []
    all_ov = []
    all_dist = []

    for start in range(0, n_osm, chunk_size):
        end = min(start + chunk_size, n_osm)
        chunk_coords = osm_coords[start:end]
        chunk_radii_rad = osm_radii_rad[start:end]

        # Pass per-POI radii so the BallTree only returns
        # neighbours within each POI's actual radius — avoids
        # the huge over-query from using max_radius for all.
        ind, dist = tree.query_radius(
            chunk_coords,
            r = chunk_radii_rad,
            return_distance = True,
        )

        for local_i in range(len(chunk_coords)):
            ov_idx = ind[local_i]
            if len(ov_idx) > 0:
                global_i = start + local_i
                d_m = dist[local_i] * EARTH_RADIUS_M
                all_osm.append(
                    np.full(
                        len(ov_idx), global_i,
                        dtype = np.int64,
                    )
                )
                all_ov.append(
                    ov_idx.astype(np.int64)
                )
                all_dist.append(d_m)

    if not all_osm:
        return pd.DataFrame(
            columns = ["osm_idx", "overture_idx", "distance_m"]
        )
    return pd.DataFrame(
        {
            "osm_idx": np.concatenate(all_osm),
            "overture_idx": np.concatenate(all_ov),
            "distance_m": np.concatenate(all_dist),
        }
    )


# -----------------------------------------------------------------
# Name scoring (vectorized)
# -----------------------------------------------------------------

_WHITESPACE_RE = re.compile(r"\s+")


def _normalize_name(s) -> str:
    """Lowercase, strip, collapse whitespace."""
    if s is None or pd.isna(s):
        return ""
    s = str(s).lower().strip()
    return _WHITESPACE_RE.sub(" ", s)


def _normalize_name_array(arr: np.ndarray) -> np.ndarray:
    """Normalize an array of names. Vectorized via list comp."""
    return np.array(
        [_normalize_name(v) for v in arr], dtype = object,
    )


def _batch_token_set_ratio(
    a_arr: np.ndarray,
    b_arr: np.ndarray,
) -> np.ndarray:
    """
    Compute token_set_ratio / 100 for paired arrays.

    Returns NaN where either side is empty.
    """
    n = len(a_arr)
    scores = np.full(n, np.nan, dtype = np.float64)
    for i in range(n):
        if a_arr[i] and b_arr[i]:
            scores[i] = fuzz.token_set_ratio(
                a_arr[i], b_arr[i]
            ) / 100.0
    return scores


def compute_name_scores(
    osm_names: np.ndarray,
    osm_brands: np.ndarray,
    overture_names: np.ndarray,
    overture_brands: np.ndarray,
    osm_idx: np.ndarray,
    overture_idx: np.ndarray,
    chunk_size: int = 2_000_000,
) -> np.ndarray:
    """
    Compute name match score for each candidate pair.

    Pre-normalizes source arrays once (~15M strings), then scores
    in chunks to limit peak memory. Takes the max of up to 4
    comparisons (name-name, brand-brand, name-brand cross).
    Returns 0.5 (neutral) when all are null.
    """
    # Normalize source arrays once (15M total, not 80M indexed)
    norm_on = _normalize_name_array(osm_names)
    norm_ob = _normalize_name_array(osm_brands)
    norm_vn = _normalize_name_array(overture_names)
    norm_vb = _normalize_name_array(overture_brands)

    n = len(osm_idx)
    scores = np.empty(n, dtype = np.float64)

    for start in range(0, n, chunk_size):
        end = min(start + chunk_size, n)
        oi = osm_idx[start:end]
        vi = overture_idx[start:end]

        on = norm_on[oi]
        ob = norm_ob[oi]
        vn = norm_vn[vi]
        vb = norm_vb[vi]

        s1 = _batch_token_set_ratio(on, vn)
        s2 = _batch_token_set_ratio(ob, vb)
        s3 = _batch_token_set_ratio(on, vb)
        s4 = _batch_token_set_ratio(ob, vn)

        stacked = np.column_stack([s1, s2, s3, s4])
        with warnings.catch_warnings():
            warnings.filterwarnings(
                "ignore", "All-NaN slice", RuntimeWarning
            )
            chunk_scores = np.nanmax(stacked, axis = 1)
        scores[start:end] = np.where(
            np.isnan(chunk_scores), 0.5, chunk_scores
        )

    del norm_on, norm_ob, norm_vn, norm_vb
    return scores


# -----------------------------------------------------------------
# Type taxonomy scoring (vectorized)
# -----------------------------------------------------------------


def compute_type_scores(
    osm_shared_labels: np.ndarray,
    overture_shared_labels: np.ndarray,
    osm_l0_bits: np.ndarray,
    overture_l0_bits: np.ndarray,
    osm_idx: np.ndarray,
    overture_idx: np.ndarray,
) -> np.ndarray:
    """
    Score how well the POI types match between OSM and Overture.

    - Exact shared_label match: 1.0
    - L0 broad-group overlap (bitmask): 0.5
    - Otherwise: 0.0
    """
    o_labels = osm_shared_labels[osm_idx]
    v_labels = overture_shared_labels[overture_idx]

    scores = np.zeros(len(osm_idx), dtype = np.float64)

    # Tier 1: exact shared_label match
    both_present = (o_labels != "") & (v_labels != "")
    exact = both_present & (o_labels == v_labels)
    scores[exact] = 1.0

    # Tier 2: L0 bitmask overlap for non-exact pairs
    not_exact = ~exact
    if not_exact.any():
        o_bits = osm_l0_bits[osm_idx[not_exact]]
        v_bits = overture_l0_bits[overture_idx[not_exact]]
        broad = (o_bits & v_bits) != 0
        idx = np.where(not_exact)[0]
        scores[idx[broad]] = 0.5

    return scores


# -----------------------------------------------------------------
# Identifier scoring
# -----------------------------------------------------------------


def compute_identifier_scores(
    osm_idx: np.ndarray,
    overture_idx: np.ndarray,
) -> np.ndarray:
    """
    Score identifier matches.

    Returns 0.5 (neutral) for all pairs. Overture schema does not
    currently expose website/phone/wikidata fields. This component
    can be extended when those fields become available.
    """
    return np.full(len(osm_idx), 0.5, dtype = np.float64)


# -----------------------------------------------------------------
# Composite scoring
# -----------------------------------------------------------------


def compute_match_scores(
    candidates: pd.DataFrame,
    osm_names: np.ndarray,
    osm_brands: np.ndarray,
    overture_names: np.ndarray,
    overture_brands: np.ndarray,
    osm_shared_labels: np.ndarray,
    overture_shared_labels: np.ndarray,
    osm_radii_m: np.ndarray,
    osm_l0_bits: np.ndarray,
    overture_l0_bits: np.ndarray,
    distance_weight: float = 0.25,
    name_weight: float = 0.30,
    type_weight: float = 0.25,
    identifier_weight: float = 0.20,
    score_chunk_size: int = 2_000_000,
) -> pd.DataFrame:
    """
    Compute composite match scores for all candidate pairs.

    Name and type scoring are processed in chunks of
    ``score_chunk_size`` pairs to limit peak memory.

    Returns the candidates DataFrame with added score columns:
    distance_score, name_score, type_score, identifier_score,
    composite_score.
    """
    osm_idx = candidates["osm_idx"].to_numpy()
    overture_idx = candidates["overture_idx"].to_numpy()
    distance_m = candidates["distance_m"].to_numpy()
    n = len(candidates)

    # A) Distance score (cheap vectorized arithmetic)
    pair_radii = osm_radii_m[osm_idx]
    distance_score = np.clip(
        1.0 - (distance_m / pair_radii), 0.0, 1.0
    )
    del pair_radii

    # B) Name score (chunked internally)
    name_score = compute_name_scores(
        osm_names, osm_brands,
        overture_names, overture_brands,
        osm_idx, overture_idx,
        chunk_size = score_chunk_size,
    )

    # C) Type taxonomy score (chunked to limit string arrays)
    type_score = np.empty(n, dtype = np.float64)
    for start in range(0, n, score_chunk_size):
        end = min(start + score_chunk_size, n)
        type_score[start:end] = compute_type_scores(
            osm_shared_labels, overture_shared_labels,
            osm_l0_bits, overture_l0_bits,
            osm_idx[start:end],
            overture_idx[start:end],
        )

    # D) Identifier score (neutral placeholder)
    identifier_score = np.full(n, 0.5, dtype = np.float64)

    composite = (
        distance_weight * distance_score
        + name_weight * name_score
        + type_weight * type_score
        + identifier_weight * identifier_score
    )

    # Mutate in place to avoid copying the full DataFrame
    candidates["distance_score"] = distance_score
    candidates["name_score"] = name_score
    candidates["type_score"] = type_score
    candidates["identifier_score"] = identifier_score
    candidates["composite_score"] = composite
    return candidates


# -----------------------------------------------------------------
# Best-match selection (greedy one-to-one)
# -----------------------------------------------------------------


def select_best_matches(
    scored: pd.DataFrame,
    min_score: float = 0.67,
) -> pd.DataFrame:
    """
    Greedy one-to-one matching above a minimum composite score.

    Sorts candidates by composite_score descending, then iterates:
    assign each pair if neither the OSM POI nor the Overture POI has
    been assigned yet.

    Returns:
        DataFrame of selected matches with all score columns.
    """
    above = scored[scored["composite_score"] >= min_score].copy()
    if above.empty:
        return above

    above = above.sort_values(
        "composite_score", ascending = False
    ).reset_index(drop = True)

    used_osm: set[int] = set()
    used_overture: set[int] = set()
    keep = []

    osm_arr = above["osm_idx"].to_numpy()
    ov_arr = above["overture_idx"].to_numpy()
    for i in range(len(above)):
        oi = int(osm_arr[i])
        vi = int(ov_arr[i])
        if oi not in used_osm and vi not in used_overture:
            keep.append(i)
            used_osm.add(oi)
            used_overture.add(vi)

    return above.iloc[keep].reset_index(drop = True)
