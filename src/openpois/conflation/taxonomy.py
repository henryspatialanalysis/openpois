#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root.
#   -------------------------------------------------------------
"""
Taxonomy crosswalk between OSM tags and Overture Maps taxonomy.

Loads four CSV files that map OSM tag key/value pairs and Overture
(L0, L1) categories to a unified ``shared_label``, plus per-label
match radii and top-level OSM-key-to-Overture-L0 mappings.
"""
from __future__ import annotations

from importlib import resources

import numpy as np
import pandas as pd


WILDCARD = "*"

# Bit flags for the five Overture L0 categories.  Used by
# ``compute_osm_l0_bits`` / ``compute_overture_l0_bits`` for
# fast vectorised broad-match checks in type scoring.
L0_BIT: dict[str, int] = {
    "arts_and_entertainment": 1,
    "food_and_drink": 2,
    "health_care": 4,
    "shopping": 8,
    "sports_and_recreation": 16,
}


# -----------------------------------------------------------------
# CSV loaders
# -----------------------------------------------------------------


def _load_csv(filename: str) -> pd.DataFrame:
    """Load a CSV from the package data directory."""
    csv_path = (
        resources.files("openpois.conflation.data")
        .joinpath(filename)
    )
    with resources.as_file(csv_path) as p:
        return pd.read_csv(
            p, dtype = str, keep_default_na = False,
        )


def load_osm_crosswalk() -> pd.DataFrame:
    """Load the OSM taxonomy crosswalk CSV.

    Columns: ``osm_key, osm_value, shared_label``.
    """
    return _load_csv("taxonomy_crosswalk_openstreetmap.csv")


def load_overture_crosswalk() -> pd.DataFrame:
    """Load the Overture Maps taxonomy crosswalk CSV.

    Columns: ``overture_l0, overture_l1, poi_category,
    shared_label``.
    """
    return _load_csv("taxonomy_crosswalk_overture_maps.csv")


def load_match_radii() -> pd.DataFrame:
    """Load the match-radii CSV.

    Columns: ``shared_label, match_radius_m``.
    """
    return _load_csv("match_radii.csv")


def load_top_level_matches() -> pd.DataFrame:
    """Load the top-level OSM-key ↔ Overture-L0 CSV.

    Columns: ``overture_l0, osm_key``.
    """
    return _load_csv("top_level_matches.csv")


# -----------------------------------------------------------------
# Shared-label assignment — OSM
# -----------------------------------------------------------------


def _build_osm_label_lookups(
    osm_crosswalk: pd.DataFrame,
) -> tuple[dict[str, pd.Series], dict[str, str]]:
    """Build per-key label lookups and wildcard fallbacks."""
    specific = osm_crosswalk[
        osm_crosswalk["osm_value"] != WILDCARD
    ].copy()
    wildcards_df = osm_crosswalk[
        osm_crosswalk["osm_value"] == WILDCARD
    ]

    lookups: dict[str, pd.Series] = {}
    for key, grp in specific.groupby("osm_key"):
        lkp = grp.set_index("osm_value")["shared_label"]
        lookups[key] = lkp

    wildcards: dict[str, str] = {}
    for _, row in wildcards_df.iterrows():
        wildcards[row["osm_key"]] = row["shared_label"]

    return lookups, wildcards


def assign_osm_shared_label(
    gdf: pd.DataFrame,
    osm_crosswalk: pd.DataFrame,
    match_radii: pd.DataFrame,
    filter_keys: list[str],
    default_radius_m: float = 100.0,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Assign a ``shared_label`` and ``match_radius_m`` to each OSM POI.

    Uses *filter_keys* in priority order (first non-null match wins),
    falling back to the wildcard row for that key if the specific
    value is not in the crosswalk.

    Returns:
        (shared_label ndarray of object, match_radius_m ndarray of
        float)
    """
    n = len(gdf)
    label = np.full(n, "", dtype = object)
    radius = np.full(n, default_radius_m, dtype = np.float64)
    matched = np.zeros(n, dtype = bool)

    lookups, wildcards = _build_osm_label_lookups(osm_crosswalk)

    # Build radius dict from match_radii DataFrame
    radii_dict: dict[str, float] = {}
    for _, row in match_radii.iterrows():
        radii_dict[row["shared_label"]] = float(
            row["match_radius_m"]
        )

    for key in filter_keys:
        if key not in gdf.columns:
            continue

        col = gdf[key]
        has_value = col.notna() & (col != "") & ~matched
        if not has_value.any():
            continue

        eligible_idx = np.where(has_value)[0]
        eligible_vals = col.to_numpy()[eligible_idx]

        lkp = lookups.get(key)
        if lkp is not None:
            mapped_label = (
                pd.Series(eligible_vals, dtype = str).map(lkp)
            )
            found = mapped_label.notna().to_numpy()
            pos = eligible_idx[found]
            labels_found = mapped_label.to_numpy()[found]
            label[pos] = labels_found
            radius[pos] = np.array(
                [
                    radii_dict.get(lb, default_radius_m)
                    for lb in labels_found
                ]
            )
            matched[pos] = True

            not_found = eligible_idx[~found]
        else:
            not_found = eligible_idx

        wildcard_label = wildcards.get(key)
        if wildcard_label is not None and len(not_found) > 0:
            label[not_found] = wildcard_label
            radius[not_found] = radii_dict.get(
                wildcard_label, default_radius_m,
            )
            matched[not_found] = True

    return label, radius


# -----------------------------------------------------------------
# Shared-label assignment — Overture
# -----------------------------------------------------------------


def assign_overture_shared_label(
    gdf: pd.DataFrame,
    overture_crosswalk: pd.DataFrame,
    match_radii: pd.DataFrame,
    default_radius_m: float = 100.0,
) -> tuple[np.ndarray, np.ndarray]:
    """
    Assign a ``shared_label`` and ``match_radius_m`` to each
    Overture POI.

    Matches by ``(overture_l0, overture_l1)``.  Falls back to
    l0-only match when l1 is missing or not found.

    Returns:
        (shared_label ndarray of object, match_radius_m ndarray of
        float)
    """
    n = len(gdf)
    label = np.full(n, "", dtype = object)
    radius = np.full(n, default_radius_m, dtype = np.float64)

    cw = overture_crosswalk.copy()

    # Build radius dict
    radii_dict: dict[str, float] = {}
    for _, row in match_radii.iterrows():
        radii_dict[row["shared_label"]] = float(
            row["match_radius_m"]
        )

    # Exact (l0, l1) lookup — deduplicate, keep first
    exact_rows = cw[cw["overture_l1"] != ""].copy()
    exact_rows["_key"] = (
        exact_rows["overture_l0"]
        + "|"
        + exact_rows["overture_l1"]
    )
    exact_lkp = (
        exact_rows.drop_duplicates("_key")
        .set_index("_key")["shared_label"]
    )

    # l0-only fallback (rows with empty l1)
    l0_rows = cw[cw["overture_l1"] == ""].copy()
    l0_lkp = (
        l0_rows.drop_duplicates("overture_l0")
        .set_index("overture_l0")["shared_label"]
    )

    # Build composite keys from the dataset
    l0_col = (
        gdf["taxonomy_l0"].fillna("")
        if "taxonomy_l0" in gdf.columns
        else pd.Series("", index = gdf.index)
    )
    l1_col = (
        gdf["taxonomy_l1"].fillna("")
        if "taxonomy_l1" in gdf.columns
        else pd.Series("", index = gdf.index)
    )
    composite_key = l0_col.astype(str) + "|" + l1_col.astype(str)

    # Phase 1: exact (l0, l1) match
    mapped_label = composite_key.map(exact_lkp)
    found = mapped_label.notna().to_numpy()
    labels_found = mapped_label.to_numpy()[found]
    label[found] = labels_found
    radius[found] = np.array(
        [
            radii_dict.get(lb, default_radius_m)
            for lb in labels_found
        ]
    )

    # Phase 2: l0-only fallback for unmatched
    remaining = ~found & (l0_col != "")
    if remaining.any():
        fb_label = l0_col[remaining].map(l0_lkp)
        fb_found = fb_label.notna().to_numpy()
        remaining_idx = np.where(remaining)[0]
        pos = remaining_idx[fb_found]
        fb_labels = fb_label.to_numpy()[fb_found]
        label[pos] = fb_labels
        radius[pos] = np.array(
            [
                radii_dict.get(lb, default_radius_m)
                for lb in fb_labels
            ]
        )

    return label, radius


# -----------------------------------------------------------------
# L0 bitmask helpers (for type scoring)
# -----------------------------------------------------------------


def compute_osm_l0_bits(
    gdf: pd.DataFrame,
    top_level_matches: pd.DataFrame,
) -> np.ndarray:
    """
    For each OSM POI, compute a uint8 bitmask encoding which
    Overture L0 categories it broadly matches.

    A non-null value in an OSM tag key (e.g. ``amenity``) sets the
    bit(s) for every L0 linked to that key via *top_level_matches*.
    For example, ``amenity`` maps to both ``arts_and_entertainment``
    (bit 1) and ``food_and_drink`` (bit 2), so any POI with a
    non-null ``amenity`` value gets ``1 | 2 = 3``.
    """
    # Build osm_key -> combined bit value
    key_bits: dict[str, int] = {}
    for _, row in top_level_matches.iterrows():
        osm_key = row["osm_key"]
        l0 = row["overture_l0"]
        bit = L0_BIT.get(l0, 0)
        key_bits[osm_key] = key_bits.get(osm_key, 0) | bit

    bits = np.zeros(len(gdf), dtype = np.uint8)
    for osm_key, bval in key_bits.items():
        if osm_key in gdf.columns:
            has_val = gdf[osm_key].notna() & (
                gdf[osm_key] != ""
            )
            bits[has_val] |= bval

    return bits


def compute_overture_l0_bits(
    l0_array: np.ndarray,
) -> np.ndarray:
    """
    For each Overture POI, compute a uint8 bitmask from its
    ``taxonomy_l0`` value.  Each POI has at most one L0 category,
    so a single bit is set.
    """
    bits = np.zeros(len(l0_array), dtype = np.uint8)
    for l0, bval in L0_BIT.items():
        mask = l0_array == l0
        bits[mask] = bval
    return bits
