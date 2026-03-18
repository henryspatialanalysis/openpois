#!/home/nathenry/miniforge3/envs/openpois/bin/python
"""
Conflate rated OSM POIs with Overture Maps POIs into a unified dataset.

Reads both snapshots, assigns each POI a shared taxonomy label via CSV
crosswalk files, finds spatial candidates within per-category radii using a
BallTree, scores candidate pairs on distance, name similarity, type agreement,
and shared identifiers, performs greedy one-to-one matching, and merges all
POIs (matched and unmatched) into a single GeoParquet output.

Config keys used (config.yaml):
    snapshot_osm.rated_snapshot            — rated OSM GeoParquet input path
    snapshot_overture.snapshot             — Overture GeoParquet input path
    conflation.conflated                   — output GeoParquet path
    download.osm.filter_keys               — tag keys used for taxonomy assignment
    conflation.overture_confidence_weight  — weight on Overture confidence in scoring
    conflation.min_match_score             — minimum composite score to accept a match
    conflation.max_radius_m                — maximum candidate search radius in meters
    conflation.default_radius_m            — fallback radius for unclassified POIs
    conflation.distance_weight             — scoring weight for spatial distance
    conflation.name_weight                 — scoring weight for name similarity
    conflation.type_weight                 — scoring weight for taxonomy agreement
    conflation.identifier_weight           — scoring weight for shared identifiers
    conflation.chunk_size                  — BallTree chunk size for memory management
    conflation.test_bbox                   — small bbox used with --test flag

Usage:
    python scripts/conflation/conflate.py           # full CONUS run
    python scripts/conflation/conflate.py --test    # Seattle test bbox

Output file:
    conflated.parquet — GeoParquet with all OSM + Overture POIs, columns:
        shared_label, source (matched/osm/overture), match_score,
        osm_id, overture_id, name, conf_mean/lower/upper, geometry, ...
"""
from __future__ import annotations

import argparse
import gc
import time

import geopandas as gpd
import numpy as np
import pyarrow.parquet as pq
from config_versioned import Config
from shapely.geometry import box

from openpois.conflation.match import (
    compute_match_scores,
    find_spatial_candidates,
    select_best_matches,
)
from openpois.conflation.merge import (
    build_merge_parts,
    save_conflated_from_parts,
)
from openpois.conflation.taxonomy import (
    assign_osm_shared_label,
    assign_overture_shared_label,
    compute_osm_l0_bits,
    compute_overture_l0_bits,
    load_match_radii,
    load_osm_crosswalk,
    load_overture_crosswalk,
    load_top_level_matches,
)


# -----------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

OSM_PATH = config.get_file_path("snapshot_osm", "rated_snapshot")
OVERTURE_PATH = config.get_file_path("snapshot_overture", "snapshot")
OUTPUT_PATH = config.get_file_path("conflation", "conflated")

FILTER_KEYS = config.get("download", "osm", "filter_keys")

OVERTURE_CONF_WEIGHT = config.get(
    "conflation", "overture_confidence_weight"
)
MIN_MATCH_SCORE = config.get("conflation", "min_match_score")
MAX_RADIUS_M = config.get("conflation", "max_radius_m")
DEFAULT_RADIUS_M = config.get("conflation", "default_radius_m")
DISTANCE_WEIGHT = config.get("conflation", "distance_weight")
NAME_WEIGHT = config.get("conflation", "name_weight")
TYPE_WEIGHT = config.get("conflation", "type_weight")
IDENTIFIER_WEIGHT = config.get("conflation", "identifier_weight")
CHUNK_SIZE = config.get("conflation", "chunk_size")
TEST_BBOX = config.get("conflation", "test_bbox")

# Columns needed for matching (memory optimization)
OSM_MATCH_COLS = [
    "osm_id", "osm_type", "name", "brand", "brand:wikidata",
    "website", "phone", "amenity", "shop", "healthcare", "leisure",
    "conf_mean", "conf_lower", "conf_upper", "geometry",
]
OVERTURE_MATCH_COLS = [
    "overture_id", "taxonomy_l0", "taxonomy_l1", "taxonomy_l2",
    "overture_name", "brand_name", "confidence", "geometry",
]


# -----------------------------------------------------------------
# Main
# -----------------------------------------------------------------


def _load_gdf(
    path, columns, test_bbox = None, label = "dataset"
):
    """Load a GeoParquet, optionally filtering to a test bbox."""
    print(f"Loading {label} from {path} ...")
    # Read only needed columns
    avail_cols = pq.read_schema(path).names
    cols = [c for c in columns if c in avail_cols]
    gdf = gpd.read_parquet(path, columns = cols)
    print(f"  Loaded {len(gdf):,} rows ({len(cols)} columns)")

    if test_bbox is not None:
        bbox_geom = box(
            test_bbox["xmin"], test_bbox["ymin"],
            test_bbox["xmax"], test_bbox["ymax"],
        )
        gdf = gdf[gdf.geometry.within(bbox_geom)].reset_index(
            drop = True
        )
        print(
            f"  Filtered to test bbox: {len(gdf):,} rows"
        )

    return gdf


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = "Conflate OSM and Overture Maps POIs."
    )
    parser.add_argument(
        "--test",
        action = "store_true",
        help = (
            "Filter both datasets to a small bbox "
            "(Seattle area) for testing."
        ),
    )
    args = parser.parse_args()
    t0 = time.time()

    test_bbox = TEST_BBOX if args.test else None

    # -- Load data -------------------------------------------------
    osm_gdf = _load_gdf(
        OSM_PATH, OSM_MATCH_COLS,
        test_bbox = test_bbox, label = "OSM rated",
    )
    overture_gdf = _load_gdf(
        OVERTURE_PATH, OVERTURE_MATCH_COLS,
        test_bbox = test_bbox, label = "Overture",
    )

    # -- Taxonomy assignment ---------------------------------------
    print("\nAssigning shared labels ...")
    osm_crosswalk = load_osm_crosswalk()
    overture_crosswalk = load_overture_crosswalk()
    match_radii = load_match_radii()
    top_level_matches = load_top_level_matches()

    osm_shared_labels, osm_radii = assign_osm_shared_label(
        osm_gdf, osm_crosswalk, match_radii, FILTER_KEYS,
        default_radius_m = DEFAULT_RADIUS_M,
    )
    overture_shared_labels, overture_radii = (
        assign_overture_shared_label(
            overture_gdf, overture_crosswalk, match_radii,
            default_radius_m = DEFAULT_RADIUS_M,
        )
    )

    osm_assigned = np.sum(osm_shared_labels != "")
    ov_assigned = np.sum(overture_shared_labels != "")
    print(
        f"  OSM: {osm_assigned:,}/{len(osm_gdf):,} assigned"
    )
    print(
        f"  Overture: {ov_assigned:,}/{len(overture_gdf):,}"
        f" assigned"
    )

    # Compute L0 bitmasks BEFORE dropping tag columns
    osm_l0_bits = compute_osm_l0_bits(
        osm_gdf, top_level_matches,
    )
    overture_l0_bits = compute_overture_l0_bits(
        overture_gdf["taxonomy_l0"].fillna("").to_numpy(),
    )

    # Drop columns only needed for taxonomy assignment
    for col in [
        "amenity", "shop", "healthcare", "leisure",
        "osm_type", "brand:wikidata", "website", "phone",
    ]:
        if col in osm_gdf.columns:
            osm_gdf.drop(columns = col, inplace = True)
    for col in ["taxonomy_l0", "taxonomy_l1", "taxonomy_l2"]:
        if col in overture_gdf.columns:
            overture_gdf.drop(columns = col, inplace = True)
    gc.collect()

    # -- Spatial candidates ----------------------------------------
    print(f"\nFinding spatial candidates (max {MAX_RADIUS_M}m) ...")
    candidates = find_spatial_candidates(
        osm_geom = osm_gdf.geometry.values,
        overture_geom = overture_gdf.geometry.values,
        osm_radii_m = osm_radii,
        max_radius_m = MAX_RADIUS_M,
        chunk_size = CHUNK_SIZE,
    )
    print(f"  Found {len(candidates):,} candidate pairs")
    gc.collect()

    if candidates.empty:
        print("No spatial candidates found. Merging all as unmatched.")
        matches = candidates
    else:
        # -- Scoring -----------------------------------------------
        print("\nScoring candidates ...")
        osm_names = osm_gdf["name"].to_numpy()
        osm_brands = (
            osm_gdf["brand"].to_numpy()
            if "brand" in osm_gdf.columns
            else np.full(len(osm_gdf), None, dtype = object)
        )
        overture_names = overture_gdf["overture_name"].to_numpy()
        overture_brands = (
            overture_gdf["brand_name"].to_numpy()
            if "brand_name" in overture_gdf.columns
            else np.full(
                len(overture_gdf), None, dtype = object
            )
        )

        scored = compute_match_scores(
            candidates = candidates,
            osm_names = osm_names,
            osm_brands = osm_brands,
            overture_names = overture_names,
            overture_brands = overture_brands,
            osm_shared_labels = osm_shared_labels,
            overture_shared_labels = overture_shared_labels,
            osm_radii_m = osm_radii,
            osm_l0_bits = osm_l0_bits,
            overture_l0_bits = overture_l0_bits,
            distance_weight = DISTANCE_WEIGHT,
            name_weight = NAME_WEIGHT,
            type_weight = TYPE_WEIGHT,
            identifier_weight = IDENTIFIER_WEIGHT,
        )
        print(
            f"  Mean composite score: "
            f"{scored['composite_score'].mean():.3f}"
        )

        # -- Best matches ------------------------------------------
        print(
            f"\nSelecting best matches "
            f"(min_score={MIN_MATCH_SCORE}) ..."
        )
        matches = select_best_matches(
            scored, min_score = MIN_MATCH_SCORE,
        )
        print(f"  Selected {len(matches):,} one-to-one matches")

        # Free scoring intermediates
        del scored, candidates
        del osm_names, osm_brands
        del overture_names, overture_brands
        gc.collect()

    # -- Merge (disk-backed to limit memory) -----------------------
    print("\nMerging into unified dataset ...")
    match_score_mean = (
        matches["composite_score"].mean() if len(matches) > 0
        else float("nan")
    )
    match_dist_mean = (
        matches["distance_m"].mean() if len(matches) > 0
        else float("nan")
    )
    n_matches = len(matches)

    part_paths = build_merge_parts(
        osm_gdf = osm_gdf,
        overture_gdf = overture_gdf,
        matches = matches,
        osm_shared_labels = osm_shared_labels,
        overture_shared_labels = overture_shared_labels,
        overture_confidence_weight = OVERTURE_CONF_WEIGHT,
    )

    # Free ALL source data before concat+save
    del osm_gdf, overture_gdf, matches
    del osm_shared_labels, overture_shared_labels, osm_radii
    del osm_l0_bits, overture_l0_bits
    gc.collect()

    # -- Save ------------------------------------------------------
    print("\nSaving conflated dataset ...")
    n_total = save_conflated_from_parts(part_paths, OUTPUT_PATH)
    config.write_self("conflation")

    # -- Summary ---------------------------------------------------
    elapsed = time.time() - t0
    print(f"\n{'=' * 60}")
    print(f"Conflation complete in {elapsed:.0f}s")
    print(f"  Total POIs:     {n_total:,}")
    if n_matches > 0:
        print(f"  Matched:        {n_matches:,}")
        print(
            f"  Mean match score: {match_score_mean:.3f}"
        )
        print(
            f"  Mean match distance: {match_dist_mean:.1f}m"
        )
    print(f"  Output: {OUTPUT_PATH}")
