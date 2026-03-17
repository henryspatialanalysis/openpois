#!/home/nathenry/miniforge3/envs/openpois/bin/python
"""
Apply OSM change-rate model predictions to the OSM POI snapshot.

Reads fitted model predictions for all versions that start with the configured
`model_stub`, then assigns a confidence estimate to every POI in the OSM
snapshot based on:
  1. Best-matching model group (by filter key priority order)
  2. Years since the element was last edited in OSM

Matching priority (first match wins):
  - For each key in `download > osm > filter_keys`, if a `<model_stub>_by_<key>`
    prediction set exists and the POI's value for that key appears in the
    predictions' group_name column, use those group-specific estimates.
  - If no filter key matches, fall back to `<model_stub>_constant`.

Output columns added to the snapshot:
  conf_mean, conf_lower, conf_upper  — confidence (1 - p_change) estimates
  t2_years                           — years since last OSM edit (rounded to
                                       0.1, capped at 10)
  model_version                      — which model version was used
  model_group                        — which group was matched, or "constant"

Saves as a spatially-sorted GeoParquet (Hilbert curve order, zstd compression,
50k-row row groups) for efficient cloud-native range reads from S3.
"""
from __future__ import annotations

import argparse
import io
from pathlib import Path

import geopandas as gpd
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
from config_versioned import Config

from openpois.models.apply import (
    PREDICTIONS_FILE,
    constant_lookup,
    group_lookup,
    load_predictions,
)


# -----------------------------------------------------------------------------
# Configuration
# -----------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

MODEL_STUB = config.get("osm_data", "apply_model", "model_stub")
FILTER_KEYS = config.get("download", "osm", "filter_keys")
SNAPSHOT_PATH = config.get_file_path("snapshot_osm", "snapshot")
OUTPUT_PATH = config.get_file_path("snapshot_osm", "rated_snapshot")

# Base directory containing all versioned model subdirectories
MODEL_BASE = Path(config.get_dir_path("model_output")).parent

# -----------------------------------------------------------------------------
# Main
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        description = "Apply change-rate model predictions to the OSM POI snapshot."
    )
    parser.add_argument(
        "--test",
        action = "store_true",
        help = "Load only the first 10,000 rows of the snapshot for testing.",
    )
    args = parser.parse_args()

    print(f"Model stub: {MODEL_STUB}")
    print(f"Filter keys: {FILTER_KEYS}")

    # -- Load predictions -------------------------------------------------------
    constant_version = f"{MODEL_STUB}_constant"
    constant_dir = MODEL_BASE / constant_version
    if not constant_dir.is_dir():
        raise FileNotFoundError(
            f"Constant model directory not found: {constant_dir}"
        )
    const_arr = constant_lookup(load_predictions(constant_dir))
    print(f"Loaded constant model from {constant_dir.name}")

    by_key_lookups: dict[str, tuple[list[str], np.ndarray]] = {}
    for key in FILTER_KEYS:
        version = f"{MODEL_STUB}_by_{key}"
        version_dir = MODEL_BASE / version
        if version_dir.is_dir() and (version_dir / PREDICTIONS_FILE).exists():
            groups, arr = group_lookup(load_predictions(version_dir))
            by_key_lookups[key] = (groups, arr)
            print(f"  Loaded {version} ({len(groups)} groups)")
        else:
            print(f"  No predictions found for {version}; will skip")

    # -- Read snapshot ----------------------------------------------------------
    print(f"\nReading OSM snapshot from {SNAPSHOT_PATH} ...")
    if args.test:
        # Read only the first row group from disk rather than all 7.8M rows,
        # then round-trip through BytesIO to preserve GeoParquet metadata.
        print("  (--test mode: loading first 10,000 rows only)")
        pf = pq.ParquetFile(SNAPSHOT_PATH)
        buf = io.BytesIO()
        pq.write_table(pf.read_row_group(0).slice(0, 10_000), buf)
        buf.seek(0)
        gdf = gpd.read_parquet(buf)
    else:
        gdf = gpd.read_parquet(SNAPSHOT_PATH)
    print(f"  {len(gdf):,} POIs loaded")

    n = len(gdf)

    # -- Compute years since last edit ------------------------------------------
    today = pd.Timestamp.now(tz = "UTC")
    last_edited = gdf["last_edited"]
    if last_edited.dt.tz is None:
        last_edited = last_edited.dt.tz_localize("UTC")
    n_null = last_edited.isna().sum()
    if n_null:
        raise ValueError(
            f"{n_null} rows have a null last_edited timestamp. "
            "Remove or impute these rows before applying the model."
        )
    elapsed_secs = (today - last_edited).dt.total_seconds().to_numpy()
    elapsed_years = elapsed_secs / (365.25 * 86_400)
    t2_years = np.clip(np.round(elapsed_years * 10) / 10, 0.0, 10.0)
    # t2_int_arr stays in numpy only; never written to gdf
    t2_int_arr = np.round(t2_years * 10).astype(int)
    gdf["t2_years"] = t2_years

    # -- Assign predictions via numpy arrays ------------------------------------
    # All matching and lookup work is done in numpy and written to gdf once at
    # the end, avoiding repeated pandas indexing overhead across 7.8M rows.

    # Initialize from constant model: single vectorized index → shape (n, 3)
    p_arr = const_arr[t2_int_arr].copy()   # columns: p_mean, p_lower, p_upper
    model_version_arr = np.full(n, constant_version, dtype = object)
    model_group_arr = np.full(n, "constant", dtype = object)
    matched = np.zeros(n, dtype = bool)

    for key in FILTER_KEYS:
        if key not in by_key_lookups:
            continue
        groups, group_arr = by_key_lookups[key]   # group_arr: (n_groups, 101, 3)

        # Map tag values to group indices; NaN and unknown values become -1.
        group_to_idx = {g: i for i, g in enumerate(groups)}
        group_ids = (
            gdf[key].map(group_to_idx).fillna(-1).astype(int).to_numpy()
        )

        eligible = ~matched & (group_ids >= 0)
        eli_pos = np.where(eligible)[0]
        if len(eli_pos) == 0:
            continue

        # Vectorized 2D fancy indexing: group_arr[m_gids, m_t2s] → (m, 3)
        p_arr[eli_pos] = group_arr[group_ids[eli_pos], t2_int_arr[eli_pos]]
        model_version_arr[eli_pos] = f"{MODEL_STUB}_by_{key}"
        model_group_arr[eli_pos] = gdf[key].to_numpy()[eli_pos]
        matched[eli_pos] = True

        print(f"  {MODEL_STUB}_by_{key}: matched {len(eli_pos):,} POIs")

    n_constant = int((~matched).sum())
    print(f"  {constant_version}: {n_constant:,} POIs (fallback)")

    # -- Assign back to GeoDataFrame --------------------------------------------
    # Convert change probability to confidence (1 - p).
    # Note: conf_lower = 1 - p_upper and conf_upper = 1 - p_lower.
    gdf["conf_mean"] = 1.0 - p_arr[:, 0]
    gdf["conf_lower"] = 1.0 - p_arr[:, 2]   # 1 - p_upper
    gdf["conf_upper"] = 1.0 - p_arr[:, 1]   # 1 - p_lower
    # Categorical dtype stores integer codes + a small lookup, saving ~90%
    # memory vs. object strings for these low-cardinality columns.
    gdf["model_version"] = pd.Categorical(model_version_arr)
    gdf["model_group"] = pd.Categorical(model_group_arr)

    # -- Spatial sort for cloud-native S3 reads ---------------------------------
    print("\nSorting by Hilbert curve index ...")
    hilbert_order = gdf.hilbert_distance()
    gdf = gdf.iloc[hilbert_order.argsort()].reset_index(drop = True)

    # -- Save -------------------------------------------------------------------
    OUTPUT_PATH.parent.mkdir(parents = True, exist_ok = True)
    print(f"Saving rated snapshot to {OUTPUT_PATH} ...")
    gdf.to_parquet(
        OUTPUT_PATH,
        compression = "zstd",
        row_group_size = 50_000,
    )
    print(f"Done. Saved {len(gdf):,} POIs.")
    print("\nModel version breakdown:")
    print(gdf["model_version"].value_counts().to_string())
