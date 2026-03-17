#!/home/nathenry/miniforge3/envs/openpois/bin/python
"""
Summarize the conflated dataset by shared_label and source.

Reads conflated.parquet and produces a CSV with one row per shared_label
showing POI counts broken down by source (matched, osm, overture) and the
average composite match score for matched pairs.

Config keys used (config.yaml):
    conflation.conflated        — input GeoParquet path (conflated.parquet)
    conflation.summary_by_label — output CSV path

Prerequisites:
    Run scripts/conflation/conflate.py first.

Output file:
    summary_by_label.csv — columns: shared_label, matched, osm, overture,
        total, avg_match_score; sorted by total descending
"""
from __future__ import annotations

import pandas as pd
from config_versioned import Config

config = Config("~/repos/openpois/config.yaml")
INPUT_PATH = config.get_file_path("conflation", "conflated")
OUTPUT_DIR = config.get_dir_path("conflation")
output_path = config.get_file_path("conflation", "summary_by_label")

if __name__ == "__main__":
    print(f"Reading {INPUT_PATH} ...")
    df = pd.read_parquet(
        INPUT_PATH,
        columns = [
            "shared_label", "source", "match_score",
        ],
    )
    print(f"  {len(df):,} rows")

    # Pivot: count by (shared_label, source)
    counts = (
        df.groupby(["shared_label", "source"])
        .size()
        .unstack(fill_value = 0)
    )
    # Reorder columns
    for col in ["matched", "osm", "overture"]:
        if col not in counts.columns:
            counts[col] = 0
    counts = counts[["matched", "osm", "overture"]]
    counts["total"] = counts.sum(axis = 1)

    # Average match score per label (matched pairs only)
    matched = df[df["source"] == "matched"]
    avg_score = (
        matched.groupby("shared_label")["match_score"]
        .mean()
        .rename("avg_match_score")
    )
    summary = counts.join(avg_score).sort_values(
        "total", ascending = False,
    )
    summary.index.name = "shared_label"

    summary.to_csv(output_path)
    print(f"\nSaved to {output_path}")
    print(f"\n{summary.to_string()}")
