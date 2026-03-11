"""
Exploratory script for downloading a current Overture Maps Places snapshot.

This script uses openpois.overture.download to:
1. Auto-detect the latest Overture Maps release from S3 (or use a pinned date).
2. Query the S3 GeoParquet files via DuckDB, filtered by bbox and taxonomy.
3. Save the result as a GeoParquet file.

No authentication required. Data is public on S3.

Smoke test: temporarily narrow the bbox in config.yaml to the Seattle area
(use the existing 'download.bbox' values) to verify the query before running
the full CONUS download.
"""
from config_versioned import Config
from openpois.overture.download import download_overture_snapshot

# -----------------------------------------------------------------------------
# Configuration constants
# -----------------------------------------------------------------------------

_cfg = Config("~/repos/openpois/config.yaml")

# None = auto-detect latest
RELEASE_DATE = _cfg.get("overture", "release_date", fail_if_none=False)
S3_BUCKET = _cfg.get("overture", "s3_bucket")
S3_REGION = _cfg.get("overture", "s3_region")
BBOX = _cfg.get("overture", "bbox")
TAXONOMY_CATEGORIES = _cfg.get("overture", "taxonomy_l0_categories")
SAVE_DIR = _cfg.get_dir_path("overture_snapshot")

SAVE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = SAVE_DIR / _cfg.get(
    "directories", "overture_snapshot", "files", "snapshot"
)


# -----------------------------------------------------------------------------
# Main workflow
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    gdf = download_overture_snapshot(
        output_path=OUTPUT_PATH,
        taxonomy_l0_categories=TAXONOMY_CATEGORIES,
        bbox=BBOX,
        bucket=S3_BUCKET,
        s3_region=S3_REGION,
        release_date=RELEASE_DATE,
    )
    print(f"Saved {len(gdf):,} Overture POIs to {OUTPUT_PATH}")
