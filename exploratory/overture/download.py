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

config = Config("~/repos/openpois/config.yaml")

# None = auto-detect latest
RELEASE_DATE = config.get("download", "overture", "release_date", fail_if_none=False)
S3_BUCKET = config.get("download", "overture", "s3_bucket")
S3_REGION = config.get("download", "overture", "s3_region")
BBOX = config.get("download", "general", "bbox")
TAXONOMY_CATEGORIES = config.get("download", "overture", "taxonomy_l0_categories")
SAVE_DIR = config.get_dir_path("snapshot_overture")

SAVE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = config.get_file_path("snapshot_overture", "snapshot")


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
