"""
Download the current US Overture Maps Places snapshot as a GeoParquet file.

Queries Overture Maps GeoParquet files on public S3 using DuckDB's httpfs and
spatial extensions, filtering to the configured bounding box and L0 taxonomy
categories. No authentication required — Overture Maps data is publicly
accessible without credentials.

Auto-detects the latest available Overture release from S3 unless a specific
release_date is pinned in config.yaml.

Smoke test: narrow download.general.bbox in config.yaml to the Seattle area
to verify the DuckDB query before running the full CONUS download.

Config keys used (config.yaml):
    download.overture.release_date           — pinned release (null = auto-detect)
    download.overture.s3_bucket              — Overture Maps S3 bucket name
    download.overture.s3_region              — AWS region of the Overture bucket
    download.overture.taxonomy_l0_categories — L0 category filter list
    download.general.bbox                    — WGS-84 bbox [xmin, ymin, xmax, ymax]
    directories.snapshot_overture            — output directory

Output file:
    overture_snapshot.parquet — GeoParquet with ~7.2M US POIs
        Columns: overture_id, overture_name, taxonomy_l0, taxonomy_l1,
        taxonomy_l2, brand_name, confidence, geometry, source
"""
from config_versioned import Config
from openpois.io.overture import download_overture_snapshot

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
