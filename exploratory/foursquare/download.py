"""
Exploratory script for downloading a current Foursquare OS Places snapshot.

This script uses openpois.foursquare.download to:
1. Authenticate to the Foursquare Places Portal Iceberg catalog.
2. Auto-detect or use a pinned release date.
3. Load US places filtered by L1 category and save as GeoParquet.

Authentication:
    Set the FSQ_PORTAL_TOKEN environment variable to your portal token before
    running. Register at https://places.foursquare.com to obtain a token.

    Example (bash):
        export FSQ_PORTAL_TOKEN="<your_token>"
        python exploratory/foursquare/download.py
"""
from config_versioned import Config
from openpois.io.foursquare import download_foursquare_snapshot

# -----------------------------------------------------------------------------
# Configuration constants
# -----------------------------------------------------------------------------

config = Config("~/repos/openpois/config.yaml")

# None = auto-detect latest
RELEASE_DATE = config.get("download", "foursquare", "release_date", fail_if_none=False)
CATALOG_URI = config.get("download", "foursquare", "catalog_uri")
CATALOG_WAREHOUSE = config.get("download", "foursquare", "catalog_warehouse")
CATALOG_NAMESPACE = config.get("download", "foursquare", "catalog_namespace")
PLACES_TABLE = config.get("download", "foursquare", "places_table")
CATEGORIES_TABLE = config.get("download", "foursquare", "categories_table")
TOKEN_ENV_VAR = config.get("download", "foursquare", "token_env_var")
L1_CATEGORIES = config.get("download", "foursquare", "l1_category_names")

SAVE_DIR = config.get_dir_path("snapshot_foursquare")
SAVE_DIR.mkdir(parents=True, exist_ok=True)
OUTPUT_PATH = config.get_file_path("snapshot_foursquare", "snapshot")


# -----------------------------------------------------------------------------
# Main workflow
# -----------------------------------------------------------------------------

if __name__ == "__main__":
    gdf = download_foursquare_snapshot(
        output_path=OUTPUT_PATH,
        l1_category_names=L1_CATEGORIES,
        catalog_uri=CATALOG_URI,
        catalog_warehouse=CATALOG_WAREHOUSE,
        catalog_namespace=CATALOG_NAMESPACE,
        places_table=PLACES_TABLE,
        categories_table=CATEGORIES_TABLE,
        token_env_var=TOKEN_ENV_VAR,
        release_date=RELEASE_DATE,
    )
    print(f"Saved {len(gdf):,} Foursquare POIs to {OUTPUT_PATH}")
