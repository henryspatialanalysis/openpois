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
from openpois.foursquare.download import download_foursquare_snapshot

# -----------------------------------------------------------------------------
# Configuration constants
# -----------------------------------------------------------------------------

_cfg = Config("~/repos/openpois/config.yaml")

RELEASE_DATE = _cfg.get("foursquare", "release_date", fail_if_none=False)  # None = auto-detect latest
CATALOG_URI = _cfg.get("foursquare", "catalog_uri")
CATALOG_WAREHOUSE = _cfg.get("foursquare", "catalog_warehouse")
CATALOG_NAMESPACE = _cfg.get("foursquare", "catalog_namespace")
PLACES_TABLE = _cfg.get("foursquare", "places_table")
CATEGORIES_TABLE = _cfg.get("foursquare", "categories_table")
TOKEN_ENV_VAR = _cfg.get("foursquare", "token_env_var")
L1_CATEGORIES = _cfg.get("foursquare", "l1_category_names")
SAVE_DIR = _cfg.get_dir_path("foursquare_snapshot")

SAVE_DIR.mkdir(parents=True, exist_ok=True)

OUTPUT_PATH = SAVE_DIR / _cfg.get(
    "directories", "foursquare_snapshot", "files", "snapshot"
)


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
