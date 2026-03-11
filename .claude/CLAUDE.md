# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Environment Setup

```bash
make build_env       # Create conda environment from environment.yml
make install_package # Install openpois in editable mode (pip install -e .)
```

The conda environment is named `openpois` and requires Python 3.10+.

## Common Commands

```bash
pytest               # Run tests
make export_env      # Export conda environment to environment.yml after adding dependencies
```

Code style is enforced by Black (format on save in VSCode). Linting via flake8 and pylint, both configured in `pyproject.toml`.

## Architecture

**openpois** models POI (Point of Interest) stability over time using historical OpenStreetMap data. The workflow is:

1. **Download OSM history** (`src/openpois/osm/download.py`) — queries the Overpass API for element histories within a bounding box and date range, producing version/change tables
2. **Format observations** (`src/openpois/osm/format_observations.py`) — converts raw OSM version histories into observation records (one row per version) with flags for tag changes and deletions
3. **Model change rates** (`src/openpois/models/`) — fits an empirical Bayes model using PyTorch to estimate per-group POI change rates (λ) as a Poisson process
4. **Visualize stability** (`src/openpois/osm/change_plots.py`) — plots how long POI tags remain unchanged

The **exploratory/** scripts are end-to-end pipelines that call library functions using settings from `config.yaml`. They are not part of the installed package and serve as reference implementations.

### Key classes and files

- `EventRate` (`models/event_rate.py`) — wraps a constant or time-varying λ; computes change probabilities via integration
- `ModelFitter` (`models/model_fitter.py`) — fits λ using PyTorch L-BFGS optimizer with optional priors; supports parameter draws for uncertainty
- `pytorch_setup()` / `prepare_data_for_model()` (`models/setup.py`) — initializes torch (GPU/CPU) and prepares filtered, grouped observation data
- `download_element_histories()` (`osm/download.py`) — main entry point for OSM history acquisition (Overpass, Seattle bbox only — do NOT modify for nationwide use)

### Configuration

`config.yaml` holds all shared settings (bounding box, date ranges, OSM tag keys, model hyperparameters, output directory paths with versioning). The `config_versioned` package (external dependency) reads this file. Exploratory scripts load config at startup; library functions accept parameters directly.

- `.get()` raises `ValueError` for null config values — pass `fail_if_none=False` for optional fields like `release_date: null`

## POI Snapshot Downloads

Three separate utilities download current US-wide snapshots (separate from the historical OSM workflow):

### OSM (`src/openpois/osm/snapshot.py`)
- `download_pbf` / `filter_pbf` / `parse_pbf_to_geodataframe` / `download_osm_snapshot`
- Geofabrik US extract (~11 GB) → osmium tags-filter → pyosmium parse → GeoParquet
- `osmium` is in the conda env bin but NOT on shell PATH; code resolves it via `Path(sys.executable).parent / "osmium"`
- Run: `python exploratory/osm_snapshot/download.py`

### Overture Maps (`src/openpois/overture/download.py`)
- DuckDB + httpfs + spatial extensions; queries public S3 directly, no auth
- `taxonomy` field is a named STRUCT: use `taxonomy.hierarchy[1]` (not `taxonomy[1]`)
- `brand` is a singular struct (not array); geometry is native DuckDB GEOMETRY type requiring `LOAD spatial` and `ST_X()/ST_Y()`
- L0 category names (Feb 2026+): `food_and_drink`, `shopping`, `arts_and_entertainment`, `sports_and_recreation`, `health_care`
- Run: `python exploratory/overture/download.py`

### Foursquare OS Places (`src/openpois/foursquare/download.py`)
- PyIceberg `RestCatalog`; requires `warehouse="places"` parameter
- Catalog: `uri=https://catalog.h3-hub.foursquare.com/iceberg`, namespace=`datasets`, tables=`places_os` / `categories_os`
- Table is **unpartitioned** (no `dt` column); release date inferred from `last_updated_at` in partition metadata
- Row filter: `country = 'US' AND date_closed IS NULL` (no dt filter)
- `fsq_category_ids` arrives as numpy/pyarrow array — use `len(x) == 0` not `if not x:`
- Token in `FSQ_PORTAL_TOKEN` env var; run: `python exploratory/foursquare/download.py`
