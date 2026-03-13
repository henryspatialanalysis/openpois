# openpois

A Python library for modeling POI (Point of Interest) stability over time using historical OpenStreetMap data, with utilities for downloading current POI snapshots from multiple sources.

## Setup

```bash
make build_env        # Create conda environment from environment.yml
make install_package  # Install openpois in editable mode
```

## POI Snapshot Downloads

Three exploratory scripts download current US-wide POI snapshots from different sources. All output GeoParquet to `~/data/`.

### OpenStreetMap

Downloads the Geofabrik US extract (~11 GB), filters to POI-relevant tags with osmium-tool, and parses with pyosmium.

```bash
python exploratory/osm_snapshot/download.py
```

Output: `~/data/openpois/snapshots/osm/<VERSION>/osm_snapshot.parquet` (~7.8M POIs)

### Overture Maps

Queries the public Overture Maps S3 bucket directly via DuckDB. No authentication required.

```bash
python exploratory/overture/download.py
```

Output: `~/data/openpois/snapshots/overture/<VERSION>/overture_snapshot.parquet` (~7.2M POIs)

### Foursquare OS Places

Queries the Foursquare Places Portal Apache Iceberg catalog. Requires a free API token from [places.foursquare.com](https://places.foursquare.com).

```bash
export FSQ_PORTAL_TOKEN="<your_token>"
python exploratory/foursquare/download.py
```

Output: `~/data/openpois/snapshots/foursquare/<VERSION>/foursquare_snapshot.parquet` (~8.3M POIs)

### Configuration

All download settings (bounding boxes, category filters, release dates, output paths) are in `config.yaml`. Set `release_date: null` under any source to auto-detect the latest available snapshot.

---

## Historical OSM Change-Rate Modeling

The core workflow models how long POI tags remain stable over time using historical OSM data.

```bash
python exploratory/osm_data/download.py       # Download OSM history for a bounding box
python exploratory/osm_data/format_tabular.py # Format into observation records
python exploratory/models/pytorch_simple.py   # Fit Poisson change-rate model
```

---

## Development

```bash
pytest                # Run tests
make export_env       # Export conda environment after adding dependencies
```
