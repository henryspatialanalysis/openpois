#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module downloads a current/latest POI snapshot for the United States from
OpenStreetMap using a Geofabrik PBF extract, osmium-tool CLI pre-filtering,
and pyosmium parsing.

It is broken into the following functions:
- download_pbf: Downloads a PBF file from a URL via streaming HTTP.
- filter_pbf: Runs osmium tags-filter to produce a reduced POI-only PBF.
- parse_pbf_to_geodataframe: Parses the filtered PBF with pyosmium into a
    GeoDataFrame of nodes (Points) and ways (Polygons or Points).
- download_osm_snapshot: End-to-end orchestrator.

Data source: https://download.geofabrik.de/north-america/us-latest.osm.pbf
(~11 GB, updated daily). The osmium-tool CLI must be installed and on PATH
(conda install -c conda-forge osmium-tool).

Note: This module is separate from openpois.osm.download, which fetches
historical OSM element version data for change-rate modeling. This module
downloads a current snapshot only.
"""
from __future__ import annotations

import shutil
import subprocess
import sys
from pathlib import Path

import geopandas as gpd
import osmium
import pandas as pd
import requests
from shapely.geometry import LineString, Point, Polygon


# -----------------------------------------------------------------------------
# Download helper
# -----------------------------------------------------------------------------


def download_pbf(
    url: str,
    output_path: Path,
    overwrite: bool = False,
) -> Path:
    """
    Downloads a PBF file from the given URL to output_path via streaming HTTP.

    Args:
        url: URL of the PBF file to download (e.g., a Geofabrik extract).
        output_path: Local path to save the downloaded PBF.
        overwrite: If False and output_path already exists, skip the download.

    Returns:
        Path to the downloaded PBF file.

    Raises:
        requests.HTTPError: If the HTTP request fails.
    """
    output_path = Path(output_path)
    if output_path.exists() and not overwrite:
        print(f"PBF already exists at {output_path}; skipping download.")
        return output_path

    output_path.parent.mkdir(parents=True, exist_ok=True)
    print(f"Downloading PBF from {url} to {output_path}...")
    with requests.get(url, stream=True, timeout=600) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        downloaded = 0
        with open(output_path, "wb") as f:
            for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                f.write(chunk)
                downloaded += len(chunk)
                if total:
                    pct = 100 * downloaded / total
                    print(f"  {pct:.1f}%", end="\r")
    print(f"\nDownload complete: {output_path}")
    return output_path


# -----------------------------------------------------------------------------
# osmium-tool filtering
# -----------------------------------------------------------------------------


def filter_pbf(
    input_pbf: Path,
    output_pbf: Path,
    osm_keys: list[str],
    overwrite: bool = False,
) -> Path:
    """
    Runs osmium tags-filter to extract nodes and ways matching the given keys.

    Constructs and runs a command of the form:
        osmium tags-filter -o {output_pbf} {input_pbf} nw/{key1} nw/{key2} ...

    The referenced nodes for matched ways are retained so that way geometries
    can be resolved by pyosmium in a subsequent step.

    Args:
        input_pbf: Path to the full PBF extract.
        output_pbf: Path to write the filtered output PBF.
        osm_keys: OSM tag keys to retain (e.g., ['amenity', 'shop']).
        overwrite: If False and output_pbf exists, skip filtering.

    Returns:
        Path to the filtered PBF file.

    Raises:
        subprocess.CalledProcessError: If osmium exits with non-zero status.
        FileNotFoundError: If osmium is not installed or not on PATH.
    """
    output_pbf = Path(output_pbf)
    if output_pbf.exists() and not overwrite:
        print(f"Filtered PBF already exists at {output_pbf}; skipping filter.")
        return output_pbf

    output_pbf.parent.mkdir(parents=True, exist_ok=True)
    # Look for osmium on PATH first, then in the same bin dir as Python
    _env_bin = Path(sys.executable).parent / "osmium"
    osmium_bin = shutil.which("osmium") or (str(_env_bin) if _env_bin.exists() else "osmium")
    key_args = [f"nw/{key}" for key in osm_keys]
    cmd = [osmium_bin, "tags-filter", "-o", str(output_pbf), str(input_pbf)] + key_args
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print(f"Filtered PBF written to {output_pbf}")
    return output_pbf


# -----------------------------------------------------------------------------
# pyosmium parsing
# -----------------------------------------------------------------------------


class _POIHandler(osmium.SimpleHandler):
    """
    pyosmium handler that collects nodes and ways with matching tag keys.
    """

    def __init__(self, osm_keys: list[str], source_label: str) -> None:
        super().__init__()
        self._osm_keys = set(osm_keys)
        self._source_label = source_label
        self.records: list[dict] = []

    def _extract_tags(self, tags: osmium.osm.TagList) -> dict:
        """Returns a dict of target-key tag values (None if absent)."""
        return {key: tags.get(key) for key in self._osm_keys}

    def _has_target_tag(self, tags: osmium.osm.TagList) -> bool:
        return any(key in tags for key in self._osm_keys)

    def node(self, n: osmium.osm.Node) -> None:
        if not self._has_target_tag(n.tags):
            return
        rec = {
            "source": self._source_label,
            "osm_id": n.id,
            "osm_type": "node",
            "name": n.tags.get("name"),
            "geometry": Point(n.location.lon, n.location.lat),
        }
        rec.update(self._extract_tags(n.tags))
        self.records.append(rec)

    def way(self, w: osmium.osm.Way) -> None:
        if not self._has_target_tag(w.tags):
            return
        try:
            coords = [(nd.lon, nd.lat) for nd in w.nodes]
        except osmium.InvalidLocationError:
            return
        if len(coords) < 2:
            return

        if coords[0] == coords[-1] and len(coords) >= 4:
            geom = Polygon(coords)
        else:
            geom = LineString(coords).centroid

        rec = {
            "source": self._source_label,
            "osm_id": w.id,
            "osm_type": "way",
            "name": w.tags.get("name"),
            "geometry": geom,
        }
        rec.update(self._extract_tags(w.tags))
        self.records.append(rec)


def parse_pbf_to_geodataframe(
    pbf_path: Path,
    osm_keys: list[str],
    source_label: str = "osm",
) -> gpd.GeoDataFrame:
    """
    Parses a filtered PBF file with pyosmium and returns a GeoDataFrame.

    Processes nodes as Point geometries and closed ways as Polygon geometries
    (open ways use their centroid). Node location resolution for ways requires
    that the PBF was NOT filtered with --omit-referenced, so that referenced
    node coordinates are present in the file.

    Args:
        pbf_path: Path to the (pre-filtered) PBF file.
        osm_keys: Tag keys to extract as individual columns. Each key becomes
            a nullable string column containing the tag value for that element.
        source_label: Value written to the 'source' column.

    Returns:
        GeoDataFrame with columns:
            source, osm_id (int64), osm_type, one column per osm_key,
            name, geometry. CRS is EPSG:4326.
    """
    handler = _POIHandler(osm_keys=osm_keys, source_label=source_label)
    print(f"Parsing {pbf_path} with pyosmium...")
    handler.apply_file(str(pbf_path), locations=True)

    if not handler.records:
        return gpd.GeoDataFrame(
            columns=["source", "osm_id", "osm_type", "name", "geometry"] + osm_keys,
            geometry="geometry",
            crs="EPSG:4326",
        )

    df = pd.DataFrame(handler.records)
    gdf = gpd.GeoDataFrame(df, geometry="geometry", crs="EPSG:4326")
    print(f"Parsed {len(gdf):,} OSM POIs ({gdf['osm_type'].value_counts().to_dict()})")
    return gdf


# -----------------------------------------------------------------------------
# Orchestrator
# -----------------------------------------------------------------------------


def download_osm_snapshot(
    pbf_url: str,
    raw_pbf_path: Path,
    filtered_pbf_path: Path,
    output_path: Path,
    osm_keys: list[str],
    overwrite_download: bool = False,
    overwrite_filter: bool = False,
    source_label: str = "osm",
) -> gpd.GeoDataFrame:
    """
    End-to-end orchestrator: download PBF, filter to POIs, parse, save GeoParquet.

    Steps:
    1. download_pbf  — streams the Geofabrik US extract (~11 GB) to raw_pbf_path.
    2. filter_pbf    — runs osmium tags-filter to produce a small POI-only PBF.
    3. parse_pbf_to_geodataframe — parses with pyosmium into a GeoDataFrame.
    4. Saves as GeoParquet at output_path.

    Steps 1 and 2 are skipped if the target files already exist unless
    overwrite_download / overwrite_filter are True.

    Args:
        pbf_url: URL of the full US PBF extract (e.g., Geofabrik US extract).
        raw_pbf_path: Local path to store the downloaded raw PBF.
        filtered_pbf_path: Local path to store the filtered PBF.
        output_path: Path to write the output GeoParquet file.
        osm_keys: OSM tag keys to filter on and extract as columns.
        overwrite_download: Re-download even if raw_pbf_path exists.
        overwrite_filter: Re-filter even if filtered_pbf_path exists.
        source_label: Value for the output 'source' column.

    Returns:
        GeoDataFrame written to output_path.
    """
    output_path = Path(output_path)

    download_pbf(url=pbf_url, output_path=raw_pbf_path, overwrite=overwrite_download)
    filter_pbf(
        input_pbf=raw_pbf_path,
        output_pbf=filtered_pbf_path,
        osm_keys=osm_keys,
        overwrite=overwrite_filter,
    )
    gdf = parse_pbf_to_geodataframe(
        pbf_path=filtered_pbf_path,
        osm_keys=osm_keys,
        source_label=source_label,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output_path)
    print(f"Saved OSM snapshot to {output_path}")
    return gdf
