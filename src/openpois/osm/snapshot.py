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
import tempfile
from pathlib import Path

import geopandas as gpd
import osmium
import pandas as pd
import requests

from openpois.osm._poi_handler import POIRecordBuilder


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
    # Write to a temp file in the same directory, then rename atomically so
    # that a partial download never masquerades as a complete file.
    with tempfile.NamedTemporaryFile(
        dir=output_path.parent, delete=False, suffix=".tmp"
    ) as tmp:
        tmp_path = Path(tmp.name)
    try:
        with requests.get(url, stream=True, timeout=(30, None)) as resp:
            resp.raise_for_status()
            total = int(resp.headers.get("content-length", 0))
            downloaded = 0
            with open(tmp_path, "wb") as f:
                for chunk in resp.iter_content(chunk_size=8 * 1024 * 1024):
                    f.write(chunk)
                    downloaded += len(chunk)
                    if total:
                        pct = 100 * downloaded / total
                        print(f"  {pct:.1f}%", end="\r")
        tmp_path.rename(output_path)
    except Exception:
        tmp_path.unlink(missing_ok=True)
        raise
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
    Runs osmium tags-filter to extract nodes, ways, and relations matching the given keys.

    Constructs and runs a command of the form:
        osmium tags-filter -o {output_pbf} {input_pbf} nwr/{key1} nwr/{key2} ...

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
    osmium_bin = (
        shutil.which("osmium") or (str(_env_bin) if _env_bin.exists() else "osmium")
    )
    key_args = [f"nwr/{key}" for key in osm_keys]
    cmd = [
        osmium_bin, "tags-filter", "--overwrite", "-o", str(output_pbf), str(input_pbf)
    ] + key_args
    print(f"Running: {' '.join(cmd)}")
    subprocess.run(cmd, check=True)
    print(f"Filtered PBF written to {output_pbf}")
    return output_pbf


# -----------------------------------------------------------------------------
# pyosmium parsing
# -----------------------------------------------------------------------------


def _flush_chunk(
    records: list[dict],
    chunk_dir: Path,
    chunk_idx: int,
) -> Path:
    """Write a list of record dicts to a temporary GeoParquet chunk file."""
    df = pd.DataFrame(records)
    gdf = gpd.GeoDataFrame(df, geometry = "geometry", crs = "EPSG:4326")
    chunk_path = chunk_dir / f"chunk_{chunk_idx:04d}.parquet"
    gdf.to_parquet(chunk_path)
    return chunk_path


def parse_pbf_to_geodataframe(
    pbf_path: Path,
    filter_keys: list[str] | None = None,
    extract_keys: list[str] | None = None,
    source_label: str = "osm",
    chunk_size: int = 500_000,
    verbose: bool = True,
) -> gpd.GeoDataFrame:
    """
    Parses a filtered PBF file with pyosmium and returns a GeoDataFrame.

    Uses ``osmium.FileProcessor`` with a disk-backed location index and
    writes records in chunks to temporary parquet files to avoid exhausting
    memory on large extracts.

    Processes nodes as Point geometries, closed ways as Polygon geometries
    (open ways use their centroid), and multipolygon relations as
    Polygon/MultiPolygon geometries. Node location resolution requires that
    the PBF was NOT filtered with --omit-referenced so that referenced node
    coordinates are present in the file.

    Args:
        pbf_path: Path to the (pre-filtered) PBF file.
        filter_keys: Tag keys used to decide which elements to keep. An
            element is accepted if it has at least one of these keys. If
            None, all elements are accepted.
        extract_keys: Tag keys to include as output columns (None for
            absent values). If None, all tags on accepted elements are
            extracted.
        source_label: Value written to the 'source' column.
        chunk_size: Number of POI records to accumulate before flushing to a
            temporary parquet file. Lower values reduce peak memory usage.
        verbose: If True, log progress after each chunk is flushed.

    Returns:
        GeoDataFrame with columns:
            source, osm_id (int64), osm_type ('node'|'way'|'relation'),
            tag columns, name, geometry. CRS is EPSG:4326.
    """
    builder = POIRecordBuilder(
        source_label = source_label,
        filter_keys = filter_keys,
        extract_keys = extract_keys,
    )
    if verbose:
        print(
            f"Parsing {pbf_path} with pyosmium"
            f" (chunk_size={chunk_size:,})..."
        )

    with tempfile.TemporaryDirectory(
        prefix = "osm_chunks_"
    ) as tmp_dir:
        chunk_dir = Path(tmp_dir)
        records: list[dict] = []
        chunk_idx = 0
        total_records = 0

        loc_cache = str(chunk_dir / "locations.dat")
        fp = osmium.FileProcessor(str(pbf_path)) \
            .with_locations(f"sparse_file_array,{loc_cache}") \
            .with_areas()

        for obj in fp:
            rec = None
            if obj.is_node():
                rec = builder.process_node(obj)
            elif obj.is_way():
                rec = builder.process_way(obj)
            elif obj.is_area():
                rec = builder.process_area(obj)

            if rec is not None:
                records.append(rec)
                if len(records) >= chunk_size:
                    _flush_chunk(records, chunk_dir, chunk_idx)
                    total_records += len(records)
                    if verbose:
                        print(
                            f"  Finished chunk {chunk_idx}"
                            f" ({total_records:,} records so far)"
                        )
                    records.clear()
                    chunk_idx += 1

        # Flush remaining records
        if records:
            _flush_chunk(records, chunk_dir, chunk_idx)
            total_records += len(records)
            chunk_idx += 1

        if total_records == 0:
            extra_cols = list(extract_keys) if extract_keys is not None else []
            return gpd.GeoDataFrame(
                columns = [
                    "source", "osm_id", "osm_type", "name", "geometry"
                ] + extra_cols,
                geometry = "geometry",
                crs = "EPSG:4326",
            )

        # Read and concatenate all chunk files
        chunk_files = sorted(chunk_dir.glob("chunk_*.parquet"))
        gdf = pd.concat(
            [gpd.read_parquet(f) for f in chunk_files],
            ignore_index = True,
        )
        gdf = gpd.GeoDataFrame(gdf, geometry = "geometry", crs = "EPSG:4326")

    if verbose:
        print(
            f"Parsed {len(gdf):,} OSM POIs"
            f" ({gdf['osm_type'].value_counts().to_dict()})"
        )
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
    keep_all_keys: bool = False,
    chunk_size: int = 500_000,
    verbose: bool = True,
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
        osm_keys: OSM tag keys used to filter elements in the PBF. Elements
            lacking all of these keys are excluded.
        overwrite_download: Re-download even if raw_pbf_path exists.
        overwrite_filter: Re-filter even if filtered_pbf_path exists.
        source_label: Value for the output 'source' column.
        keep_all_keys: If True, all OSM tags are retained as columns in the
            output GeoDataFrame, not just those in osm_keys. osm_keys is still
            used to filter which elements are included.
        chunk_size: Number of POI records per temporary parquet chunk during
            parsing. Lower values reduce peak memory usage.
        verbose: If True, log progress after each chunk is flushed.

    Returns:
        GeoDataFrame written to output_path.
    """
    output_path = Path(output_path)

    download_pbf(
        url = pbf_url,
        output_path = raw_pbf_path,
        overwrite = overwrite_download,
    )
    filter_pbf(
        input_pbf = raw_pbf_path,
        output_pbf = filtered_pbf_path,
        osm_keys = osm_keys,
        overwrite = overwrite_filter,
    )
    extract_keys = None if keep_all_keys else osm_keys
    gdf = parse_pbf_to_geodataframe(
        pbf_path = filtered_pbf_path,
        filter_keys = osm_keys,
        extract_keys = extract_keys,
        source_label = source_label,
        chunk_size = chunk_size,
        verbose = verbose,
    )

    output_path.parent.mkdir(parents=True, exist_ok=True)
    gdf.to_parquet(output_path)
    print(f"Saved OSM snapshot to {output_path}")
    return gdf
