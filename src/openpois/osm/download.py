#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
This module downloads OpenStreetMap data for a given area of interest.

It is broken into the following functions:
- build_query_string: Builds an Overpass query string for a given date and bbox.
- build_date_range: Creates a list of dates for querying.
- collect_element_ids: Queries Overpass for element IDs across a date range.
- process_version: Extracts metadata and tags from a single element version.
- compare_tags: Compares two sets of tags to find changes.
- process_element: Processes an element's full history into versions and changes.
- download_element_histories: Downloads and parses histories for a table of elements.

"""
from __future__ import annotations

import datetime
import time
import xml.etree.ElementTree as ET
from collections import namedtuple
from typing import TYPE_CHECKING

import overpass
import pandas as pd
import requests

if TYPE_CHECKING:
    import geopandas as gpd


# -----------------------------------------------------------------------------
# Query helpers
# -----------------------------------------------------------------------------


def build_query_string(
    date: datetime.datetime,
    bbox: dict,
    keys: list,
    timeout: int,
) -> str:
    """
    Builds an Overpass query string for the given date, bbox, keys, and timeout.

    Args:
        date: The date to query for.
        bbox: The bounding box to query for. Must contain keys 'ymin', 'xmin',
            'ymax', 'xmax'.
        keys: The OSM keys to query for (e.g., ['amenity', 'shop']).
        timeout: The timeout for the query in seconds.

    Returns:
        A query string suitable for the Overpass API.
    """
    query_string = f"""
        [out:xml][timeout:{timeout}]
        [date:"{date.strftime("%Y-%m-%dT00:00:00Z")}"];
        (
    """

    def add_group(key: str) -> str:
        prefix = f"nwr({bbox['ymin']}, {bbox['xmin']}, {bbox['ymax']}, {bbox['xmax']})"
        return f"{prefix}[{key}];\n"

    for key in keys:
        query_string += add_group(key)
    query_string += """
        );
        out ids;
    """
    return query_string


def build_date_range(
    start_date: datetime.datetime,
    end_date: datetime.datetime,
    interval: datetime.timedelta,
) -> list[datetime.datetime]:
    """
    Creates a list of dates from start_date to end_date at the given interval.

    Args:
        start_date: The start date for the range.
        end_date: The end date for the range.
        interval: The interval between dates.

    Returns:
        A list of datetime objects from start_date to end_date.
    """
    date_range = [
        start_date + i * interval
        for i in range(((end_date - start_date) // interval) + 1)
    ]
    if date_range[-1] != end_date:
        date_range.append(end_date)
    return date_range


def collect_element_ids(
    date_range: list[datetime.datetime],
    bbox: dict,
    osm_keys: list[str],
    timeout: int = 1000,
    endpoint: str = "https://maps.mail.ru/osm/tools/overpass/api/interpreter",
) -> tuple[pd.DataFrame, list[datetime.datetime], list[datetime.datetime]]:
    """
    Queries Overpass for element IDs across a date range.

    Args:
        date_range: A list of dates to query.
        bbox: The bounding box to query for.
        osm_keys: The OSM keys to query for.
        timeout: The timeout for each query in seconds.
        endpoint: The Overpass API endpoint URL.

    Returns:
        A tuple containing:
        - A DataFrame with columns 'type' and 'id' for all discovered elements.
        - A list of dates that succeeded.
        - A list of dates that failed.
    """
    consider_ids: dict[str, set] = {
        "node": set(),
        "way": set(),
        "relation": set(),
    }

    api = overpass.API(timeout=timeout, endpoint=endpoint)
    failed_dates = []
    succeed_dates = []

    for this_date in date_range:
        try:
            start_time = time.time()
            for key in osm_keys:
                query_string = build_query_string(
                    date=this_date,
                    bbox=bbox,
                    keys=[key],
                    timeout=timeout,
                )
                result_xml = api.get(query=query_string, build=False)
                result_etree = ET.fromstring(result_xml)
                for e_type in consider_ids:
                    elements = result_etree.findall(f".//{e_type}")
                    for element in elements:
                        consider_ids[e_type].add(element.get("id"))
            print(
                f"Successfully queried date {this_date} in "
                f"{time.time() - start_time:.2f} seconds"
            )
            succeed_dates.append(this_date)
        except Exception:
            failed_dates.append(this_date)
            print(f"Failed to query date {this_date}; adding to failed_dates")
            time.sleep(1)

    elements_table = pd.concat(
        [
            pd.DataFrame({"type": e_type, "id": list(consider_ids[e_type])})
            for e_type in consider_ids
        ]
    )
    return elements_table, succeed_dates, failed_dates


# -----------------------------------------------------------------------------
# XML parsing helpers
# -----------------------------------------------------------------------------


def print_etree_structure(elem: ET.Element, indent: int = 0) -> None:
    """
    Prints the full structure of an ElementTree element for debugging.

    Args:
        elem: The ElementTree element to print.
        indent: The current indentation level.
    """
    print("  " * indent + f"<{elem.tag} {dict(elem.attrib)}>")
    for child in elem:
        print_etree_structure(child, indent + 1)


def process_version(
    version_etree: ET.Element,
) -> tuple[pd.DataFrame, set[tuple[str, str]]]:
    """
    Extracts metadata and tags from a single element version.

    Args:
        version_etree: An ElementTree element representing one version of an OSM
            element.

    Returns:
        A tuple containing:
        - A DataFrame with the version metadata (id, version, timestamp, etc.).
        - A set of (key, value) tuples for all tags in this version.
    """
    tag_keys = ["lat", "lon", "visible"]
    non_tag_df = pd.DataFrame(
        [
            {
                key: version_etree.get(key)
                for key in version_etree.attrib
                if key not in tag_keys
            }
        ]
    )
    non_tag_df["type"] = version_etree.tag
    # Get all k,v pairs for this version
    tag_tuples = [
        (key, version_etree.get(key))
        for key in version_etree.attrib
        if key in tag_keys
    ]
    for tag_item in version_etree.findall(".//tag"):
        tag_tuples.append((tag_item.get("k"), tag_item.get("v")))
    return non_tag_df, set(tag_tuples)


def compare_tags(
    v1: set[tuple[str, str]], v2: set[tuple[str, str]]
) -> pd.DataFrame:
    """
    Get all changes between two sets of key-value pairs.

    Args:
        v1: The set of (key, value) tuples from the previous version.
        v2: The set of (key, value) tuples from the current version.

    Returns:
        A DataFrame with columns 'key', 'value', and 'change' indicating
        whether each tag was 'Added', 'Deleted', or 'Changed'.
    """
    new_tuples = list(v2 - v1)
    removed_tuples = list(v1 - v2)
    new_df = pd.DataFrame(new_tuples, columns=["key", "value"])
    new_df["change"] = "Added"
    removed_df = pd.DataFrame(removed_tuples, columns=["key", "value"])
    removed_df["change"] = "Deleted"
    # Check for changed keys
    new_df.loc[new_df["key"].isin(removed_df["key"]), "change"] = "Changed"
    removed_df = removed_df.loc[~removed_df["key"].isin(new_df["key"]), :]
    all_changes_df = pd.concat([new_df, removed_df])
    return all_changes_df


def process_element(
    element_etree: ET.Element,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """
    Process an element and return all changes over multiple versions.

    Args:
        element_etree: An ElementTree element containing multiple version
            sub-elements for a single OSM element.

    Returns:
        A tuple containing:
        - A DataFrame with all version metadata concatenated.
        - A DataFrame with all tag changes across versions.
    """
    previous_tags: set[tuple[str, str]] = set()
    versions_list = []
    changes_list = []
    for version_etree in element_etree:
        non_tag_df, current_tags = process_version(version_etree)
        versions_list.append(non_tag_df)
        changes_df = compare_tags(previous_tags, current_tags)
        changes_df["id"] = non_tag_df["id"].iloc[0]
        changes_df["version"] = non_tag_df["version"].iloc[0]
        changes_list.append(changes_df)
        previous_tags = current_tags
    all_versions_df = pd.concat(versions_list) if versions_list else pd.DataFrame()
    all_changes_df = pd.concat(changes_list) if changes_list else pd.DataFrame()
    return all_versions_df, all_changes_df


def download_element_histories(
    elements_table: pd.DataFrame,
    timeout: int = 1000,
    progress: bool = False,
) -> tuple[pd.DataFrame, pd.DataFrame, list]:
    """
    Downloads and parses histories for a table of OSM elements.

    Args:
        elements_table: A DataFrame with columns 'type' and 'id' for each
            element to download history for.
        timeout: The timeout for each request in seconds.
        progress: Whether to print progress messages.
    Returns:
        A tuple containing:
        - A DataFrame with all version metadata for all elements.
        - A DataFrame with all tag changes for all elements.
        - A list of rows that failed to download.
    """
    versions_list = []
    changes_list = []
    failed_rows = []

    for idx, row in elements_table.iterrows():
        history_url = (
            f"https://api.openstreetmap.org/api/0.6/{row['type']}/{row['id']}/history"
        )
        try:
            history_response = requests.get(history_url, timeout=timeout)
        except Exception as e:
            if progress:
                print(f"Failed to get history for row {idx}: {e}")
            failed_rows.append(row)
            time.sleep(1)
            continue
        history_etree = ET.fromstring(history_response.text)
        versions_df, changes_df = process_element(history_etree)
        versions_list.append(versions_df)
        changes_list.append(changes_df)
        if progress and (idx % 100 == 0):
            print(f"Processed {idx} rows")
    if progress:
        print(f"Finished processing {len(elements_table)} rows")
    return (
        pd.concat(versions_list) if versions_list else pd.DataFrame(),
        pd.concat(changes_list) if changes_list else pd.DataFrame(),
        failed_rows,
    )
