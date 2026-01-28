import overpass
import xml.etree.ElementTree as ET
import requests
import datetime
import time
import pandas as pd


TIMEOUT = 1000
BBOX = {'ymin': 47.41, 'xmin': -122.48, 'ymax': 47.79, 'xmax': -122.16}
START_DATE = datetime.datetime(2016, 1, 1) # Earliest option is September 13, 2012
END_DATE = datetime.datetime(2026, 1, 1) # Latest
DATE_INTERVAL = datetime.timedelta(days=7)
OSM_KEYS = ['amenity', 'shop', 'healthcare', 'leisure']

# Create date range
date_range = [
    START_DATE + i * DATE_INTERVAL
    for i in range(((END_DATE - START_DATE) // DATE_INTERVAL) + 1)
]
if date_range[-1] != END_DATE:
    date_range.append(END_DATE)


def build_query_string(
        date: datetime.datetime,
        bbox: dict,
        keys: list,
        timeout: int
    ) -> str:
    """
    Builds a query string for the given date, bbox, kvs, and timeout.

    Args:
        date: The date to query for.
        bbox: The bounding box to query for.
        amenities: The amenities to query for.
        timeout: The timeout for the query.
    Returns:
        A query string.
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

consider_ids = {
    'node': set(),
    'way': set(),
    'relation': set(),
}

# Build query string

api = overpass.API(
    timeout = TIMEOUT,
    endpoint = "https://maps.mail.ru/osm/tools/overpass/api/interpreter"
)
failed_dates = []
succeed_dates = []

for this_date in date_range:
    try:
        start_time = time.time()
        for key in OSM_KEYS:
            # Query all matching elements from this date
            query_string = build_query_string(
                date = this_date,
                bbox = BBOX,
                keys = [key],
                timeout = TIMEOUT
            )
            result_xml = api.get(query = query_string, build = False)
            # Get all IDs for matching elements; add them to the consider_ids sets
            result_etree = ET.fromstring(result_xml)
            for e_type in consider_ids:
                elements = result_etree.findall(f'.//{e_type}')
                for element in elements:
                    consider_ids[e_type].add(element.get('id'))
        print(
            f"Successfully queried date {this_date} in {time.time() - start_time} seconds"
        )
        succeed_dates.append(this_date)
    except Exception as e:
        failed_dates.append(this_date)
        print(f"Failed to query date {this_date}; adding to failed_dates")
        time.sleep(1)


# Save elements
elements_table = pd.concat([
    pd.DataFrame({'type': e_type, 'id': list(consider_ids[e_type])})
    for e_type in consider_ids
])
elements_table.to_csv(
    '~/data/openpois/osm_elements.csv',
    index = False
)

# Print the full structure of the Etree
def print_etree_structure(elem, indent=0):
    print('  ' * indent + f"<{elem.tag} {dict(elem.attrib)}>")
    for child in elem:
        print_etree_structure(child, indent + 1)

def process_version(version_etree: ET.ElementTree) -> tuple[pd.DataFrame, set[tuple[str, str]]]:
    # Extract version metadata
    tag_keys = ['lat', 'lon', 'visible']
    non_tag_df = pd.DataFrame(
        [{key: version_etree.get(key) for key in version_etree.attrib if key not in tag_keys}]
    )
    non_tag_df['type'] = version_etree.tag
    # Get all k,v pairs for this version
    tag_tuples = [
        (key, version_etree.get(key))
        for key in version_etree.attrib
        if key in tag_keys
    ]
    for tag_item in version_etree.findall('.//tag'):
        tag_tuples.append((tag_item.get('k'), tag_item.get('v')))
    return non_tag_df, set(tag_tuples)

def compare_tags(v1: set[tuple[str, str]], v2: set[tuple[str, str]]) -> pd.DataFrame:
    """Get all changes between two sets of key-value pairs."""
    new_tuples = list(v2 - v1)
    removed_tuples = list(v1 - v2)
    new_df = pd.DataFrame(new_tuples, columns = ['key', 'value'])
    new_df['change'] = 'Added'
    removed_df = pd.DataFrame(removed_tuples, columns = ['key', 'value'])
    removed_df['change'] = 'Deleted'
    # Check for changed keys
    new_df.loc[new_df['key'].isin(removed_df['key']), 'change'] = 'Changed'
    removed_df = removed_df.loc[~removed_df['key'].isin(new_df['key']), :]
    all_changes_df = pd.concat([new_df, removed_df])
    return all_changes_df

def process_element(element_etree: ET.ElementTree) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Process an element and return all changes over multiple versions."""
    previous_tags = set()
    versions_list = []
    changes_list = []
    for version_etree in element_etree:
        non_tag_df, current_tags = process_version(version_etree)
        versions_list.append(non_tag_df)
        changes_df = compare_tags(previous_tags, current_tags)
        changes_df['id'] = non_tag_df['id'].iloc[0]
        changes_df['version'] = non_tag_df['version'].iloc[0]
        changes_list.append(changes_df)
        previous_tags = current_tags
    return pd.concat(versions_list), pd.concat(changes_list)


versions_list = []
changes_list = []

for idx, row in elements_table.iterrows():
    print(f"   Row {idx}: type={row['type']}, id={row['id']}")
    history_url = f"https://api.openstreetmap.org/api/0.6/{row['type']}/{row['id']}/history"
    history_response = requests.get(history_url, timeout = TIMEOUT)
    history_etree = ET.fromstring(history_response.text)
    versions_df, changes_df = process_element(history_etree)
    versions_list.append(versions_df)
    changes_list.append(changes_df)
    if idx % 100 == 0:
        print(f"Processed {idx} rows")
        pd.concat(versions_list).to_csv('osm_versions.csv', index = False)
        pd.concat(changes_list).to_csv('osm_changes.csv', index = False)
