import overpass
import xml.etree.ElementTree as ET
import requests
import datetime
import time

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

api = overpass.API(timeout = TIMEOUT)
failed_dates = []

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
    except Exception as e:
        failed_dates.append(this_date)
        print(f"Failed to query date {this_date}; adding to failed_dates")
        time.sleep(10)


# # Get the changeset for each element
# element_id = consider_ids.pop()
# history_url = f"https://api.openstreetmap.org/api/0.6/node/{element_id}/history"
# history_response = requests.get(history_url)
# history_etree = ET.fromstring(history_response.text)
