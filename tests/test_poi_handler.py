#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
Unit tests for openpois.osm._poi_handler.POIRecordBuilder.

pyosmium objects are replaced by lightweight plain-Python stubs (no osmium
import required for most tests).  The only real osmium dependency is
``osmium.InvalidLocationError``, which is raised in process_way and
process_area error-path tests.
"""
from __future__ import annotations

import osmium
import pytest
from shapely.geometry import MultiPolygon, Point, Polygon

from openpois.osm._poi_handler import POIRecordBuilder


# ---------------------------------------------------------------------------
# Stub helpers — plain Python objects that mimic pyosmium's interface
# ---------------------------------------------------------------------------


class _Tag:
    """Minimal tag stub with .k and .v attributes."""

    def __init__(self, k: str, v: str) -> None:
        self.k = k
        self.v = v


class _TagList:
    """Minimal TagList stub backed by a plain dict."""

    def __init__(self, tag_dict: dict) -> None:
        self._d = tag_dict

    def __iter__(self):
        return (_Tag(k, v) for k, v in self._d.items())

    def __contains__(self, key):
        return key in self._d

    def get(self, key, default=None):
        return self._d.get(key, default)


class _Location:
    def __init__(self, lon: float, lat: float) -> None:
        self.lon = lon
        self.lat = lat


class _Node:
    def __init__(self, osm_id: int, lon: float, lat: float, tag_dict: dict) -> None:
        self.id = osm_id
        self.location = _Location(lon, lat)
        self.tags = _TagList(tag_dict)


class _WayNode:
    def __init__(self, lon: float, lat: float) -> None:
        self.lon = lon
        self.lat = lat


class _Way:
    def __init__(
        self,
        osm_id: int,
        coords: list[tuple[float, float]],
        tag_dict: dict,
    ) -> None:
        self.id = osm_id
        self.nodes = [_WayNode(lon, lat) for lon, lat in coords]
        self.tags = _TagList(tag_dict)


class _Ring:
    """Sequence of _WayNode-like objects representing one ring."""

    def __init__(self, coords: list[tuple[float, float]]) -> None:
        self._nodes = [_WayNode(lon, lat) for lon, lat in coords]

    def __iter__(self):
        return iter(self._nodes)


class _Area:
    """
    Stub for osmium.osm.Area.

    ``outer`` is a list of ring coordinate lists (each list of (lon, lat)).
    ``inners`` is a dict mapping outer-ring index → list of inner ring
    coordinate lists.  Areas derived from ways are flagged by setting
    ``is_from_way=True``.
    """

    def __init__(
        self,
        osm_id: int,
        tag_dict: dict,
        outer: list[list[tuple[float, float]]],
        inners: dict[int, list[list[tuple[float, float]]]] | None = None,
        is_from_way: bool = False,
    ) -> None:
        self._osm_id = osm_id
        self.tags = _TagList(tag_dict)
        self._outer = outer
        self._inners = inners or {}
        self._is_from_way = is_from_way

    def from_way(self) -> bool:
        return self._is_from_way

    def orig_id(self) -> int:
        return self._osm_id

    def outer_rings(self):
        return [_Ring(coords) for coords in self._outer]

    def inner_rings(self, outer_ring: _Ring) -> list[_Ring]:
        # Match by object identity via position in outer_rings list
        for idx, coords in enumerate(self._outer):
            if [n.lon for n in outer_ring] == [c[0] for c in coords]:
                return [_Ring(ic) for ic in self._inners.get(idx, [])]
        return []


# Convenience constructor for a simple rectangular ring (closed, 5 points)
def _rect_ring(
    lon_min: float,
    lat_min: float,
    lon_max: float,
    lat_max: float,
) -> list[tuple[float, float]]:
    return [
        (lon_min, lat_min),
        (lon_max, lat_min),
        (lon_max, lat_max),
        (lon_min, lat_max),
        (lon_min, lat_min),
    ]


# ---------------------------------------------------------------------------
# process_node
# ---------------------------------------------------------------------------


class TestProcessNode:
    def test_accepted_when_filter_key_present(self):
        """process_node returns a dict when element has a filter key."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        node = _Node(
            osm_id = 1,
            lon = -122.3,
            lat = 47.6,
            tag_dict = {"amenity": "restaurant", "name": "Good Eats"},
        )
        rec = builder.process_node(node)

        assert rec is not None
        assert rec["osm_id"] == 1
        assert rec["osm_type"] == "node"
        assert rec["source"] == "test"
        assert rec["name"] == "Good Eats"

    def test_geometry_is_point_with_correct_coordinates(self):
        """process_node produces a Point with (lon, lat) = (x, y)."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        node = _Node(
            osm_id = 2,
            lon = -122.3,
            lat = 47.6,
            tag_dict = {"amenity": "cafe"},
        )
        rec = builder.process_node(node)

        assert isinstance(rec["geometry"], Point)
        assert rec["geometry"].x == pytest.approx(-122.3)
        assert rec["geometry"].y == pytest.approx(47.6)

    def test_rejected_when_no_filter_key_present(self):
        """process_node returns None when element lacks all filter keys."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        node = _Node(
            osm_id = 3,
            lon = 0.0,
            lat = 0.0,
            tag_dict = {"highway": "crossing"},
        )
        assert builder.process_node(node) is None

    def test_accepted_when_filter_keys_is_none(self):
        """filter_keys=None means every element is accepted."""
        builder = POIRecordBuilder(source_label = "test", filter_keys = None)
        node = _Node(
            osm_id = 4,
            lon = 1.0,
            lat = 2.0,
            tag_dict = {"highway": "crossing"},
        )
        rec = builder.process_node(node)
        assert rec is not None

    def test_accepted_when_one_of_multiple_filter_keys_present(self):
        """Element is accepted if it has any one of the filter keys."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity", "shop", "tourism"],
        )
        node = _Node(
            osm_id = 5,
            lon = 0.0,
            lat = 0.0,
            tag_dict = {"shop": "bakery"},
        )
        assert builder.process_node(node) is not None


# ---------------------------------------------------------------------------
# process_node — extract_keys behaviour
# ---------------------------------------------------------------------------


class TestProcessNodeExtractKeys:
    def test_extract_keys_none_returns_all_tags(self):
        """extract_keys=None includes every tag on the element."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
            extract_keys = None,
        )
        node = _Node(
            osm_id = 10,
            lon = 0.0,
            lat = 0.0,
            tag_dict = {"amenity": "pub", "cuisine": "italian", "name": "Trattoria"},
        )
        rec = builder.process_node(node)

        assert rec["amenity"] == "pub"
        assert rec["cuisine"] == "italian"

    def test_extract_keys_set_returns_only_those_columns(self):
        """extract_keys restricts output columns to the specified set."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
            extract_keys = ["amenity"],
        )
        node = _Node(
            osm_id = 11,
            lon = 0.0,
            lat = 0.0,
            tag_dict = {"amenity": "pub", "cuisine": "italian"},
        )
        rec = builder.process_node(node)

        assert rec["amenity"] == "pub"
        assert "cuisine" not in rec

    def test_extract_keys_missing_tag_gets_none(self):
        """A key in extract_keys that is absent on the element maps to None."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
            extract_keys = ["amenity", "shop", "tourism"],
        )
        node = _Node(
            osm_id = 12,
            lon = 0.0,
            lat = 0.0,
            tag_dict = {"amenity": "cafe"},
        )
        rec = builder.process_node(node)

        assert rec["amenity"] == "cafe"
        assert rec["shop"] is None
        assert rec["tourism"] is None

    def test_filter_and_extract_keys_are_independent(self):
        """Can filter on amenity but extract a disjoint set of keys."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
            extract_keys = ["name", "cuisine"],
        )
        node = _Node(
            osm_id = 13,
            lon = 0.0,
            lat = 0.0,
            tag_dict = {"amenity": "restaurant", "cuisine": "thai", "name": "Lotus"},
        )
        rec = builder.process_node(node)

        # amenity used to decide acceptance but NOT in extract_keys → absent
        assert "amenity" not in rec
        assert rec["cuisine"] == "thai"
        assert rec["name"] == "Lotus"


# ---------------------------------------------------------------------------
# process_way
# ---------------------------------------------------------------------------


class TestProcessWay:
    def test_closed_way_produces_polygon(self):
        """A closed ring with ≥ 4 coords becomes a Polygon geometry."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        coords = _rect_ring(-122.1, 47.0, -122.0, 47.1)
        way = _Way(osm_id = 100, coords = coords, tag_dict = {"amenity": "school"})

        rec = builder.process_way(way)

        assert rec is not None
        assert rec["osm_type"] == "way"
        assert isinstance(rec["geometry"], Polygon)

    def test_open_way_produces_centroid_point(self):
        """An open way (first != last) becomes a centroid Point."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        coords = [(-122.0, 47.0), (-121.9, 47.1), (-121.8, 47.2)]
        way = _Way(osm_id = 101, coords = coords, tag_dict = {"amenity": "parking"})

        rec = builder.process_way(way)

        assert rec is not None
        assert isinstance(rec["geometry"], Point)

    def test_rejected_without_filter_key(self):
        """process_way returns None when no filter key is present."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        coords = [(-122.0, 47.0), (-121.9, 47.1)]
        way = _Way(osm_id = 102, coords = coords, tag_dict = {"highway": "path"})

        assert builder.process_way(way) is None

    def test_fewer_than_two_coords_returns_none(self):
        """Ways with a single coordinate are skipped."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        way = _Way(
            osm_id = 103,
            coords = [(-122.0, 47.0)],
            tag_dict = {"amenity": "bench"},
        )
        assert builder.process_way(way) is None

    def test_invalid_location_error_returns_none(self):
        """process_way swallows InvalidLocationError and returns None."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )

        class _BadWay:
            id = 104
            tags = _TagList({"amenity": "cafe"})

            @property
            def nodes(self):
                raise osmium.InvalidLocationError("no location")

        assert builder.process_way(_BadWay()) is None

    def test_closed_way_with_only_three_coords_becomes_open_way(self):
        """
        A 'closed' ring that has fewer than 4 coords (e.g., 3 identical points)
        doesn't satisfy the Polygon threshold, so the first==last check passes
        only if len(coords) >= 4.  A 3-coord ring is processed as an open way
        (centroid Point) because the closed condition requires len >= 4.
        """
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        # 3 coords where first == last → open-way path (len < 4)
        coords = [(-122.0, 47.0), (-121.9, 47.1), (-122.0, 47.0)]
        way = _Way(osm_id = 105, coords = coords, tag_dict = {"amenity": "cafe"})

        rec = builder.process_way(way)

        assert rec is not None
        assert isinstance(rec["geometry"], Point)

    def test_filter_keys_none_accepts_any_way(self):
        """filter_keys=None accepts ways regardless of their tags."""
        builder = POIRecordBuilder(source_label = "test", filter_keys = None)
        coords = [(-122.0, 47.0), (-121.9, 47.1)]
        way = _Way(osm_id = 106, coords = coords, tag_dict = {"highway": "path"})

        rec = builder.process_way(way)

        assert rec is not None


# ---------------------------------------------------------------------------
# process_area
# ---------------------------------------------------------------------------


class TestProcessArea:
    def _simple_area(
        self,
        osm_id: int = 200,
        tag_dict: dict | None = None,
        is_from_way: bool = False,
    ) -> _Area:
        """Return an area with one rectangular outer ring."""
        return _Area(
            osm_id = osm_id,
            tag_dict = tag_dict or {"amenity": "park"},
            outer = [_rect_ring(-122.1, 47.0, -122.0, 47.1)],
            is_from_way = is_from_way,
        )

    def test_from_way_area_returns_none(self):
        """process_area skips areas derived from closed ways."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        area = self._simple_area(is_from_way = True)
        assert builder.process_area(area) is None

    def test_single_outer_ring_produces_polygon(self):
        """A relation area with one outer ring becomes a Polygon."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        area = self._simple_area()
        rec = builder.process_area(area)

        assert rec is not None
        assert isinstance(rec["geometry"], Polygon)
        assert rec["osm_type"] == "relation"
        assert rec["osm_id"] == 200

    def test_multiple_outer_rings_produce_multipolygon(self):
        """A relation area with two outer rings becomes a MultiPolygon."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        area = _Area(
            osm_id = 201,
            tag_dict = {"amenity": "park"},
            outer = [
                _rect_ring(-122.1, 47.0, -122.0, 47.1),
                _rect_ring(-121.9, 47.0, -121.8, 47.1),
            ],
        )
        rec = builder.process_area(area)

        assert rec is not None
        assert isinstance(rec["geometry"], MultiPolygon)

    def test_rejected_without_filter_key(self):
        """process_area returns None when no filter key matches."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        area = _Area(
            osm_id = 202,
            tag_dict = {"natural": "water"},
            outer = [_rect_ring(-122.1, 47.0, -122.0, 47.1)],
        )
        assert builder.process_area(area) is None

    def test_filter_keys_none_accepts_any_area(self):
        """filter_keys=None accepts any relation area."""
        builder = POIRecordBuilder(source_label = "test", filter_keys = None)
        area = _Area(
            osm_id = 203,
            tag_dict = {"natural": "water"},
            outer = [_rect_ring(-122.1, 47.0, -122.0, 47.1)],
        )
        rec = builder.process_area(area)
        assert rec is not None

    def test_outer_ring_with_fewer_than_four_coords_skipped(self):
        """Outer rings with < 4 coords are not turned into polygons."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        area = _Area(
            osm_id = 204,
            tag_dict = {"amenity": "park"},
            # Only 3 coords — too few for a valid polygon
            outer = [[(-122.0, 47.0), (-121.9, 47.0), (-122.0, 47.0)]],
        )
        # No valid polygons → returns None
        assert builder.process_area(area) is None

    def test_invalid_location_error_returns_none(self):
        """process_area swallows InvalidLocationError and returns None."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )

        class _BadArea:
            tags = _TagList({"amenity": "park"})

            def from_way(self):
                return False

            def orig_id(self):
                return 205

            def outer_rings(self):
                raise osmium.InvalidLocationError("no loc")

        assert builder.process_area(_BadArea()) is None

    def test_area_with_inner_ring_produces_polygon_with_hole(self):
        """A polygon with an inner ring should be a non-empty Polygon."""
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
        )
        area = _Area(
            osm_id = 206,
            tag_dict = {"amenity": "park"},
            outer = [_rect_ring(-123.0, 46.0, -120.0, 49.0)],
            inners = {0: [_rect_ring(-122.5, 46.5, -120.5, 48.5)]},
        )
        rec = builder.process_area(area)

        assert rec is not None
        assert isinstance(rec["geometry"], Polygon)
        # A polygon with a hole has a non-zero area but an interior ring
        assert len(rec["geometry"].interiors) == 1

    def test_extract_keys_applied_to_area(self):
        """
        extract_keys restricts the tag columns appended via _extract_tags.

        The fixed base fields (source, osm_id, osm_type, name, geometry) are
        always present regardless of extract_keys.  Only the tag columns added
        by _extract_tags are filtered: here extract_keys=["amenity"] means
        'cuisine' is absent but 'amenity' is present.  'name' is part of the
        fixed base, so it is always included.
        """
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
            extract_keys = ["amenity"],
        )
        area = _Area(
            osm_id = 207,
            tag_dict = {"amenity": "park", "name": "Central Park", "cuisine": "n/a"},
            outer = [_rect_ring(-122.1, 47.0, -122.0, 47.1)],
        )
        rec = builder.process_area(area)

        # amenity is in extract_keys → present
        assert rec["amenity"] == "park"
        # cuisine is not in extract_keys → absent from tag columns
        assert "cuisine" not in rec
        # name is a fixed base field set before _extract_tags → always present
        assert rec["name"] == "Central Park"

    def test_area_within_node_limit_is_kept(self):
        """Areas under max_area_nodes are returned normally."""
        # _rect_ring produces 5 nodes; limit of 10 should pass.
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
            max_area_nodes = 10,
        )
        area = self._simple_area()
        assert builder.process_area(area) is not None

    def test_area_exceeding_node_limit_is_skipped(self):
        """Areas exceeding max_area_nodes are skipped before geometry is built."""
        # _rect_ring produces 5 nodes; two outer rings = 10 nodes, limit of 9.
        builder = POIRecordBuilder(
            source_label = "test",
            filter_keys = ["amenity"],
            max_area_nodes = 9,
        )
        area = _Area(
            osm_id = 209,
            tag_dict = {"amenity": "park"},
            outer = [
                _rect_ring(-122.1, 47.0, -122.0, 47.1),
                _rect_ring(-121.9, 47.0, -121.8, 47.1),
            ],
        )
        assert builder.process_area(area) is None


# ---------------------------------------------------------------------------
# source_label propagation
# ---------------------------------------------------------------------------


class TestSourceLabel:
    def test_source_label_in_node_record(self):
        builder = POIRecordBuilder(
            source_label = "geofabrik_us",
            filter_keys = None,
        )
        node = _Node(
            osm_id = 1,
            lon = 0.0,
            lat = 0.0,
            tag_dict = {"amenity": "cafe"},
        )
        rec = builder.process_node(node)
        assert rec["source"] == "geofabrik_us"

    def test_source_label_in_way_record(self):
        builder = POIRecordBuilder(
            source_label = "geofabrik_us",
            filter_keys = None,
        )
        coords = [(-122.0, 47.0), (-121.9, 47.1)]
        way = _Way(osm_id = 2, coords = coords, tag_dict = {"amenity": "cafe"})
        rec = builder.process_way(way)
        assert rec["source"] == "geofabrik_us"

    def test_source_label_in_area_record(self):
        builder = POIRecordBuilder(
            source_label = "geofabrik_us",
            filter_keys = None,
        )
        area = _Area(
            osm_id = 3,
            tag_dict = {"amenity": "park"},
            outer = [_rect_ring(-122.1, 47.0, -122.0, 47.1)],
        )
        rec = builder.process_area(area)
        assert rec["source"] == "geofabrik_us"
