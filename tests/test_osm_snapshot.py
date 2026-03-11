#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
Unit tests for openpois.osm.snapshot.

All external I/O (requests.get, subprocess.run, osmium handler.apply_file)
is mocked so tests run in milliseconds without network or filesystem access.
"""
from __future__ import annotations

from unittest.mock import MagicMock, patch

import osmium
import pytest
from shapely.geometry import Point, Polygon

from openpois.osm.snapshot import (
    _POIHandler,
    download_pbf,
    filter_pbf,
    parse_pbf_to_geodataframe,
)


# ---------------------------------------------------------------------------
# Helpers: fake osmium objects
# ---------------------------------------------------------------------------


def _make_tags(tag_dict: dict) -> MagicMock:
    """Return a mock osmium TagList from a plain dict."""
    tags = MagicMock()
    tags.__iter__ = MagicMock(return_value=iter(tag_dict.keys()))
    tags.__contains__ = MagicMock(side_effect=lambda k: k in tag_dict)
    tags.get = MagicMock(side_effect=lambda k, default=None: tag_dict.get(k, default))
    return tags


def _make_node(osm_id: int, lon: float, lat: float, tag_dict: dict) -> MagicMock:
    node = MagicMock()
    node.id = osm_id
    node.location.lon = lon
    node.location.lat = lat
    node.tags = _make_tags(tag_dict)
    return node


def _make_way_nodes(coords: list[tuple[float, float]]) -> list[MagicMock]:
    way_nodes = []
    for lon, lat in coords:
        nd = MagicMock()
        nd.lon = lon
        nd.lat = lat
        way_nodes.append(nd)
    return way_nodes


def _make_way(
    osm_id: int, coords: list[tuple[float, float]], tag_dict: dict
) -> MagicMock:
    way = MagicMock()
    way.id = osm_id
    way.nodes = _make_way_nodes(coords)
    way.tags = _make_tags(tag_dict)
    return way


# ---------------------------------------------------------------------------
# download_pbf
# ---------------------------------------------------------------------------


class TestDownloadPbf:
    def test_skips_if_exists_and_no_overwrite(self, tmp_path):
        """Should return early without calling requests.get when file exists."""
        output = tmp_path / "out.pbf"
        output.write_bytes(b"fake")

        with patch("openpois.osm.snapshot.requests.get") as mock_get:
            result = download_pbf("http://example.com/file.pbf", output, overwrite=False)

        mock_get.assert_not_called()
        assert result == output

    def test_downloads_when_no_file(self, tmp_path):
        """Should call requests.get with stream=True and write content."""
        output = tmp_path / "subdir" / "out.pbf"

        mock_resp = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.headers = {"content-length": "10"}
        mock_resp.iter_content = MagicMock(return_value=[b"hellworld!"])

        with patch(
            "openpois.osm.snapshot.requests.get", return_value=mock_resp
        ) as mock_get:
            result = download_pbf(
                "http://example.com/file.pbf", output, overwrite=False
            )

        mock_get.assert_called_once_with(
            "http://example.com/file.pbf", stream=True, timeout=(30, None)
        )
        assert result == output
        assert output.exists()

    def test_overwrites_existing_file(self, tmp_path):
        """Should call requests.get even if file already exists when overwrite=True."""
        output = tmp_path / "out.pbf"
        output.write_bytes(b"old content")

        mock_resp = MagicMock()
        mock_resp.__enter__ = MagicMock(return_value=mock_resp)
        mock_resp.__exit__ = MagicMock(return_value=False)
        mock_resp.headers = {}
        mock_resp.iter_content = MagicMock(return_value=[b"new"])

        with patch(
            "openpois.osm.snapshot.requests.get", return_value=mock_resp
        ) as mock_get:
            download_pbf("http://example.com/file.pbf", output, overwrite=True)

        mock_get.assert_called_once()


# ---------------------------------------------------------------------------
# filter_pbf
# ---------------------------------------------------------------------------


class TestFilterPbf:
    def test_skips_if_output_exists_and_no_overwrite(self, tmp_path):
        """Should return early without calling subprocess.run when output exists."""
        input_pbf = tmp_path / "in.pbf"
        output_pbf = tmp_path / "out.pbf"
        input_pbf.write_bytes(b"fake")
        output_pbf.write_bytes(b"fake")

        with patch("openpois.osm.snapshot.subprocess.run") as mock_run:
            result = filter_pbf(input_pbf, output_pbf, ["amenity"], overwrite=False)

        mock_run.assert_not_called()
        assert result == output_pbf

    def test_runs_osmium_command_with_correct_args(self, tmp_path):
        """Should construct osmium tags-filter command with nw/ prefixed keys."""
        input_pbf = tmp_path / "in.pbf"
        output_pbf = tmp_path / "out.pbf"
        input_pbf.write_bytes(b"fake")
        osm_keys = ["amenity", "shop", "tourism"]

        with patch("openpois.osm.snapshot.subprocess.run") as mock_run, \
             patch("openpois.osm.snapshot.shutil.which", return_value="/usr/bin/osmium"):
            filter_pbf(input_pbf, output_pbf, osm_keys, overwrite=False)

        mock_run.assert_called_once()
        cmd = mock_run.call_args[0][0]
        # First element is the osmium binary
        assert cmd[1] == "tags-filter"
        assert "-o" in cmd
        # Each key should appear as nw/{key}
        for key in osm_keys:
            assert f"nw/{key}" in cmd
        # check=True must be passed
        assert mock_run.call_args[1].get("check") is True

    def test_overwrites_when_flag_set(self, tmp_path):
        """Should call subprocess.run even if output exists when overwrite=True."""
        input_pbf = tmp_path / "in.pbf"
        output_pbf = tmp_path / "out.pbf"
        input_pbf.write_bytes(b"fake")
        output_pbf.write_bytes(b"existing")

        with patch("openpois.osm.snapshot.subprocess.run") as mock_run, \
             patch("openpois.osm.snapshot.shutil.which", return_value="/usr/bin/osmium"):
            filter_pbf(input_pbf, output_pbf, ["amenity"], overwrite=True)

        mock_run.assert_called_once()


# ---------------------------------------------------------------------------
# _POIHandler.node
# ---------------------------------------------------------------------------


class TestPOIHandlerNode:
    def test_appends_record_when_tag_matches(self):
        """node() should add a record with required keys when tag key matches."""
        handler = _POIHandler(osm_keys=["amenity", "shop"], source_label="osm_test")
        node = _make_node(
            osm_id=42,
            lon=-122.3,
            lat=47.6,
            tag_dict={"amenity": "restaurant", "name": "Good Eats"},
        )
        handler.node(node)

        assert len(handler.records) == 1
        rec = handler.records[0]
        assert rec["osm_id"] == 42
        assert rec["osm_type"] == "node"
        assert rec["source"] == "osm_test"
        assert rec["amenity"] == "restaurant"
        assert rec["shop"] is None  # key present but no value
        assert rec["name"] == "Good Eats"
        assert isinstance(rec["geometry"], Point)
        assert rec["geometry"].x == pytest.approx(-122.3)
        assert rec["geometry"].y == pytest.approx(47.6)

    def test_skips_when_no_matching_tag(self):
        """node() should not append a record when no target key is present."""
        handler = _POIHandler(osm_keys=["amenity"], source_label="osm")
        node = _make_node(
            osm_id=99,
            lon=0.0,
            lat=0.0,
            tag_dict={"highway": "crossing"},
        )
        handler.node(node)
        assert handler.records == []


# ---------------------------------------------------------------------------
# _POIHandler.way
# ---------------------------------------------------------------------------


class TestPOIHandlerWay:
    def test_closed_way_produces_polygon(self):
        """A closed way with >= 4 coords should produce a Polygon geometry."""
        handler = _POIHandler(osm_keys=["amenity"], source_label="osm")
        # Closed ring: first == last, len >= 4
        coords = [(-122.0, 47.0), (-121.9, 47.0), (-121.9, 47.1), (-122.0, 47.0)]
        way = _make_way(osm_id=10, coords=coords, tag_dict={"amenity": "school"})
        handler.way(way)

        assert len(handler.records) == 1
        assert isinstance(handler.records[0]["geometry"], Polygon)
        assert handler.records[0]["osm_type"] == "way"

    def test_open_way_produces_centroid_point(self):
        """An open way should produce a Point (centroid of LineString)."""
        handler = _POIHandler(osm_keys=["amenity"], source_label="osm")
        # Open way: first != last
        coords = [(-122.0, 47.0), (-121.9, 47.1), (-121.8, 47.2)]
        way = _make_way(osm_id=20, coords=coords, tag_dict={"amenity": "parking"})
        handler.way(way)

        assert len(handler.records) == 1
        assert isinstance(handler.records[0]["geometry"], Point)

    def test_invalid_location_error_is_silently_skipped(self):
        """way() should swallow InvalidLocationError and append nothing."""
        handler = _POIHandler(osm_keys=["amenity"], source_label="osm")
        way = MagicMock()
        way.id = 30
        way.tags = _make_tags({"amenity": "cafe"})

        # Iterating w.nodes raises InvalidLocationError
        def _raising_iter():
            raise osmium.InvalidLocationError("no location")

        way.nodes.__iter__ = MagicMock(side_effect=_raising_iter)

        handler.way(way)
        assert handler.records == []

    def test_skips_when_no_matching_tag(self):
        """way() should not append a record when no target key is present."""
        handler = _POIHandler(osm_keys=["amenity"], source_label="osm")
        coords = [(-122.0, 47.0), (-121.9, 47.1)]
        way = _make_way(osm_id=40, coords=coords, tag_dict={"highway": "path"})
        handler.way(way)
        assert handler.records == []

    def test_way_with_fewer_than_two_coords_skipped(self):
        """way() should skip ways with fewer than 2 coordinates."""
        handler = _POIHandler(osm_keys=["amenity"], source_label="osm")
        coords = [(-122.0, 47.0)]
        way = _make_way(osm_id=50, coords=coords, tag_dict={"amenity": "bench"})
        handler.way(way)
        assert handler.records == []


# ---------------------------------------------------------------------------
# parse_pbf_to_geodataframe
# ---------------------------------------------------------------------------


class TestParsePbfToGeoDataFrame:
    def test_returns_empty_geodataframe_with_correct_schema_when_no_records(
        self, tmp_path
    ):
        """Should return an empty GeoDataFrame with all expected columns."""
        pbf_path = tmp_path / "empty.pbf"
        pbf_path.write_bytes(b"fake")
        osm_keys = ["amenity", "shop"]

        with patch(
            "openpois.osm.snapshot._POIHandler.apply_file"
        ):  # do nothing on apply_file
            gdf = parse_pbf_to_geodataframe(pbf_path, osm_keys=osm_keys)

        assert len(gdf) == 0
        assert gdf.crs.to_epsg() == 4326
        for col in ["source", "osm_id", "osm_type", "name", "geometry"] + osm_keys:
            assert col in gdf.columns

    def test_returns_geodataframe_from_handler_records(self, tmp_path):
        """Should build a GeoDataFrame from records collected by the handler."""
        pbf_path = tmp_path / "pois.pbf"
        pbf_path.write_bytes(b"fake")
        osm_keys = ["amenity"]

        injected_records = [
            {
                "source": "osm",
                "osm_id": 1,
                "osm_type": "node",
                "name": "A Cafe",
                "geometry": Point(-122.0, 47.0),
                "amenity": "cafe",
            }
        ]

        def _fake_apply_file(handler_self, *args, **kwargs):
            handler_self.records.extend(injected_records)

        with patch.object(_POIHandler, "apply_file", _fake_apply_file):
            gdf = parse_pbf_to_geodataframe(
                pbf_path, osm_keys=osm_keys, source_label="osm"
            )

        assert len(gdf) == 1
        assert gdf.iloc[0]["amenity"] == "cafe"
        assert gdf.crs.to_epsg() == 4326
