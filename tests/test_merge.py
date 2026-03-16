"""Tests for openpois.conflation.merge."""
from __future__ import annotations

import geopandas as gpd
import numpy as np
import pandas as pd
import pytest
from shapely.geometry import MultiPolygon, Point, Polygon

from openpois.conflation.merge import merge_matched_pois


@pytest.fixture
def osm_gdf():
    return gpd.GeoDataFrame(
        {
            "osm_id": [100, 200, 300],
            "name": ["Coffee Shop", "Grocery Store", "Park"],
            "brand": ["Starbucks", None, None],
            "conf_mean": [0.8, 0.6, 0.9],
            "conf_lower": [0.7, 0.5, 0.85],
            "conf_upper": [0.9, 0.7, 0.95],
        },
        geometry = [
            Point(-122.335, 47.608),
            Point(-122.340, 47.610),
            Polygon([
                (-122.33, 47.60), (-122.33, 47.61),
                (-122.32, 47.61), (-122.32, 47.60),
                (-122.33, 47.60),
            ]),
        ],
        crs = "EPSG:4326",
    )


@pytest.fixture
def overture_gdf():
    return gpd.GeoDataFrame(
        {
            "overture_id": ["ov_a", "ov_b", "ov_c"],
            "overture_name": [
                "Starbucks Coffee", "Fresh Market", "City Park",
            ],
            "brand_name": ["Starbucks", None, None],
            "confidence": [0.95, 0.7, 0.8],
        },
        geometry = [
            Point(-122.3351, 47.6081),
            Point(-122.350, 47.620),
            Point(-122.325, 47.605),
        ],
        crs = "EPSG:4326",
    )


@pytest.fixture
def matches():
    return pd.DataFrame(
        {
            "osm_idx": [0],
            "overture_idx": [0],
            "distance_m": [15.0],
            "composite_score": [0.85],
        }
    )


class TestMergeMatchedPois:
    def test_output_has_all_sources(
        self, osm_gdf, overture_gdf, matches,
    ):
        osm_labels = np.array(
            ["Cafe", "Supermarket", "Park"]
        )
        ov_labels = np.array(["Cafe", "Supermarket", "Park"])
        result = merge_matched_pois(
            osm_gdf, overture_gdf, matches,
            osm_labels, ov_labels,
            overture_confidence_weight = 0.7,
        )
        sources = set(result["source"].unique())
        assert sources == {"matched", "osm", "overture"}

    def test_total_count(
        self, osm_gdf, overture_gdf, matches,
    ):
        osm_labels = np.array(
            ["Cafe", "Supermarket", "Park"]
        )
        ov_labels = np.array(["Cafe", "Supermarket", "Park"])
        result = merge_matched_pois(
            osm_gdf, overture_gdf, matches,
            osm_labels, ov_labels,
        )
        # 1 matched + 2 unmatched OSM + 2 unmatched Overture = 5
        assert len(result) == 5

    def test_confidence_blending(
        self, osm_gdf, overture_gdf, matches,
    ):
        osm_labels = np.array(
            ["Cafe", "Supermarket", "Park"]
        )
        ov_labels = np.array(["Cafe", "Supermarket", "Park"])
        w = 0.7
        result = merge_matched_pois(
            osm_gdf, overture_gdf, matches,
            osm_labels, ov_labels,
            overture_confidence_weight = w,
        )
        matched_row = result[
            result["source"] == "matched"
        ].iloc[0]
        osm_conf = 0.8
        ov_conf = 0.95
        expected = osm_conf / (1 + w) + ov_conf * w / (1 + w)
        assert matched_row["conf_mean"] == pytest.approx(
            expected, abs = 0.01
        )

    def test_unmatched_overture_downweighted(
        self, osm_gdf, overture_gdf, matches,
    ):
        osm_labels = np.array(
            ["Cafe", "Supermarket", "Park"]
        )
        ov_labels = np.array(["Cafe", "Supermarket", "Park"])
        w = 0.7
        result = merge_matched_pois(
            osm_gdf, overture_gdf, matches,
            osm_labels, ov_labels,
            overture_confidence_weight = w,
        )
        unmatched_ov = result[result["source"] == "overture"]
        for _, row in unmatched_ov.iterrows():
            assert row["conf_mean"] == pytest.approx(
                row["overture_confidence"] * w, abs = 0.01
            )

    def test_unmatched_osm_keeps_confidence(
        self, osm_gdf, overture_gdf, matches,
    ):
        osm_labels = np.array(
            ["Cafe", "Supermarket", "Park"]
        )
        ov_labels = np.array(["Cafe", "Supermarket", "Park"])
        result = merge_matched_pois(
            osm_gdf, overture_gdf, matches,
            osm_labels, ov_labels,
        )
        unmatched_osm = result[result["source"] == "osm"]
        # OSM ids 200 and 300 should be unmatched
        for _, row in unmatched_osm.iterrows():
            assert row["conf_mean"] == row["osm_conf_mean"]

    def test_geometry_preference_polygon_over_point(self):
        """When OSM has a Polygon and Overture has a Point,
        the merged geometry should be the Polygon."""
        poly = Polygon([
            (-122.33, 47.60), (-122.33, 47.61),
            (-122.32, 47.61), (-122.32, 47.60),
            (-122.33, 47.60),
        ])
        osm = gpd.GeoDataFrame(
            {
                "osm_id": [1],
                "name": ["Park"],
                "brand": [None],
                "conf_mean": [0.9],
                "conf_lower": [0.85],
                "conf_upper": [0.95],
            },
            geometry = [poly],
            crs = "EPSG:4326",
        )
        overture = gpd.GeoDataFrame(
            {
                "overture_id": ["ov_1"],
                "overture_name": ["City Park"],
                "brand_name": [None],
                "confidence": [0.8],
            },
            geometry = [Point(-122.325, 47.605)],
            crs = "EPSG:4326",
        )
        m = pd.DataFrame(
            {
                "osm_idx": [0],
                "overture_idx": [0],
                "distance_m": [50.0],
                "composite_score": [0.75],
            }
        )
        result = merge_matched_pois(
            osm, overture, m,
            np.array(["Park"]), np.array(["Park"]),
        )
        matched = result[
            result["source"] == "matched"
        ].iloc[0]
        assert matched.geometry.geom_type == "Polygon"

    def test_expected_columns(
        self, osm_gdf, overture_gdf, matches,
    ):
        osm_labels = np.array(
            ["Cafe", "Supermarket", "Park"]
        )
        ov_labels = np.array(["Cafe", "Supermarket", "Park"])
        result = merge_matched_pois(
            osm_gdf, overture_gdf, matches,
            osm_labels, ov_labels,
        )
        expected_cols = {
            "unified_id", "source", "osm_id", "overture_id",
            "name", "brand", "shared_label",
            "conf_mean", "conf_lower", "conf_upper",
            "match_score", "match_distance_m",
            "osm_name", "overture_name",
            "osm_brand", "overture_brand",
            "osm_conf_mean", "overture_confidence",
            "geometry",
        }
        assert set(result.columns) == expected_cols

    def test_shared_label_values(
        self, osm_gdf, overture_gdf, matches,
    ):
        osm_labels = np.array(
            ["Cafe", "Supermarket", "Park"]
        )
        ov_labels = np.array(["Cafe", "Supermarket", "Park"])
        result = merge_matched_pois(
            osm_gdf, overture_gdf, matches,
            osm_labels, ov_labels,
        )
        assert "shared_label" in result.columns
        matched = result[
            result["source"] == "matched"
        ].iloc[0]
        assert matched["shared_label"] == "Cafe"
        # Unmatched Park should retain its label
        park_row = result[
            result["shared_label"] == "Park"
        ].iloc[0]
        assert park_row["shared_label"] == "Park"
