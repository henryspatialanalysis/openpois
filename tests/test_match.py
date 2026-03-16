"""Tests for openpois.conflation.match."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest
from shapely.geometry import Point

from openpois.conflation.match import (
    _normalize_name,
    compute_match_scores,
    compute_name_scores,
    compute_type_scores,
    find_spatial_candidates,
    select_best_matches,
)


# -----------------------------------------------------------------
# Helpers
# -----------------------------------------------------------------


def _make_geom_array(coords: list[tuple[float, float]]):
    """Create a numpy array of shapely Points from (lon, lat) pairs."""
    return np.array([Point(lon, lat) for lon, lat in coords])


# -----------------------------------------------------------------
# Spatial candidates
# -----------------------------------------------------------------


class TestFindSpatialCandidates:
    def test_nearby_points_match(self):
        """Two points ~11m apart should match at 50m radius."""
        osm_geoms = _make_geom_array([(-122.335, 47.608)])
        ov_geoms = _make_geom_array([(-122.335, 47.6081)])
        radii = np.array([50.0])

        result = find_spatial_candidates(
            osm_geoms, ov_geoms, radii, max_radius_m = 200.0,
        )
        assert len(result) == 1
        assert result.iloc[0]["osm_idx"] == 0
        assert result.iloc[0]["overture_idx"] == 0
        assert result.iloc[0]["distance_m"] < 50.0

    def test_far_points_no_match(self):
        """Two points ~1km apart should not match at 50m radius."""
        osm_geoms = _make_geom_array([(-122.335, 47.608)])
        ov_geoms = _make_geom_array([(-122.335, 47.618)])
        radii = np.array([50.0])

        result = find_spatial_candidates(
            osm_geoms, ov_geoms, radii, max_radius_m = 200.0,
        )
        assert len(result) == 0

    def test_per_poi_radius_filtering(self):
        """A point within 200m but beyond 50m should only match
        if the radius is large enough."""
        osm_geoms = _make_geom_array([
            (-122.335, 47.608),   # 50m radius
            (-122.335, 47.6088),  # 200m radius
        ])
        # This point is ~100m from both OSM points
        ov_geoms = _make_geom_array([(-122.335, 47.6089)])
        radii = np.array([50.0, 200.0])

        result = find_spatial_candidates(
            osm_geoms, ov_geoms, radii, max_radius_m = 200.0,
        )
        # Only the second OSM point (200m radius) should match
        assert len(result) >= 1
        matched_osm = set(result["osm_idx"].tolist())
        assert 1 in matched_osm

    def test_empty_input(self):
        osm_geoms = _make_geom_array([])
        ov_geoms = _make_geom_array([(-122.335, 47.608)])
        radii = np.array([], dtype = float)

        result = find_spatial_candidates(
            osm_geoms, ov_geoms, radii, max_radius_m = 200.0,
        )
        assert len(result) == 0

    def test_chunking(self):
        """Verify chunking produces same results as no chunking."""
        np.random.seed(42)
        n = 100
        osm_lons = -122.3 + np.random.randn(n) * 0.01
        osm_lats = 47.6 + np.random.randn(n) * 0.01
        ov_lons = -122.3 + np.random.randn(n) * 0.01
        ov_lats = 47.6 + np.random.randn(n) * 0.01

        osm_geoms = _make_geom_array(
            list(zip(osm_lons, osm_lats))
        )
        ov_geoms = _make_geom_array(
            list(zip(ov_lons, ov_lats))
        )
        radii = np.full(n, 200.0)

        r1 = find_spatial_candidates(
            osm_geoms, ov_geoms, radii,
            max_radius_m = 200.0, chunk_size = 10,
        )
        r2 = find_spatial_candidates(
            osm_geoms, ov_geoms, radii,
            max_radius_m = 200.0, chunk_size = 1000,
        )
        assert len(r1) == len(r2)


# -----------------------------------------------------------------
# Name scoring
# -----------------------------------------------------------------


class TestNameScoring:
    def test_identical_names(self):
        scores = compute_name_scores(
            osm_names = np.array(["Starbucks"]),
            osm_brands = np.array([None]),
            overture_names = np.array(["Starbucks"]),
            overture_brands = np.array([None]),
            osm_idx = np.array([0]),
            overture_idx = np.array([0]),
        )
        assert scores[0] == pytest.approx(1.0)

    def test_brand_vs_name(self):
        """Brand-to-name cross-compare should still score high."""
        scores = compute_name_scores(
            osm_names = np.array([None]),
            osm_brands = np.array(["Walmart"]),
            overture_names = np.array(["Walmart Supercenter"]),
            overture_brands = np.array([None]),
            osm_idx = np.array([0]),
            overture_idx = np.array([0]),
        )
        assert scores[0] > 0.8

    def test_all_null_neutral(self):
        """All null names/brands should give neutral 0.5."""
        scores = compute_name_scores(
            osm_names = np.array([None]),
            osm_brands = np.array([None]),
            overture_names = np.array([None]),
            overture_brands = np.array([None]),
            osm_idx = np.array([0]),
            overture_idx = np.array([0]),
        )
        assert scores[0] == pytest.approx(0.5)

    def test_completely_different(self):
        scores = compute_name_scores(
            osm_names = np.array(["Alpha Restaurant"]),
            osm_brands = np.array([None]),
            overture_names = np.array(["Zephyr Grocery"]),
            overture_brands = np.array([None]),
            osm_idx = np.array([0]),
            overture_idx = np.array([0]),
        )
        assert scores[0] < 0.5


class TestNormalizeName:
    def test_basic(self):
        assert _normalize_name("  Hello  World ") == "hello world"

    def test_none(self):
        assert _normalize_name(None) == ""

    def test_nan(self):
        assert _normalize_name(float("nan")) == ""


# -----------------------------------------------------------------
# Type taxonomy scoring
# -----------------------------------------------------------------


class TestTypeScoring:
    def test_exact_shared_label(self):
        scores = compute_type_scores(
            osm_shared_labels = np.array(["Restaurant"]),
            overture_shared_labels = np.array(["Restaurant"]),
            osm_l0_bits = np.array([3], dtype = np.uint8),
            overture_l0_bits = np.array([2], dtype = np.uint8),
            osm_idx = np.array([0]),
            overture_idx = np.array([0]),
        )
        assert scores[0] == 1.0

    def test_l0_overlap(self):
        """Different labels, but L0 bits overlap -> 0.5."""
        scores = compute_type_scores(
            osm_shared_labels = np.array(["Restaurant"]),
            overture_shared_labels = np.array(["Cafe"]),
            osm_l0_bits = np.array([3], dtype = np.uint8),
            overture_l0_bits = np.array([2], dtype = np.uint8),
            osm_idx = np.array([0]),
            overture_idx = np.array([0]),
        )
        assert scores[0] == 0.5

    def test_no_l0_overlap(self):
        """Different labels, no L0 overlap -> 0.0."""
        scores = compute_type_scores(
            osm_shared_labels = np.array(["Restaurant"]),
            overture_shared_labels = np.array(["Park"]),
            osm_l0_bits = np.array([3], dtype = np.uint8),
            overture_l0_bits = np.array([16], dtype = np.uint8),
            osm_idx = np.array([0]),
            overture_idx = np.array([0]),
        )
        assert scores[0] == 0.0

    def test_unmapped_zero(self):
        """Empty label + zero bits -> 0.0."""
        scores = compute_type_scores(
            osm_shared_labels = np.array([""]),
            overture_shared_labels = np.array(["Restaurant"]),
            osm_l0_bits = np.array([0], dtype = np.uint8),
            overture_l0_bits = np.array([2], dtype = np.uint8),
            osm_idx = np.array([0]),
            overture_idx = np.array([0]),
        )
        assert scores[0] == 0.0


# -----------------------------------------------------------------
# Composite scoring and selection
# -----------------------------------------------------------------


class TestSelectBestMatches:
    def test_greedy_one_to_one(self):
        scored = pd.DataFrame(
            {
                "osm_idx": [0, 0, 1],
                "overture_idx": [0, 1, 0],
                "distance_m": [10, 20, 15],
                "composite_score": [0.9, 0.7, 0.8],
            }
        )
        result = select_best_matches(scored, min_score = 0.67)
        # OSM 0 -> Overture 0 (score 0.9, highest)
        # OSM 1 -> Overture 0 is blocked, so OSM 1 unmatched
        # OSM 0 -> Overture 1 is blocked, so only 1 match
        assert len(result) == 1
        assert result.iloc[0]["osm_idx"] == 0
        assert result.iloc[0]["overture_idx"] == 0

    def test_min_score_filter(self):
        scored = pd.DataFrame(
            {
                "osm_idx": [0, 1],
                "overture_idx": [0, 1],
                "distance_m": [10, 10],
                "composite_score": [0.8, 0.5],
            }
        )
        result = select_best_matches(scored, min_score = 0.67)
        assert len(result) == 1

    def test_empty_input(self):
        scored = pd.DataFrame(
            columns = [
                "osm_idx", "overture_idx",
                "distance_m", "composite_score",
            ]
        )
        result = select_best_matches(scored, min_score = 0.67)
        assert len(result) == 0
