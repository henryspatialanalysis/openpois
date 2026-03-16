"""Tests for openpois.conflation.taxonomy."""
from __future__ import annotations

import numpy as np
import pandas as pd
import pytest

from openpois.conflation.taxonomy import (
    assign_osm_shared_label,
    assign_overture_shared_label,
    compute_osm_l0_bits,
    compute_overture_l0_bits,
    load_match_radii,
    load_osm_crosswalk,
    load_overture_crosswalk,
    load_top_level_matches,
)


# -----------------------------------------------------------------
# Fixtures
# -----------------------------------------------------------------


@pytest.fixture
def mini_osm_crosswalk():
    """Small OSM crosswalk for focused tests."""
    return pd.DataFrame(
        {
            "osm_key": [
                "amenity", "amenity", "amenity",
                "shop", "shop",
                "leisure",
            ],
            "osm_value": [
                "restaurant", "cafe", "*",
                "supermarket", "*",
                "park",
            ],
            "shared_label": [
                "Restaurant", "Cafe", "Other Amenity",
                "Supermarket", "Other Shop",
                "Park",
            ],
        }
    )


@pytest.fixture
def mini_overture_crosswalk():
    """Small Overture crosswalk for focused tests."""
    return pd.DataFrame(
        {
            "overture_l0": [
                "food_and_drink", "food_and_drink",
                "shopping",
                "sports_and_recreation",
            ],
            "overture_l1": [
                "restaurant", "beverage_shop",
                "food_and_beverage_store",
                "park",
            ],
            "poi_category": [
                "restaurant", "cafe",
                "supermarket",
                "park",
            ],
            "shared_label": [
                "Restaurant", "Cafe",
                "Supermarket",
                "Park",
            ],
        }
    )


@pytest.fixture
def mini_match_radii():
    """Small match-radii table for focused tests."""
    return pd.DataFrame(
        {
            "shared_label": [
                "Restaurant", "Cafe", "Other Amenity",
                "Supermarket", "Other Shop", "Park",
            ],
            "match_radius_m": [
                "100", "100", "100",
                "200", "100", "200",
            ],
        }
    )


@pytest.fixture
def mini_top_level_matches():
    """Small top-level matches table for focused tests."""
    return pd.DataFrame(
        {
            "overture_l0": [
                "arts_and_entertainment",
                "food_and_drink",
                "health_care",
                "shopping",
                "sports_and_recreation",
            ],
            "osm_key": [
                "amenity", "amenity",
                "healthcare", "shop", "leisure",
            ],
        }
    )


# -----------------------------------------------------------------
# CSV loaders
# -----------------------------------------------------------------


class TestLoadOsmCrosswalk:
    def test_returns_dataframe(self):
        cw = load_osm_crosswalk()
        assert isinstance(cw, pd.DataFrame)
        assert len(cw) > 0

    def test_expected_columns(self):
        cw = load_osm_crosswalk()
        assert set(cw.columns) == {
            "osm_key", "osm_value", "shared_label",
        }

    def test_has_wildcard_rows(self):
        cw = load_osm_crosswalk()
        wildcards = cw[cw["osm_value"] == "*"]
        assert len(wildcards) >= 4


class TestLoadOvertureCrosswalk:
    def test_returns_dataframe(self):
        cw = load_overture_crosswalk()
        assert isinstance(cw, pd.DataFrame)
        assert len(cw) > 0

    def test_expected_columns(self):
        cw = load_overture_crosswalk()
        assert set(cw.columns) == {
            "overture_l0", "overture_l1",
            "poi_category", "shared_label",
        }


class TestLoadMatchRadii:
    def test_returns_dataframe(self):
        mr = load_match_radii()
        assert isinstance(mr, pd.DataFrame)
        assert len(mr) > 0

    def test_expected_columns(self):
        mr = load_match_radii()
        assert set(mr.columns) == {
            "shared_label", "match_radius_m",
        }


class TestLoadTopLevelMatches:
    def test_returns_dataframe(self):
        tlm = load_top_level_matches()
        assert isinstance(tlm, pd.DataFrame)
        assert len(tlm) == 5

    def test_expected_columns(self):
        tlm = load_top_level_matches()
        assert set(tlm.columns) == {"overture_l0", "osm_key"}


# -----------------------------------------------------------------
# OSM shared-label assignment
# -----------------------------------------------------------------


class TestAssignOsmSharedLabel:
    def test_exact_match(
        self, mini_osm_crosswalk, mini_match_radii,
    ):
        gdf = pd.DataFrame(
            {
                "amenity": ["restaurant", "cafe"],
                "shop": [None, None],
            }
        )
        labels, radii = assign_osm_shared_label(
            gdf, mini_osm_crosswalk, mini_match_radii,
            ["shop", "amenity"],
        )
        assert labels[0] == "Restaurant"
        assert labels[1] == "Cafe"
        assert radii[0] == 100.0

    def test_wildcard_fallback(
        self, mini_osm_crosswalk, mini_match_radii,
    ):
        gdf = pd.DataFrame(
            {"amenity": ["bank"], "shop": [None]}
        )
        labels, radii = assign_osm_shared_label(
            gdf, mini_osm_crosswalk, mini_match_radii,
            ["shop", "amenity"],
        )
        assert labels[0] == "Other Amenity"

    def test_priority_order(
        self, mini_osm_crosswalk, mini_match_radii,
    ):
        """shop should take priority over amenity."""
        gdf = pd.DataFrame(
            {
                "amenity": ["restaurant"],
                "shop": ["supermarket"],
            }
        )
        labels, radii = assign_osm_shared_label(
            gdf, mini_osm_crosswalk, mini_match_radii,
            ["shop", "amenity"],
        )
        assert labels[0] == "Supermarket"
        assert radii[0] == 200.0

    def test_empty_dataframe(
        self, mini_osm_crosswalk, mini_match_radii,
    ):
        gdf = pd.DataFrame(
            {"amenity": pd.Series(dtype = str)}
        )
        labels, radii = assign_osm_shared_label(
            gdf, mini_osm_crosswalk, mini_match_radii,
            ["amenity"],
        )
        assert len(labels) == 0

    def test_all_null(
        self, mini_osm_crosswalk, mini_match_radii,
    ):
        gdf = pd.DataFrame(
            {
                "amenity": [None, None],
                "shop": [None, None],
            }
        )
        labels, radii = assign_osm_shared_label(
            gdf, mini_osm_crosswalk, mini_match_radii,
            ["shop", "amenity"],
        )
        assert labels[0] == ""
        assert labels[1] == ""


# -----------------------------------------------------------------
# Overture shared-label assignment
# -----------------------------------------------------------------


class TestAssignOvertureSharedLabel:
    def test_exact_l0_l1_match(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": ["food_and_drink"],
                "taxonomy_l1": ["restaurant"],
            }
        )
        labels, radii = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == "Restaurant"
        assert radii[0] == 100.0

    def test_null_taxonomy(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": [None],
                "taxonomy_l1": [None],
            }
        )
        labels, radii = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == ""
        assert radii[0] == 100.0


# -----------------------------------------------------------------
# L0 bitmask helpers
# -----------------------------------------------------------------


class TestL0Bitmasks:
    def test_osm_amenity_gets_two_bits(
        self, mini_top_level_matches,
    ):
        """amenity maps to arts_and_entertainment (1) and
        food_and_drink (2), so bits = 3."""
        gdf = pd.DataFrame(
            {"amenity": ["restaurant"], "shop": [None]}
        )
        bits = compute_osm_l0_bits(
            gdf, mini_top_level_matches,
        )
        assert bits[0] == 1 | 2  # 3

    def test_osm_shop_gets_one_bit(
        self, mini_top_level_matches,
    ):
        gdf = pd.DataFrame(
            {"amenity": [None], "shop": ["supermarket"]}
        )
        bits = compute_osm_l0_bits(
            gdf, mini_top_level_matches,
        )
        assert bits[0] == 8  # shopping

    def test_osm_both_keys_ored(
        self, mini_top_level_matches,
    ):
        gdf = pd.DataFrame(
            {
                "amenity": ["restaurant"],
                "shop": ["supermarket"],
            }
        )
        bits = compute_osm_l0_bits(
            gdf, mini_top_level_matches,
        )
        # amenity (1|2) | shop (8) = 11
        assert bits[0] == 11

    def test_osm_null_gets_zero(
        self, mini_top_level_matches,
    ):
        gdf = pd.DataFrame(
            {"amenity": [None], "shop": [None]}
        )
        bits = compute_osm_l0_bits(
            gdf, mini_top_level_matches,
        )
        assert bits[0] == 0

    def test_overture_food_and_drink(self):
        l0 = np.array(["food_and_drink"])
        bits = compute_overture_l0_bits(l0)
        assert bits[0] == 2

    def test_overture_shopping(self):
        l0 = np.array(["shopping"])
        bits = compute_overture_l0_bits(l0)
        assert bits[0] == 8

    def test_overture_null_gets_zero(self):
        l0 = np.array([""])
        bits = compute_overture_l0_bits(l0)
        assert bits[0] == 0
