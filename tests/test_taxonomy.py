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
    """Small Overture crosswalk covering all 4 tiers."""
    return pd.DataFrame(
        {
            "overture_l0": [
                # Tier 1: L0 + L1 + L2 (all populated)
                "food_and_drink", "food_and_drink",
                "shopping", "shopping",
                "sports_and_recreation",
                # Tier 2: L0 + L2 (L1 empty)
                "arts_and_entertainment",
                # Tier 3: L0 + L1 (L2 empty, catch-all)
                "shopping",
                # Tier 4: L0-only (both L1 and L2 empty)
                "shopping",
            ],
            "overture_l1": [
                "restaurant", "beverage_shop",
                "food_and_beverage_store", "market",
                "park",
                "",
                "market",
                "",
            ],
            "overture_l2": [
                "restaurant", "cafe",
                "supermarket", "farmers_market",
                "park",
                "college",
                "",
                "",
            ],
            "shared_label": [
                "Restaurant", "Cafe",
                "Supermarket", "Farmers Market",
                "Park",
                "University",
                "Market",
                "Other Shop",
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
                "Farmers Market", "University", "Market",
            ],
            "match_radius_m": [
                "100", "100", "100",
                "200", "100", "200",
                "100", "200", "50",
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
            "overture_l2", "shared_label",
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
    def test_tier1_l0_l1_l2_match(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        """Tier 1: exact (L0, L1, L2) match."""
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": ["food_and_drink"],
                "taxonomy_l1": ["restaurant"],
                "taxonomy_l2": ["restaurant"],
            }
        )
        labels, radii = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == "Restaurant"
        assert radii[0] == 100.0

    def test_tier1_differentiates_same_l1(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        """Two POIs with same (L0, L1) but different L2 get
        different labels via tier 1."""
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": [
                    "shopping", "shopping",
                ],
                "taxonomy_l1": [
                    "market", "market",
                ],
                "taxonomy_l2": [
                    "farmers_market", "flea_market",
                ],
            }
        )
        labels, radii = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == "Farmers Market"
        # flea_market has no L2 match, falls to tier 3 catch-all
        assert labels[1] == "Market"

    def test_tier2_l0_l2_ignores_l1(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        """Tier 2: (L0, L2) match ignores the L1 in data."""
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": ["arts_and_entertainment"],
                "taxonomy_l1": ["performing_arts_venue"],
                "taxonomy_l2": ["college"],
            }
        )
        labels, radii = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == "University"
        assert radii[0] == 200.0

    def test_tier3_l0_l1_catchall(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        """Tier 3: (L0, L1) catch-all when L2 is unmatched."""
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": ["shopping"],
                "taxonomy_l1": ["market"],
                "taxonomy_l2": ["night_market"],
            }
        )
        labels, radii = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == "Market"
        assert radii[0] == 50.0

    def test_tier4_l0_fallback(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        """Tier 4: L0-only when nothing else matches."""
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": ["shopping"],
                "taxonomy_l1": ["unknown_l1"],
                "taxonomy_l2": ["unknown_l2"],
            }
        )
        labels, radii = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == "Other Shop"

    def test_null_taxonomy(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": [None],
                "taxonomy_l1": [None],
                "taxonomy_l2": [None],
            }
        )
        labels, radii = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == ""
        assert radii[0] == 100.0

    def test_backward_compat_no_l2_column(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        """GeoDataFrame without taxonomy_l2 still works via
        tier 3 and tier 4 fallback."""
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": ["shopping"],
                "taxonomy_l1": ["market"],
            }
        )
        labels, radii = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == "Market"

    def test_tier1_wins_over_tier3(
        self, mini_overture_crosswalk, mini_match_radii,
    ):
        """Tier 1 (L0+L1+L2) takes priority over tier 3
        (L0+L1 catch-all)."""
        gdf = pd.DataFrame(
            {
                "taxonomy_l0": ["shopping"],
                "taxonomy_l1": ["market"],
                "taxonomy_l2": ["farmers_market"],
            }
        )
        labels, _ = assign_overture_shared_label(
            gdf, mini_overture_crosswalk, mini_match_radii,
        )
        assert labels[0] == "Farmers Market"


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
