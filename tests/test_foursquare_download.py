#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
Unit tests for openpois.foursquare.download.

All external calls (pyiceberg RestCatalog, catalog.load_table, scan) are
mocked so tests run without network access or real credentials.
"""
from __future__ import annotations

import datetime
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from openpois.foursquare.download import (
    get_fsq_catalog,
    get_latest_fsq_release_date,
    load_fsq_us_places,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _make_catalog_mock() -> MagicMock:
    """Return a mock RestCatalog."""
    return MagicMock()


def _make_table_mock(scan_df: pd.DataFrame) -> MagicMock:
    """Return a mock Iceberg table whose scan().to_pandas() returns scan_df."""
    table = MagicMock()
    scan_result = MagicMock()
    scan_result.to_pandas = MagicMock(return_value=scan_df)
    table.scan = MagicMock(return_value=scan_result)
    return table


# ---------------------------------------------------------------------------
# get_fsq_catalog
# ---------------------------------------------------------------------------


class TestGetFsqCatalog:
    def test_raises_environment_error_when_token_not_set(self, monkeypatch):
        """Should raise EnvironmentError when token is None and env var absent."""
        monkeypatch.delenv("FSQ_PORTAL_TOKEN", raising=False)

        mock_catalog_cls = MagicMock()

        with patch.dict("sys.modules", {"pyiceberg.catalog.rest": MagicMock(
            RestCatalog=mock_catalog_cls
        )}):
            with pytest.raises(EnvironmentError, match="FSQ_PORTAL_TOKEN"):
                get_fsq_catalog(
                    catalog_uri="https://example.com",
                    catalog_warehouse="places",
                    token_env_var="FSQ_PORTAL_TOKEN",
                    token=None,
                )

    def test_raises_import_error_when_pyiceberg_missing(self, monkeypatch):
        """Should raise ImportError with a helpful message when pyiceberg absent."""
        import builtins
        import sys

        original_import = builtins.__import__

        def _blocking_import(name, *args, **kwargs):
            if name.startswith("pyiceberg"):
                raise ImportError("No module named 'pyiceberg'")
            return original_import(name, *args, **kwargs)

        # Remove any cached pyiceberg modules so the import is re-attempted
        cached_mods = [k for k in sys.modules if k.startswith("pyiceberg")]
        for mod in cached_mods:
            monkeypatch.delitem(sys.modules, mod)

        monkeypatch.setattr(builtins, "__import__", _blocking_import)

        with pytest.raises(ImportError, match="pyiceberg is required"):
            get_fsq_catalog(
                catalog_uri="https://example.com",
                catalog_warehouse="places",
                token_env_var="FSQ_PORTAL_TOKEN",
                token="fake-token",
            )

    def test_returns_catalog_when_token_supplied_directly(self):
        """Should create and return a RestCatalog when a token is given."""
        mock_rest_catalog_cls = MagicMock()
        mock_instance = MagicMock()
        mock_rest_catalog_cls.return_value = mock_instance

        mock_pyiceberg_module = MagicMock()
        mock_pyiceberg_module.RestCatalog = mock_rest_catalog_cls

        with patch.dict(
            "sys.modules", {"pyiceberg.catalog.rest": mock_pyiceberg_module}
        ):
            # Re-import to pick up the patched module path in the function
            import importlib
            import openpois.foursquare.download as fsq_mod
            importlib.reload(fsq_mod)

            fsq_mod.get_fsq_catalog(
                catalog_uri="https://example.com",
                catalog_warehouse="places",
                token_env_var="FSQ_PORTAL_TOKEN",
                token="my-secret-token",
            )

        mock_rest_catalog_cls.assert_called_once()
        call_kwargs = mock_rest_catalog_cls.call_args.kwargs
        assert call_kwargs["token"] == "my-secret-token"

    def test_reads_token_from_env_var(self, monkeypatch):
        """Should read the token from environment when token param is None."""
        monkeypatch.setenv("FSQ_PORTAL_TOKEN", "env-token-value")

        mock_catalog_cls = MagicMock()
        mock_pyiceberg_module = MagicMock()
        mock_pyiceberg_module.RestCatalog = mock_catalog_cls

        with patch.dict(
            "sys.modules", {"pyiceberg.catalog.rest": mock_pyiceberg_module}
        ):
            import importlib
            import openpois.foursquare.download as fsq_mod
            importlib.reload(fsq_mod)

            fsq_mod.get_fsq_catalog(
                catalog_uri="https://example.com",
                catalog_warehouse="places",
                token_env_var="FSQ_PORTAL_TOKEN",
                token=None,
            )

        call_kwargs = mock_catalog_cls.call_args.kwargs
        assert call_kwargs["token"] == "env-token-value"


# ---------------------------------------------------------------------------
# get_latest_fsq_release_date
# ---------------------------------------------------------------------------


class TestGetLatestFsqReleaseDate:
    def test_raises_value_error_when_no_partition_rows(self):
        """Should raise ValueError when the partitions table is empty."""
        catalog = _make_catalog_mock()
        table = MagicMock()
        partitions_mock = MagicMock()
        partitions_mock.to_pylist = MagicMock(return_value=[])
        table.inspect.partitions = MagicMock(return_value=partitions_mock)
        catalog.load_table = MagicMock(return_value=table)

        with pytest.raises(ValueError, match="No partition metadata found"):
            get_latest_fsq_release_date(
                catalog, catalog_namespace="ns", places_table="places"
            )

    def test_raises_value_error_when_no_last_updated_at(self):
        """Should raise ValueError when partition rows have no last_updated_at."""
        catalog = _make_catalog_mock()
        table = MagicMock()
        partitions_mock = MagicMock()
        partitions_mock.to_pylist = MagicMock(
            return_value=[{"some_other_field": "value"}]
        )
        table.inspect.partitions = MagicMock(return_value=partitions_mock)
        catalog.load_table = MagicMock(return_value=table)

        with pytest.raises(ValueError, match="last_updated_at"):
            get_latest_fsq_release_date(
                catalog, catalog_namespace="ns", places_table="places"
            )

    def test_returns_formatted_date_string_from_datetime(self):
        """Should return 'YYYY-MM-DD' string when timestamps are datetimes."""
        catalog = _make_catalog_mock()
        table = MagicMock()
        partitions_mock = MagicMock()
        dt_older = datetime.datetime(2026, 1, 10, 12, 0, 0)
        dt_newer = datetime.datetime(2026, 2, 12, 8, 30, 0)
        partitions_mock.to_pylist = MagicMock(
            return_value=[
                {"last_updated_at": dt_older},
                {"last_updated_at": dt_newer},
            ]
        )
        table.inspect.partitions = MagicMock(return_value=partitions_mock)
        catalog.load_table = MagicMock(return_value=table)

        result = get_latest_fsq_release_date(
            catalog, catalog_namespace="ns", places_table="places"
        )

        assert result == "2026-02-12"

    def test_returns_date_from_string_fallback(self):
        """Should slice first 10 chars when timestamp lacks strftime."""
        catalog = _make_catalog_mock()
        table = MagicMock()
        partitions_mock = MagicMock()
        # Plain string timestamp (no strftime method)
        partitions_mock.to_pylist = MagicMock(
            return_value=[{"last_updated_at": "2026-03-01T00:00:00Z"}]
        )
        table.inspect.partitions = MagicMock(return_value=partitions_mock)
        catalog.load_table = MagicMock(return_value=table)

        result = get_latest_fsq_release_date(
            catalog, catalog_namespace="ns", places_table="places"
        )

        assert result == "2026-03-01"


# ---------------------------------------------------------------------------
# load_fsq_us_places
# ---------------------------------------------------------------------------


class TestLoadFsqUsPlaces:
    def _make_cats_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "category_id": ["cat_001", "cat_002", "cat_003"],
                "level1_category_name": [
                    "Dining and Drinking",
                    "Retail",
                    "Sports and Recreation",
                ],
                "category_label": ["Restaurant", "Clothing Store", "Gym"],
            }
        )

    def _make_places_df(self) -> pd.DataFrame:
        return pd.DataFrame(
            {
                "fsq_place_id": ["p1", "p2", "p3"],
                "name": ["The Grill", "Shoe Shop", "Yoga Studio"],
                "latitude": [37.0, 38.0, 39.0],
                "longitude": [-120.0, -121.0, -122.0],
                "country": ["US", "US", "US"],
                "fsq_category_ids": [
                    ["cat_001"],   # Dining and Drinking -> keep
                    ["cat_002"],   # Retail -> keep
                    ["cat_003"],   # Sports and Recreation -> filtered out
                ],
                "fsq_category_labels": [
                    ["Restaurant"],
                    ["Clothing Store"],
                    ["Gym"],
                ],
            }
        )

    def test_filters_out_rows_with_no_matching_l1_category(self):
        """Rows whose category IDs don't match target L1 names are dropped."""
        catalog = _make_catalog_mock()
        cats_table = _make_table_mock(self._make_cats_df())
        places_table = _make_table_mock(self._make_places_df())

        def _load_table(name):
            if "categories" in name:
                return cats_table
            return places_table

        catalog.load_table = MagicMock(side_effect=_load_table)

        gdf = load_fsq_us_places(
            catalog=catalog,
            release_date="2026-02-12",
            l1_category_names=["Dining and Drinking", "Retail"],
            catalog_namespace="ns",
            places_table="places",
            categories_table="categories",
        )

        # Sports and Recreation row should be dropped
        assert len(gdf) == 2
        assert "Yoga Studio" not in gdf["name"].values

    def test_correctly_resolves_l1_category_column(self):
        """Each retained row should have the correct L1 category name."""
        catalog = _make_catalog_mock()
        cats_table = _make_table_mock(self._make_cats_df())
        places_table = _make_table_mock(self._make_places_df())

        def _load_table(name):
            if "categories" in name:
                return cats_table
            return places_table

        catalog.load_table = MagicMock(side_effect=_load_table)

        gdf = load_fsq_us_places(
            catalog=catalog,
            release_date="2026-02-12",
            l1_category_names=["Dining and Drinking", "Retail"],
            catalog_namespace="ns",
            places_table="places",
            categories_table="categories",
        )

        grill_row = gdf.loc[gdf["name"] == "The Grill"].iloc[0]
        shop_row = gdf.loc[gdf["name"] == "Shoe Shop"].iloc[0]
        assert grill_row["fsq_l1_category"] == "Dining and Drinking"
        assert shop_row["fsq_l1_category"] == "Retail"

    def test_returns_geodataframe_with_correct_crs(self):
        """Output GeoDataFrame should use EPSG:4326 Point geometries."""
        catalog = _make_catalog_mock()
        cats_table = _make_table_mock(self._make_cats_df())
        places_table = _make_table_mock(self._make_places_df())

        def _load_table(name):
            if "categories" in name:
                return cats_table
            return places_table

        catalog.load_table = MagicMock(side_effect=_load_table)

        gdf = load_fsq_us_places(
            catalog=catalog,
            release_date="2026-02-12",
            l1_category_names=["Dining and Drinking"],
            catalog_namespace="ns",
            places_table="places",
            categories_table="categories",
        )

        assert gdf.crs.to_epsg() == 4326
        from shapely.geometry import Point
        assert all(isinstance(g, Point) for g in gdf.geometry)

    def test_all_rows_filtered_when_no_l1_match(self):
        """Should return an empty GeoDataFrame when no categories match."""
        catalog = _make_catalog_mock()
        cats_table = _make_table_mock(self._make_cats_df())
        places_table = _make_table_mock(self._make_places_df())

        def _load_table(name):
            if "categories" in name:
                return cats_table
            return places_table

        catalog.load_table = MagicMock(side_effect=_load_table)

        gdf = load_fsq_us_places(
            catalog=catalog,
            release_date="2026-02-12",
            l1_category_names=["Nonexistent Category"],
            catalog_namespace="ns",
            places_table="places",
            categories_table="categories",
        )

        assert len(gdf) == 0

    def test_release_date_added_to_output(self):
        """The release_date column should contain the supplied date string."""
        catalog = _make_catalog_mock()
        cats_table = _make_table_mock(self._make_cats_df())
        places_table = _make_table_mock(self._make_places_df())

        def _load_table(name):
            if "categories" in name:
                return cats_table
            return places_table

        catalog.load_table = MagicMock(side_effect=_load_table)

        gdf = load_fsq_us_places(
            catalog=catalog,
            release_date="2026-02-12",
            l1_category_names=["Dining and Drinking"],
            catalog_namespace="ns",
            places_table="places",
            categories_table="categories",
        )

        assert (gdf["release_date"] == "2026-02-12").all()
