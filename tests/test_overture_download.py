#   -------------------------------------------------------------
#   Copyright (c) Henry Spatial Analysis. All rights reserved.
#   Licensed under the MIT License. See LICENSE in project root for information.
#   -------------------------------------------------------------

"""
Unit tests for openpois.overture.download.

All external calls (requests.get for S3 listing, duckdb queries) are mocked
so tests run without network access.
"""
from __future__ import annotations

import textwrap
from unittest.mock import MagicMock, patch

import geopandas as gpd
import pandas as pd
import pytest

from openpois.overture.download import (
    build_overture_s3_path,
    download_overture_snapshot,
    get_latest_release_date,
)


# ---------------------------------------------------------------------------
# Fixtures / helpers
# ---------------------------------------------------------------------------

# Minimal S3 XML list-type=2 response with two release prefixes.
_S3_XML_TWO_RELEASES = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>overturemaps-us-west-2</Name>
      <CommonPrefixes><Prefix>release/2025-11-13.0/</Prefix></CommonPrefixes>
      <CommonPrefixes><Prefix>release/2026-02-18.0/</Prefix></CommonPrefixes>
    </ListBucketResult>
""")

_S3_XML_EMPTY = textwrap.dedent("""\
    <?xml version="1.0" encoding="UTF-8"?>
    <ListBucketResult xmlns="http://s3.amazonaws.com/doc/2006-03-01/">
      <Name>overturemaps-us-west-2</Name>
    </ListBucketResult>
""")


def _mock_requests_response(text: str, status_code: int = 200) -> MagicMock:
    resp = MagicMock()
    resp.text = text
    resp.status_code = status_code
    resp.raise_for_status = MagicMock()
    return resp


# ---------------------------------------------------------------------------
# get_latest_release_date
# ---------------------------------------------------------------------------


class TestGetLatestReleaseDate:
    def test_returns_lexicographically_latest_date(self):
        """Should return the largest date string when multiple releases exist."""
        mock_resp = _mock_requests_response(_S3_XML_TWO_RELEASES)

        with patch(
            "openpois.overture.download.requests.get", return_value=mock_resp
        ):
            result = get_latest_release_date("overturemaps-us-west-2")

        assert result == "2026-02-18.0"

    def test_raises_value_error_when_no_prefixes(self):
        """Should raise ValueError when the S3 bucket lists no release prefixes."""
        mock_resp = _mock_requests_response(_S3_XML_EMPTY)

        with patch(
            "openpois.overture.download.requests.get", return_value=mock_resp
        ):
            with pytest.raises(ValueError, match="No release prefixes found"):
                get_latest_release_date("overturemaps-us-west-2")

    def test_raises_on_http_error(self):
        """Should propagate HTTPError from raise_for_status."""
        import requests as _req

        mock_resp = _mock_requests_response("", status_code=403)
        mock_resp.raise_for_status = MagicMock(
            side_effect=_req.HTTPError("403 Forbidden")
        )

        with patch(
            "openpois.overture.download.requests.get", return_value=mock_resp
        ):
            with pytest.raises(_req.HTTPError):
                get_latest_release_date("overturemaps-us-west-2")

    def test_queries_correct_s3_url(self):
        """Should construct the S3 list-type=2 URL with the given bucket name."""
        mock_resp = _mock_requests_response(_S3_XML_TWO_RELEASES)

        with patch(
            "openpois.overture.download.requests.get", return_value=mock_resp
        ) as mock_get:
            get_latest_release_date("my-bucket")

        url = mock_get.call_args[0][0]
        assert "my-bucket.s3.amazonaws.com" in url
        assert "list-type=2" in url
        assert "prefix=release" in url


# ---------------------------------------------------------------------------
# build_overture_s3_path
# ---------------------------------------------------------------------------


class TestBuildOvertureS3Path:
    def test_returns_expected_path_format(self):
        """Should embed bucket, release date, and place paths correctly."""
        result = build_overture_s3_path(
            release_date="2026-02-18.0",
            bucket="overturemaps-us-west-2",
        )
        assert result == (
            "s3://overturemaps-us-west-2/release/2026-02-18.0"
            "/theme=places/type=place/*.parquet"
        )

    def test_different_bucket(self):
        """Should use whatever bucket name is supplied."""
        result = build_overture_s3_path(
            release_date="2025-01-01.0",
            bucket="custom-bucket",
        )
        assert result.startswith("s3://custom-bucket/")
        assert "2025-01-01.0" in result


# ---------------------------------------------------------------------------
# download_overture_snapshot
# ---------------------------------------------------------------------------


class TestDownloadOvertureSnapshot:
    def _make_mock_conn(self, df: pd.DataFrame) -> MagicMock:
        """Return a mock duckdb connection whose execute().df() returns df."""
        conn = MagicMock()
        # conn.execute() is called multiple times (INSTALL, LOAD, SET, query)
        exec_result = MagicMock()
        exec_result.df = MagicMock(return_value=df)
        conn.execute = MagicMock(return_value=exec_result)
        return conn

    def test_calls_duckdb_with_s3_path_and_bbox(self, tmp_path):
        """DuckDB query should reference the S3 path and bbox filter values."""
        output = tmp_path / "overture.parquet"
        bbox = {"xmin": -125.0, "ymin": 24.0, "xmax": -66.0, "ymax": 50.0}
        df = pd.DataFrame(
            {
                "source": ["overture"],
                "overture_id": ["abc"],
                "release_date": ["2026-02-18.0"],
                "taxonomy_l0": ["eat_and_drink"],
                "taxonomy_l1": [None],
                "overture_name": ["Cafe"],
                "brand_name": [None],
                "confidence": [0.9],
                "longitude": [-120.0],
                "latitude": [37.0],
            }
        )
        mock_conn = self._make_mock_conn(df)

        with patch("openpois.overture.download.duckdb.connect", return_value=mock_conn), \
             patch.object(
                 gpd.GeoDataFrame, "to_parquet", return_value=None
             ):
            gdf = download_overture_snapshot(
                output_path=output,
                taxonomy_l0_categories=["eat_and_drink"],
                bbox=bbox,
                bucket="overturemaps-us-west-2",
                s3_region="us-west-2",
                release_date="2026-02-18.0",
            )

        # Collect all SQL strings passed to conn.execute
        sql_calls = [
            str(c.args[0]) for c in mock_conn.execute.call_args_list if c.args
        ]
        final_query = sql_calls[-1]  # last call is the data query
        assert "read_parquet" in final_query
        assert "2026-02-18.0" in final_query
        assert str(bbox["xmin"]) in final_query
        assert "eat_and_drink" in final_query

        assert len(gdf) == 1
        assert gdf.crs.to_epsg() == 4326

    def test_fetches_latest_release_when_not_provided(self, tmp_path):
        """Should call get_latest_release_date when release_date is None."""
        output = tmp_path / "overture.parquet"
        df = pd.DataFrame(
            {
                "source": [],
                "overture_id": [],
                "release_date": [],
                "taxonomy_l0": [],
                "taxonomy_l1": [],
                "overture_name": [],
                "brand_name": [],
                "confidence": [],
                "longitude": [],
                "latitude": [],
            }
        )
        mock_conn = self._make_mock_conn(df)

        with (
            patch(
                "openpois.overture.download.duckdb.connect",
                return_value=mock_conn,
            ),
            patch(
                "openpois.overture.download.get_latest_release_date",
                return_value="2026-02-18.0",
            ) as mock_latest,
            patch.object(gpd.GeoDataFrame, "to_parquet", return_value=None),
        ):
            download_overture_snapshot(
                output_path=output,
                taxonomy_l0_categories=["eat_and_drink"],
                bbox={"xmin": -125.0, "ymin": 24.0, "xmax": -66.0, "ymax": 50.0},
                bucket="overturemaps-us-west-2",
                s3_region="us-west-2",
                release_date=None,
            )

        mock_latest.assert_called_once_with(bucket="overturemaps-us-west-2")
