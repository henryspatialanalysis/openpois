Workflows
=========

This page describes the four end-to-end pipelines that make up the openpois
data processing system, in the order they should be executed. Each pipeline
is implemented as a series of scripts in the ``scripts/`` directory; the
scripts call library functions documented in the :doc:`api`.

All scripts read their configuration from ``config.yaml`` via
``config_versioned.Config``. See the individual script docstrings for the
exact config keys each script uses.

---

Pipeline 1: POI Snapshot Downloads
------------------------------------

These three scripts are independent and can be run in any order (or in
parallel). Each downloads a current US-wide POI snapshot from one data
source and saves it as a GeoParquet file.

**OSM snapshot**

.. code-block:: bash

   python scripts/osm_snapshot/download.py

Downloads the Geofabrik North America PBF extract (~11 GB), filters with
osmium, and parses with pyosmium. Output: ``osm_snapshot.parquet``
(~7.8 M POIs).

See :mod:`openpois.io.osm_snapshot`.

**Overture Maps snapshot**

.. code-block:: bash

   python scripts/overture/download.py

Queries Overture Maps GeoParquet files on public S3 via DuckDB. No
credentials required. Output: ``overture_snapshot.parquet`` (~7.2 M POIs).

See :mod:`openpois.io.overture`.

**Foursquare OS Places snapshot**

.. code-block:: bash

   python scripts/foursquare/download.py

Authenticates to the Foursquare Places Portal Iceberg catalog (requires
``FSQ_PORTAL_TOKEN`` env var). Output: ``foursquare_snapshot.parquet``
(~8.3 M POIs).

See :mod:`openpois.io.foursquare`.

**Quick schema inspection** *(optional)*

.. code-block:: bash

   python scripts/snapshots/load_samples.py

Reads the first 100 rows of each snapshot without loading the full files,
saving snippet CSVs to the ``testing/`` directory for column inspection.

---

Pipeline 2: OSM Historical Change-Rate Model
--------------------------------------------

This pipeline downloads OpenStreetMap element histories for a Seattle-area
bounding box and fits a Poisson change-rate model to estimate how quickly
different POI categories become outdated.

**Step 1 — Download OSM element histories**

.. code-block:: bash

   python scripts/osm_data/download.py

Queries the Overpass API across a configured date range to collect element IDs
for each tag key, then fetches the full version history of each element via the
OSM API. Outputs ``osm_elements.csv``, ``osm_versions.csv``,
``osm_changes.csv``, and ``osm_failed_elements.csv``.

See :mod:`openpois.io.osm_history`.

**Step 2 — Reformat into observations**

.. code-block:: bash

   python scripts/osm_data/format_tabular.py

Converts raw version histories into one-row-per-observation records, each
flagged for whether the configured tag changed. Output:
``osm_observations_{tag_key}.csv``.

See :mod:`openpois.osm.format_observations`.

**Step 3 — Fit the change-rate model**

.. code-block:: bash

   python scripts/models/osm_turnover.py

Fits an empirical Bayes PyTorch model (constant or random-effects by type)
estimating the Poisson change rate λ per group. Outputs ``fitted_params.csv``
and ``predictions.csv`` (and optionally ``param_draws.csv`` /
``fitted_model.pt``).

See :mod:`openpois.models.model_fitter`, :mod:`openpois.models.setup`, and
:mod:`openpois.models.event_rate`.

**Step 4 — Visualise stability curves** *(optional)*

.. code-block:: bash

   python scripts/osm_data/data_viz.py

Produces Kaplan-Meier-style survival curve plots saved to
``osm_data/viz/``.

See :mod:`openpois.osm.change_plots`.

---

Pipeline 3: Rate the OSM Snapshot
------------------------------------

This pipeline applies the fitted change-rate model (Pipeline 2) to the OSM
snapshot (Pipeline 1) to assign a confidence score to every POI.

**Prerequisites:** Pipeline 2 (model fitted) and Pipeline 1 OSM snapshot.

**Step 1 — Apply model predictions**

.. code-block:: bash

   python scripts/osm_snapshot/apply_model.py
   python scripts/osm_snapshot/apply_model.py --test   # first 10 k rows only

Matches each POI to its best-fit model group (by tag key priority), then
looks up the predicted change probability at the POI's age. Adds columns
``conf_mean``, ``conf_lower``, ``conf_upper``, ``t2_years``,
``model_version``, and ``model_group``. Output: ``osm_snapshot_rated.parquet``.

See :mod:`openpois.models.apply`.

**Step 2 — Partition for upload**

.. code-block:: bash

   python scripts/osm_snapshot/format_for_upload.py

Adds geohash columns and writes a Hive-style partitioned dataset so that the
web map can fetch only the tiles it needs. Output:
``osm_snapshot_partitioned/``.

See :mod:`openpois.io.geohash_partition`.

**Step 3 — Upload to S3**

.. code-block:: bash

   python scripts/osm_snapshot/upload_to_s3.py

Uploads the partitioned dataset to the configured public S3 bucket with
public-read ACL. Requires AWS credentials (``AWS_ACCESS_KEY_ID`` /
``AWS_SECRET_ACCESS_KEY`` env vars or ``~/.aws/credentials``).

See :mod:`openpois.io.s3`.

---

Pipeline 4: Conflation and Upload
------------------------------------

This pipeline conflates the rated OSM snapshot with the Overture Maps snapshot
into a single unified POI dataset for the web map.

**Prerequisites:** Pipeline 3 rated OSM snapshot and Pipeline 1 Overture
snapshot.

**Step 1 — Conflate**

.. code-block:: bash

   python scripts/conflation/conflate.py
   python scripts/conflation/conflate.py --test   # Seattle bbox only

Assigns shared taxonomy labels, finds spatial candidates via BallTree, scores
on distance + name + type + identifiers, performs greedy one-to-one matching,
and saves a unified GeoParquet. Output: ``conflated.parquet``.

See :mod:`openpois.conflation.match`, :mod:`openpois.conflation.merge`, and
:mod:`openpois.conflation.taxonomy`.

**Step 2 — Summarise** *(optional)*

.. code-block:: bash

   python scripts/conflation/summarize.py

Produces a summary CSV with match counts and average match scores per
shared taxonomy label. Output: ``summary_by_label.csv``.

**Step 3 — Partition for upload**

.. code-block:: bash

   python scripts/conflation/format_for_upload.py

Adds geohash columns and writes a Hive-style partitioned dataset.
Output: ``conflated_partitioned/``.

See :mod:`openpois.io.geohash_partition`.

**Step 4 — Upload to S3**

.. code-block:: bash

   python scripts/conflation/upload_to_s3.py

Uploads the partitioned conflated dataset to S3 with public-read ACL.

See :mod:`openpois.io.s3`.
