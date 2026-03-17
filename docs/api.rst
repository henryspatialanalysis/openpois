API Reference
=============

conflation
----------

openpois.conflation.match
~~~~~~~~~~~~~~~~~~~~~~~~~

Spatial candidate matching and composite scoring for POI conflation. Provides
a BallTree-based radius search to find nearby OSM–Overture candidate pairs
within category-specific thresholds, a multi-component scorer (distance, name
similarity, taxonomy agreement, shared identifiers), and a greedy one-to-one
assignment step that filters below a minimum composite score.

.. automodule:: openpois.conflation.match
   :members:
   :undoc-members:
   :show-inheritance:

openpois.conflation.merge
~~~~~~~~~~~~~~~~~~~~~~~~~

Merge matched and unmatched POIs into a unified conflated GeoDataFrame.
Produces a superset containing matched OSM–Overture pairs with blended
confidence scores, unmatched OSM POIs at their original confidence, and
unmatched Overture POIs at downweighted confidence. Uses a disk-backed
split-then-concat pattern to avoid peak memory issues at CONUS scale.

.. automodule:: openpois.conflation.merge
   :members:
   :undoc-members:
   :show-inheritance:

openpois.conflation.taxonomy
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Taxonomy crosswalk between OSM tags and the Overture Maps category hierarchy.
Loads four CSV reference files (OSM crosswalk, Overture crosswalk, match radii,
and top-level key-to-L0 mappings) and provides functions to assign each POI a
``shared_label`` string, a per-category spatial match radius, and an L0 bitmask
used for type-agreement scoring.

.. automodule:: openpois.conflation.taxonomy
   :members:
   :undoc-members:
   :show-inheritance:

----

io
--

openpois.io.osm_history
~~~~~~~~~~~~~~~~~~~~~~~

Download OpenStreetMap element change histories via the Overpass and OSM APIs.
Builds Overpass queries across a configured date range to collect element IDs,
then fetches the full version history of each element, producing per-version
and per-change tables suitable for the change-rate model.

.. automodule:: openpois.io.osm_history
   :members:
   :undoc-members:
   :show-inheritance:

openpois.io.osm_snapshot
~~~~~~~~~~~~~~~~~~~~~~~~

Download a current US-wide OSM POI snapshot from a Geofabrik PBF extract.
Streams the PBF (~11 GB), runs ``osmium tags-filter`` to reduce it to
matching tag keys, then parses nodes and way centroids with pyosmium into
a GeoParquet file. The osmium binary is resolved from the conda environment
rather than the system PATH.

.. automodule:: openpois.io.osm_snapshot
   :members:
   :undoc-members:
   :show-inheritance:

openpois.io.overture
~~~~~~~~~~~~~~~~~~~~

Download a current US-wide Overture Maps Places snapshot. Uses DuckDB's
``httpfs`` and ``spatial`` extensions to query Overture GeoParquet files
directly from public S3, filtering by bounding box and L0 taxonomy category.
No authentication is required. Auto-detects the latest Overture release date
from S3 if a specific date is not pinned.

.. automodule:: openpois.io.overture
   :members:
   :undoc-members:
   :show-inheritance:

openpois.io.foursquare
~~~~~~~~~~~~~~~~~~~~~~

Download a current US-wide Foursquare OS Places snapshot via the Foursquare
Places Portal Apache Iceberg REST catalog. Authenticates with a portal token,
loads US open venues filtered by L1 category, and resolves category names from
the categories table. Requires the ``FSQ_PORTAL_TOKEN`` environment variable.

.. automodule:: openpois.io.foursquare
   :members:
   :undoc-members:
   :show-inheritance:

openpois.io.geohash_partition
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Utilities for spatially partitioning GeoDataFrames by geohash for efficient
web-map viewport queries. Computes geohash columns from geometry centroids,
writes Hive-style partitioned Parquet datasets (``geohash_prefix=XX/``), and
sorts rows within each partition by a finer geohash for spatial locality.

.. automodule:: openpois.io.geohash_partition
   :members:
   :undoc-members:
   :show-inheritance:

openpois.io.s3
~~~~~~~~~~~~~~

Upload a locally partitioned dataset to a public S3 bucket. Walks the Hive
partition directory, uploads each Parquet file under a versioned S3 prefix
with public-read ACL, and reports the public base URL on completion. Requires
AWS credentials via environment variables or ``~/.aws/credentials``.

.. automodule:: openpois.io.s3
   :members:
   :undoc-members:
   :show-inheritance:

----

models
------

openpois.models.event_rate
~~~~~~~~~~~~~~~~~~~~~~~~~~

Representation of a Poisson event rate (λ) used by the change-rate model.
Wraps a constant or time-varying λ tensor and computes the probability that
at least one change event occurs within a given time interval via numerical
or closed-form integration.

.. automodule:: openpois.models.event_rate
   :members:
   :undoc-members:
   :show-inheritance:

openpois.models.model_fitter
~~~~~~~~~~~~~~~~~~~~~~~~~~~~

L-BFGS optimizer wrapper for POI change-rate models. Fits model parameters
using PyTorch and the ``torchmin`` optimizer, generates posterior parameter
draws for uncertainty quantification, and produces prediction tables of
change probability versus time.

.. automodule:: openpois.models.model_fitter
   :members:
   :undoc-members:
   :show-inheritance:

openpois.models.setup
~~~~~~~~~~~~~~~~~~~~~

Environment setup utilities for PyTorch model runs. Selects GPU or CPU
device, configures ``torch_continuum`` optimisation level, and prepares
filtered and grouped observation data for model fitting.

.. automodule:: openpois.models.setup
   :members:
   :undoc-members:
   :show-inheritance:

openpois.models.apply
~~~~~~~~~~~~~~~~~~~~~

Apply saved change-rate model predictions to a POI snapshot. Loads
``predictions.csv`` from a versioned model output directory and builds
fast numpy lookup arrays (indexed by group and time step) for both constant
and random-effects model variants.

.. automodule:: openpois.models.apply
   :members:
   :undoc-members:
   :show-inheritance:

----

osm
---

openpois.osm.format_observations
~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~

Convert raw OSM version histories into modelling-ready observation records.
Joins version and change tables to produce one row per element version,
with timestamps for the previous and current tag values and a flag indicating
whether the configured tag changed at this version.

.. automodule:: openpois.osm.format_observations
   :members:
   :undoc-members:
   :show-inheritance:

openpois.osm.change_plots
~~~~~~~~~~~~~~~~~~~~~~~~~

Kaplan-Meier-style tag stability plots using plotnine. Computes the
proportion of tag assignments that remain unchanged over time from
observation records, and renders single-panel and faceted multi-panel
figures saved as PNG files.

.. automodule:: openpois.osm.change_plots
   :members:
   :undoc-members:
   :show-inheritance:
