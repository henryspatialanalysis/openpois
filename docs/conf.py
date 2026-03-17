# Configuration file for the Sphinx documentation builder.
# https://www.sphinx-doc.org/en/master/usage/configuration.html

import os
import sys

sys.path.insert(0, os.path.abspath("../src/"))

# -- Project information -------------------------------------------------------

project = "openpois"
copyright = "2024, Nathaniel Henry"
author = "Nathaniel Henry"
release = "0.1.0"

# -- General configuration -----------------------------------------------------

extensions = [
    "sphinx.ext.autodoc",
    "sphinx.ext.napoleon",
    "sphinx.ext.viewcode",
    "sphinx.ext.intersphinx",
]

templates_path = ["_templates"]
exclude_patterns = ["_build", "Thumbs.db", ".DS_Store"]

# Mock heavy imports so Sphinx can parse docstrings without installing the
# full conda environment in CI.
autodoc_mock_imports = [
    "torch",
    "numpy",
    "pandas",
    "geopandas",
    "shapely",
    "fiona",
    "pyosmium",
    "osmium",
    "duckdb",
    "pyiceberg",
    "pyarrow",
    "config_versioned",
    "ngeohash",
    "requests",
    "scipy",
    "matplotlib",
    "sklearn",
    "overturemaps",
    "plotnine",
    "mizani",
]

# Napoleon settings
napoleon_include_init_with_doc = True
napoleon_include_private_with_doc = False

# Autodoc settings
autodoc_default_options = {
    "members": True,
    "undoc-members": True,
    "show-inheritance": True,
}

# -- Options for HTML output ---------------------------------------------------

html_theme = "sphinx_rtd_theme"
