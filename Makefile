# Export the environment to a yml file
export_env:
	@conda env export > environment.yml;

# Build conda environment from the yml file
build_env:
	@conda env create -f environment.yml;

# Install the package to pip
install_package:
	@pip install -e .;

# Run the unit test suite (no network calls, runs in seconds)
test:
	@python -m pytest tests/ -v;

# Lint source code, exploratory scripts, and tests
# Uses the openpois conda env's binaries regardless of whether it is activated
CONDA_PYTHON := $(shell conda run -n openpois which python 2>/dev/null || echo python)
CONDA_BIN := $(dir $(CONDA_PYTHON))

lint:
	@$(CONDA_BIN)flake8 src/ exploratory/ tests/
	@$(CONDA_BIN)pylint src/openpois/

# Build the site for production
site_build:
	@cd site && npm run build;

# Serve the site locally with hot reload
# Note: does not build Sphinx docs; use site_preview for a full build
site_dev:
	@cd site && npm run dev;

# Generate site/public/taxonomy.html from the conflation data CSVs
# Requires the openpois conda env to be active (for pandas)
build_taxonomy:
	@python scripts/build_taxonomy.py;

# Full build + local preview: Sphinx docs, Vite production build, then serve
# Mirrors the GitHub Actions workflow; serves at http://localhost:4173
# Requires the openpois conda env to be active (for sphinx-build)
# Uses Python's HTTP server instead of vite preview so /docs/ is served
# correctly (vite preview uses SPA fallback which swallows directory requests)
site_preview:
	@python scripts/build_taxonomy.py
	@sphinx-build -b html docs docs/_build/html -q
	@cd site && npm run build
	@cp -r docs/_build/html site/dist/docs
	@python -m http.server 4173 --directory site/dist;

# Convenience target to print all of the available targets in this file
# From https://stackoverflow.com/questions/4219255
.PHONY: list
list:
	@LC_ALL=C $(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | \
		awk -v RS= -F: '/^# File/,/^# Finished Make data base/ \
		{if ($$1 !~ "^[#.]") {print $$1}}' | \
		sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'
