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

# Convenience target to print all of the available targets in this file
# From https://stackoverflow.com/questions/4219255
.PHONY: list
list:
	@LC_ALL=C $(MAKE) -pRrq -f $(lastword $(MAKEFILE_LIST)) : 2>/dev/null | \
		awk -v RS= -F: '/^# File/,/^# Finished Make data base/ \
		{if ($$1 !~ "^[#.]") {print $$1}}' | \
		sort | egrep -v -e '^[^[:alnum:]]' -e '^$@$$'
