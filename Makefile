export JAVA_HOME := $(shell /usr/libexec/java_home -v 21)
PYTHON ?= python3.13
PY := .venv/bin/python
TLC := https://d37ci6vzurychx.cloudfront.net

all: results/bench.csv up

.venv:
	$(PYTHON) -m venv .venv
	.venv/bin/pip install -q pyspark==4.0.1

data/raw/taxi_zone_lookup.csv:
	mkdir -p data/raw
	set -e; for m in 01 02 03 04 05 06; do \
		curl -sfL -o data/raw/yellow_tripdata_2024-$$m.parquet $(TLC)/trip-data/yellow_tripdata_2024-$$m.parquet; \
	done
	curl -sfL -o $@ $(TLC)/misc/taxi_zone_lookup.csv

data/parquet/_SUCCESS: data/raw/taxi_zone_lookup.csv | .venv
	$(PY) scripts/prepare.py

results/bench.csv: data/parquet/_SUCCESS
	$(PY) scripts/bench.py

up:
	docker compose up -d

.PHONY: all up
