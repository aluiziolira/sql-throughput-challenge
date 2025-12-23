SHELL := /bin/sh
PROJECT_NAME := sql-throughput-challenge
PYTHON := python

ENV_FILE ?= .env
COMPOSE := docker-compose --env-file $(ENV_FILE)
ROWS ?= 100000

.PHONY: help
help:
	@echo "Available targets:"
	@echo "  setup        Install dependencies"
	@echo "  up           Start docker services (Postgres)"
	@echo "  down         Stop docker services"
	@echo "  logs         Tail docker logs"
	@echo "  seed         Generate and load data into Postgres"
	@echo "  benchmark    Run benchmarks (all strategies via CLI)"
	@echo "  test         Run tests (unit + integration)"

.PHONY: setup
setup:
	@echo "Installing dependencies via pip..."
	$(PYTHON) -m pip install --upgrade pip
	$(PYTHON) -m pip install -r requirements.txt || true

.PHONY: up
up:
	$(COMPOSE) up -d

.PHONY: down
down:
	$(COMPOSE) down

.PHONY: logs
logs:
	$(COMPOSE) logs -f

.PHONY: seed
seed:
	$(PYTHON) scripts/generate_data.py --rows $(ROWS)

.PHONY: benchmark
benchmark:
	$(PYTHON) -m src.main run --strategy all --rows $(ROWS)

.PHONY: test
test:
	$(PYTHON) -m pytest -q
