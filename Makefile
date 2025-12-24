SHELL := /bin/sh

COMPOSE := docker-compose
COMPOSE_BENCHMARK := docker-compose -f docker-compose.yml -f docker-compose.benchmark.yml

ROWS ?= 100000
RUNS ?= 1
STRATEGY ?= all

.PHONY: help
help:
	@echo "SQL Throughput Challenge - Makefile"
	@echo ""
	@echo "Primary Workflows:"
	@echo "  benchmark        Run containerized benchmark (recommended)"
	@echo "  up               Start PostgreSQL"
	@echo "  down             Stop PostgreSQL"
	@echo "  clean            Remove containers and volumes"
	@echo ""
	@echo "Parameters:"
	@echo "  ROWS=N           Number of rows (default: 100000)"
	@echo "  RUNS=N           Number of runs (default: 1)"
	@echo "  STRATEGY=name    Strategy to run (default: all)"
	@echo ""
	@echo "Examples:"
	@echo "  make benchmark ROWS=100000"
	@echo "  make benchmark ROWS=500000 RUNS=3 STRATEGY=async_stream"

# =============================================================================
# Benchmarking (Containerized)
# =============================================================================

.PHONY: benchmark
benchmark:
	$(COMPOSE_BENCHMARK) build benchmark
	$(COMPOSE_BENCHMARK) up -d postgres
	@until $(COMPOSE_BENCHMARK) exec -T postgres pg_isready -U postgres -d throughput_challenge 2>/dev/null; do sleep 1; done
	$(COMPOSE_BENCHMARK) run --rm --entrypoint python benchmark scripts/generate_data.py --rows $(ROWS)
	$(COMPOSE_BENCHMARK) run --rm benchmark run --strategy $(STRATEGY) --rows $(ROWS) --runs $(RUNS)
	$(COMPOSE_BENCHMARK) down

# =============================================================================
# Docker Management
# =============================================================================

.PHONY: up
up:
	$(COMPOSE) up -d

.PHONY: down
down:
	$(COMPOSE) down
	-$(COMPOSE_BENCHMARK) down 2>/dev/null

.PHONY: clean
clean:
	$(COMPOSE) down --volumes --remove-orphans
	-$(COMPOSE_BENCHMARK) down --volumes --remove-orphans 2>/dev/null
	-docker rmi sql-benchmark-runner:latest 2>/dev/null
	rm -rf results/*.json __pycache__ .pytest_cache .mypy_cache .ruff_cache
