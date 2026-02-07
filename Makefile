.DEFAULT_GOAL := help

# =============================================================================
# Variables
# =============================================================================

SHELL := /bin/sh

# Docker Compose commands
COMPOSE := docker-compose
COMPOSE_BENCHMARK := docker-compose -f docker-compose.yml -f docker-compose.benchmark.yml

# Benchmark Configuration
ROWS ?= 100000
RUNS ?= 1
STRATEGY ?= all
CONCURRENCY ?=
WARMUP ?=

# Colors for output
RESET := \033[0m
BOLD := \033[1m
GREEN := \033[32m
YELLOW := \033[33m
CYAN := \033[36m

# =============================================================================
# Help
# =============================================================================

.PHONY: help
help: ## Display this help message
	@echo ""
	@echo "  $(BOLD)SQL Throughput Challenge - Makefile$(RESET)"
	@echo ""
	@echo "  $(YELLOW)Usage:$(RESET)"
	@echo "    make $(CYAN)<target>$(RESET)"
	@echo ""
	@echo "  $(YELLOW)Primary Workflows:$(RESET)"
	@grep -E '^[a-zA-Z0-9_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    $(CYAN)%-20s$(RESET) %s\n", $$1, $$2}'
	@echo ""
	@echo "  $(YELLOW)Benchmark Parameters:$(RESET)"
	@echo "    $(GREEN)ROWS$(RESET)         Number of rows (default: 100000)"
	@echo "    $(GREEN)RUNS$(RESET)         Number of runs (default: 1)"
	@echo "    $(GREEN)STRATEGY$(RESET)     Strategy to run (default: all)"
	@echo "    $(GREEN)CONCURRENCY$(RESET)  Concurrency level (optional)"
	@echo "    $(GREEN)WARMUP$(RESET)       Enable warmup: WARMUP=1 (optional)"
	@echo ""
	@echo "  $(YELLOW)Examples:$(RESET)"
	@echo "    make benchmark ROWS=100000"
	@echo "    make benchmark ROWS=500000 RUNS=3 STRATEGY=async_stream"
	@echo ""

# =============================================================================
# Development
# =============================================================================

.PHONY: install
install: ## Install dependencies using Poetry
	@echo "$(BOLD)Installing dependencies with Poetry...$(RESET)"
	poetry install

.PHONY: lint
lint: ## Run code quality checks (ruff, mypy)
	@echo "$(BOLD)Running linters...$(RESET)"
	poetry run ruff check src/ tests/
	poetry run ruff format --check src/ tests/
	poetry run mypy src/ --strict

.PHONY: format
format: ## Format code and fix linting issues
	@echo "$(BOLD)Formatting code...$(RESET)"
	poetry run ruff format src/ tests/
	poetry run ruff check --fix src/ tests/

.PHONY: test
test: ## Run full test suite (unit + integration)
	@echo "$(BOLD)Starting test environment...$(RESET)"
	$(COMPOSE) up -d postgres
	@echo "Waiting for PostgreSQL..."
	@until $(COMPOSE) exec -T postgres pg_isready -U postgres -d throughput_challenge 2>/dev/null; do sleep 1; done
	@echo "Seeding test database..."
	$(COMPOSE) exec -T postgres psql -U postgres -d throughput_challenge -c \
		"CREATE TABLE IF NOT EXISTS records (id SERIAL PRIMARY KEY, data TEXT, created_at TIMESTAMP DEFAULT NOW());" 2>/dev/null || true
	$(COMPOSE) exec -T postgres psql -U postgres -d throughput_challenge -c \
		"INSERT INTO records (data) SELECT 'test_data_' || generate_series(1, 1000) ON CONFLICT DO NOTHING;" 2>/dev/null || true
	@echo "$(BOLD)Running tests...$(RESET)"
	RUN_INTEGRATION_TESTS=1 poetry run pytest -v
	@echo "$(GREEN)Tests completed.$(RESET) Database container left running. Use 'make down' to stop."

# =============================================================================
# Benchmarking
# =============================================================================

.PHONY: benchmark
benchmark: ## Run containerized benchmark (recommended)
	@echo "$(BOLD)Building benchmark container...$(RESET)"
	$(COMPOSE_BENCHMARK) build benchmark
	@echo "$(BOLD)Starting database for benchmark...$(RESET)"
	$(COMPOSE_BENCHMARK) up -d postgres
	@until $(COMPOSE_BENCHMARK) exec -T postgres pg_isready -U postgres -d throughput_challenge 2>/dev/null; do sleep 1; done
	@echo "$(BOLD)Generating data ($(ROWS) rows)...$(RESET)"
	$(COMPOSE_BENCHMARK) run --rm --entrypoint python benchmark scripts/generate_data.py --rows $(ROWS)
	@echo "$(BOLD)Running benchmark (Strategy: $(STRATEGY))...$(RESET)"
	$(COMPOSE_BENCHMARK) run --rm benchmark run \
		--strategy $(STRATEGY) \
		--rows $(ROWS) \
		--runs $(RUNS) \
		$(if $(CONCURRENCY),--concurrency $(CONCURRENCY)) \
		$(if $(WARMUP),--warmup)
	@echo "$(BOLD)Cleaning up benchmark resources...$(RESET)"
	$(COMPOSE_BENCHMARK) down

# =============================================================================
# Infrastructure
# =============================================================================

.PHONY: up
up: ## Start local PostgreSQL database
	$(COMPOSE) up -d

.PHONY: down
down: ## Stop all containers and clean up
	$(COMPOSE) down
	-$(COMPOSE_BENCHMARK) down 2>/dev/null

.PHONY: clean
clean: ## Remove all containers, volumes, and temporary files
	@echo "$(BOLD)Cleaning up...$(RESET)"
	$(COMPOSE) down --volumes --remove-orphans
	-$(COMPOSE_BENCHMARK) down --volumes --remove-orphans 2>/dev/null
	-docker rmi sql-benchmark-runner:latest 2>/dev/null
	rm -rf results/*.json __pycache__ .pytest_cache .mypy_cache .ruff_cache
	@echo "$(GREEN)Clean complete.$(RESET)"
