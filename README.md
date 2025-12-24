![CI](https://github.com/aluiziolira/sql-throughput-challenge/workflows/CI/badge.svg)
![Python 3.10+](https://img.shields.io/badge/python-3.10%2B-blue)
![License: MIT](https://img.shields.io/badge/License-MIT-green.svg)

# SQL Throughput Challenge

Benchmarking suite comparing Python strategies for bulk reads from PostgreSQL. Demonstrates connection pooling, async I/O, multiprocessing, and memory-efficient streaming.

## Quick Start

**Requirements:** Docker and Docker Compose only. No Python setup needed.

```bash
# Run all benchmarks with 100K rows (results displayed automatically)
make benchmark ROWS=100000
```

Results are displayed as a formatted table upon completion. Raw JSON is also saved to `results/latest.json` for programmatic access.

## Strategy Comparison

| Strategy | Concurrency | Memory | Best For |
|----------|-------------|--------|----------|
| **Naive** | Single-threaded | High (loads all) | Small datasets, simplicity |
| **Cursor Pagination** | Single-threaded | Low (streaming) | Memory-constrained environments |
| **Pooled Sync** | Single-threaded | Low (streaming) | Connection reuse |
| **Multiprocessing** | Multi-process | High (per-process) | CPU-bound, multi-core |
| **Async Stream** | Async I/O | Low (streaming) | I/O-bound, high concurrency |

## Key Results

Benchmark results from 500K rows (3 runs, median values):

| Strategy | Throughput | Peak Memory | Best For |
|----------|------------|-------------|----------|
| **Multiprocessing** | ~68,871 rows/s | 93 MB | CPU-bound bulk ETL, maximum throughput |
| **Async Stream** | ~51,161 rows/s | 85 MB | Memory-constrained I/O, high concurrency |
| **Naive** | ~41,337 rows/s | 1,796 MB | Small datasets only (memory intensive) |
| **Pooled Sync** | ~32,387 rows/s | 855 MB | Connection reuse with moderate memory |
| **Cursor Pagination** | ~31,122 rows/s | 119 MB | Large result streaming, minimal memory |

> **Key Insight:** Multiprocessing achieves 2.2x higher throughput than async streaming and 2.2x faster than single-threaded approaches, while async streaming uses 95% less memory than naive fetching (85 MB vs 1,796 MB), making it ideal for memory-constrained environments.

## Benchmark Visualizations

### Throughput Comparison

#### 100K Rows
![Benchmark 100K](docs/images/benchmark-100k.png)

#### 500K Rows
![Benchmark 500K](docs/images/benchmark-500k.png)

#### 1M Rows
![Benchmark 1M](docs/images/benchmark-1m.png)

## Make Targets

| Target | Description |
|--------|-------------|
| `make benchmark` | Run containerized benchmark (all strategies) |
| `make up` | Start PostgreSQL container |
| `make down` | Stop PostgreSQL container |
| `make clean` | Remove containers and volumes |

### Customization

Customize benchmark runs with parameters:

```bash
# Run specific strategy
make benchmark STRATEGY=async_stream

# Run with different dataset sizes
make benchmark ROWS=500000

# Multiple runs for statistical aggregation
make benchmark ROWS=100000 RUNS=3

# Combine parameters
make benchmark ROWS=1000000 RUNS=5 STRATEGY=multiprocessing
```

## Features

- **5 Benchmark Strategies**: Naive, cursor pagination, connection pooling, multiprocessing, async streaming
- **Comprehensive Profiling**: Wall-clock time, peak RSS, tracemalloc, CPU utilization
- **Statistical Aggregation**: Multiple runs with median/mean/stddev
- **Reproducible Environment**: Docker containers with fixed CPU/memory constraints
- **CI/CD Pipeline**: GitHub Actions for linting, type checking, and testing

## Project Structure

```
├── src/
│   ├── strategies/          # Benchmark implementations
│   │   ├── naive.py
│   │   ├── cursor_pagination.py
│   │   ├── pooled_sync.py
│   │   ├── multiprocessing.py
│   │   └── async_stream.py
│   ├── orchestrator.py      # Strategy execution + profiling
│   ├── reporter.py          # Results formatting
│   └── utils/profiler.py    # Metrics collection
├── db/init.sql              # Database schema
├── scripts/generate_data.py # Data seeding
├── tests/                   # Unit + integration tests
└── docs/methodology.md      # Detailed benchmark methodology
```

## Understanding Results

### Single Run Output

```json
{
  "strategy": "async_stream",
  "rows": 100000,
  "duration_seconds": 2.34,
  "throughput_rows_per_sec": 42735.04,
  "peak_rss_bytes": 52428800,
  "cpu_percent": 85.3
}
```

### Multiple Runs Output (Statistical Aggregation)

```json
{
  "strategy": "async_stream",
  "rows": 100000,
  "runs": 3,
  "duration_seconds": {
    "median": 2.34,
    "mean": 2.35,
    "stddev": 0.02
  },
  "throughput_rows_per_sec": {
    "median": 42735.04,
    "mean": 42553.19,
    "stddev": 364.31
  }
}
```

### Querying Results (Optional)

For programmatic access, raw JSON is saved to `results/latest.json`. If you have [`jq`](https://jqlang.github.io/jq/) installed:

```bash
# Single run results
cat results/latest.json | jq '.results[] | {strategy, throughput: .throughput_rows_per_sec}'

# Aggregated results (multiple runs)
cat results/latest.json | jq '.results[] | {strategy, throughput: .throughput_rows_per_sec.median}'
```

## Documentation

- [Benchmark Methodology](docs/methodology.md) — Detailed explanation of metrics, statistical approach, and reproducibility

## Tech Stack

**Core Technologies:**
- Python 3.11
- PostgreSQL 16
- Docker & Docker Compose

**Python Libraries:**
- `psycopg3` — Synchronous PostgreSQL adapter with connection pooling
- `asyncpg` — High-performance async PostgreSQL driver
- `typer` — CLI framework
- `pydantic` — Configuration management
- `rich` — Terminal output formatting
- `psutil` — System resource monitoring

**Development Tools:**
- `pytest` — Testing framework
- `ruff` — Linter and formatter
- `mypy` — Static type checker
- GitHub Actions — CI/CD pipeline

## CI/CD Pipeline

The project includes comprehensive GitHub Actions workflows:

- **CI Workflow** (`ci.yml`)
  - Code quality checks (Ruff linting + formatting)
  - Type checking (MyPy)
  - Full test suite (unit + integration)
  - Matrix testing across Python 3.10, 3.11, 3.12
  - Smoke benchmarks on pull requests

- **Benchmark Workflow** (`benchmark.yml`)
  - Manual and tag-triggered benchmark runs
  - Configurable parameters (rows, strategy, runs)
  - Artifact uploading for result analysis

## License

MIT