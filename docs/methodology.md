# Benchmark Methodology

This document describes the methodology, rationale, and best practices for benchmarking PostgreSQL read strategies in the SQL Throughput Challenge.

## Table of Contents

- [Overview](#overview)
- [Hardware & Environment](#hardware--environment)
- [Standardized Execution Environment](#standardized-execution-environment)
- [Benchmark Design](#benchmark-design)
- [Metrics & Measurement](#metrics--measurement)
- [Strategy Comparison](#strategy-comparison)
- [Reproducibility](#reproducibility)
- [Interpreting Results](#interpreting-results)

---

## Executive Summary

**For non-technical stakeholders:**

This project answers: *"What's the fastest way to read large amounts of data from a database in Python?"*

We tested 5 different approaches and found:
- **Best overall performance:** Multiprocessing (~68,871 rows/s) - distributes work across CPU cores
- **Best memory efficiency:** Async streaming (85 MB vs 1,796 MB for naive) - uses 95% less memory than basic approach
- **Simplest option:** Naive fetching (~41,337 rows/s) - fine for small datasets under 10K rows

The results help teams choose the right approach based on their constraints (speed vs. memory vs. code complexity).

---

## Overview

The SQL Throughput Challenge benchmarks different Python strategies for reading large datasets from PostgreSQL. The goal is to demonstrate:

1. **Performance characteristics** of various approaches (naive, paginated, pooled, parallel, async)
2. **Resource utilization** (memory, CPU) under different workloads
3. **Trade-offs** between throughput, latency, and resource consumption
4. **Best practices** for database interaction in Python backend systems

### Key Objectives

- **Reproducibility**: All benchmarks should produce consistent results across runs
- **Fairness**: Each strategy operates on the same dataset with identical database state
- **Realism**: Test scenarios reflect real-world use cases (ETL, reporting, bulk processing)
- **Transparency**: All methodology decisions and limitations are documented

---

## Hardware & Environment

### Recommended Specifications

For consistent benchmarking results:

- **CPU**: 4+ cores (modern x86_64 or ARM64)
- **RAM**: 8GB+ (to avoid swapping during benchmarks)
- **Storage**: SSD recommended (reduces I/O bottlenecks)
- **OS**: Linux, macOS, or Windows with Docker support

### Software Stack

- **Python**: 3.10+ (type hints, structural pattern matching)
- **PostgreSQL**: 16.x (latest stable with performance improvements)
- **psycopg**: 3.1+ (modern sync driver)
- **asyncpg**: 0.29+ (high-performance async driver)

### Docker Configuration

The `docker-compose.yml` configuration uses:
- PostgreSQL 16 Alpine (minimal footprint)
- Default PostgreSQL settings (no tuning for reproducibility)
- Named volume for data persistence
- Health checks for readiness detection

**Note**: Default PostgreSQL settings are intentionally used to simulate a "typical" deployment. For production optimization, consider:
- `shared_buffers` (25% of RAM)
- `work_mem` (query memory per operation)
- `effective_cache_size` (OS cache hint)
- `max_connections` (connection limit)

---

## Standardized Execution Environment

For **reproducible benchmark results**, the project provides a containerized benchmark environment with fixed resource constraints via Docker Compose.

### Resource Allocation

| Component   | CPUs | Memory |
|-------------|------|--------|
| PostgreSQL  | 2.0  | 2 GB   |
| Benchmark   | 2.0  | 4 GB   |
| **Total**   | 4.0  | 6 GB   |

### Running Containerized Benchmarks

```bash
# Using Make (recommended)
make benchmark ROWS=100000 RUNS=3

# Or using Docker Compose directly
docker-compose -f docker-compose.yml -f docker-compose.benchmark.yml up -d postgres
docker-compose -f docker-compose.yml -f docker-compose.benchmark.yml run --rm benchmark \
    python -m src.main run --strategy all --rows 100000 --runs 3
```

The containerized benchmark ensures consistent resource allocation across different host machines, making results comparable and reproducible.

---

## Benchmark Design

### Dataset Characteristics

The synthetic dataset is designed to be:

1. **Deterministic**: Fixed random seed (default: 42) ensures reproducibility
2. **Representative**: Includes typical data types (timestamps, JSON, numerics, booleans, text)
3. **Realistic size**: Default 1M rows (~500MB) simulates medium-scale reporting queries
4. **Indexed**: Primary key (id) + secondary indexes on common filter columns

#### Schema

```sql
CREATE TABLE public.records (
    id            BIGSERIAL PRIMARY KEY,           -- Sequential ID
    created_at    TIMESTAMPTZ NOT NULL,            -- Timestamp with timezone
    updated_at    TIMESTAMPTZ NOT NULL,            -- Auto-updated via trigger
    category      TEXT NOT NULL,                   -- Categorical data (4 values)
    payload       JSONB NOT NULL,                  -- Semi-structured data
    amount        NUMERIC(12,2) NOT NULL,          -- Decimal precision
    is_active     BOOLEAN NOT NULL DEFAULT TRUE,   -- Boolean flag
    source        TEXT NOT NULL DEFAULT 'generator'-- Origin tracking
);

CREATE INDEX idx_records_created_at ON public.records (created_at);
CREATE INDEX idx_records_category    ON public.records (category);
CREATE INDEX idx_records_is_active   ON public.records (is_active);
```

### Data Generation Strategy

Data is generated via `scripts/generate_data.py`:

1. **CSV buffering**: Rows are written to CSV in batches (default: 10,000)
2. **Bulk loading**: PostgreSQL `COPY` command loads CSV (10-50x faster than INSERTs)
3. **Consistent payload**: JSON payloads contain realistic nested structures

### Benchmark Scenarios

#### Small (Smoke Tests)
- **Rows**: 1,000 - 10,000
- **Purpose**: Quick validation, CI/CD pipelines
- **Duration**: < 5 seconds per strategy

#### Medium (Development)
- **Rows**: 100,000
- **Purpose**: Local development, feature testing
- **Duration**: 10-30 seconds per strategy

#### Large (Production-like)
- **Rows**: 1,000,000+
- **Purpose**: Performance profiling, portfolio demonstration
- **Duration**: 1-5 minutes per strategy

---

## Metrics & Measurement

### Primary Metrics

| Metric | Description | Unit | Collection Method |
|--------|-------------|------|-------------------|
| **Duration** | Wall-clock time | seconds | `time.perf_counter()` |
| **Throughput** | Rows processed per second | rows/s | `rows / duration` |
| **Peak RSS** | Maximum resident set size | bytes | `psutil` background sampling |
| **Peak Traced Memory** | Python allocation peak | bytes | `tracemalloc` |
| **CPU %** | CPU utilization | percent | `psutil.Process.cpu_percent()` |

### Secondary Metrics

- **Query count**: Number of database round-trips
- **Connection pool stats**: Active/idle connections
- **Network I/O**: Bytes transferred (if applicable)

### Measurement Tools

#### Profiler (`src/utils/profiler.py`)

The profiler uses:

1. **High-resolution timer**: `time.perf_counter()` for nanosecond precision
2. **Background sampling**: Thread-based RSS sampling at 50ms intervals
3. **tracemalloc**: Python-level memory allocation tracking
4. **psutil**: OS-level process metrics

```python
with profile_block("strategy-name") as stats:
    strategy.execute(limit=1_000_000)

print(f"Duration: {stats.duration_seconds:.2f}s")
print(f"Peak RSS: {stats.peak_rss_bytes / 1024**2:.1f} MB")
print(f"Throughput: {rows / stats.duration_seconds:,.0f} rows/s")
```

#### Statistical Aggregation

For robust results, run each strategy multiple times (default: `--runs 3`):

- **Median**: Robust central tendency (less affected by outliers)
- **Mean**: Average performance
- **StdDev**: Consistency indicator (lower = more consistent)
- **Min/Max**: Range of observed values

---

## Strategy Comparison

### 1. Naive (Baseline)

**Description**: Single `SELECT * ... LIMIT N` with `fetchall()`.

**Characteristics**:
- ✅ Simplest implementation
- ❌ Loads entire result set into memory
- ❌ Single connection, no concurrency
- ❌ Poor memory efficiency

**Use Case**: Small result sets (< 10K rows), simplicity over performance.

**Expected Performance**:
- Throughput: Moderate (single-threaded)
- Memory: High (linear with result set size)

---

### 2. Cursor Pagination

**Description**: Server-side cursor with `fetchmany(batch_size)`.

**Characteristics**:
- ✅ Memory-efficient streaming
- ✅ Configurable batch size
- ❌ Single connection
- ⚠️  Requires transaction to keep cursor alive

**Use Case**: Memory-constrained environments, large result sets.

**Expected Performance**:
- Throughput: Moderate (single-threaded)
- Memory: Low (constant with batch size)

**Configuration**:
```python
CursorPaginationStrategy(batch_size=10_000)
```

---

### 3. Pooled Sync

**Description**: Connection pool + batched fetching.

**Characteristics**:
- ✅ Reusable connections (reduces overhead)
- ✅ Memory-efficient
- ⚠️  Pool contention with high concurrency
- ❌ Still single-threaded execution

**Use Case**: Applications with repeated queries, connection overhead reduction.

**Expected Performance**:
- Throughput: Moderate to high (reduced connection overhead)
- Memory: Low (constant with batch size)

**Configuration**:
```python
PooledSyncStrategy(
    batch_size=10_000,
    pool_min_size=2,
    pool_max_size=10
)
```

---

### 4. Multiprocessing

**Description**: Process pool with ID-range partitioning.

**Characteristics**:
- ✅ True parallelism (bypasses GIL)
- ✅ Scalable with CPU cores
- ❌ Higher memory (per-process overhead)
- ❌ Process spawn overhead
- ⚠️  Requires picklable functions

**Use Case**: CPU-bound processing, bulk ETL, multi-core servers.

**Expected Performance**:
- Throughput: High (scales with cores)
- Memory: High (per-process overhead)

**Configuration**:
```python
MultiprocessingStrategy(
    processes=4,
    chunk_size=50_000
)
```

**Trade-offs**:
- Optimal `processes`: 80-90% of CPU cores (leave headroom)
- Optimal `chunk_size`: Balance between overhead and parallelism

---

### 5. Async Stream

**Description**: asyncpg with asynchronous cursor streaming.

**Characteristics**:
- ✅ Non-blocking I/O
- ✅ High concurrency potential
- ✅ Memory-efficient
- ❌ Requires async-aware code
- ⚠️  Single event loop (limited by I/O, not CPU)

**Use Case**: I/O-bound workloads, high-concurrency services, modern async stacks.

**Expected Performance**:
- Throughput: High (for I/O-bound)
- Memory: Low (streaming)

**Configuration**:
```python
AsyncStreamStrategy(batch_size=10_000)
```

---

## Reproducibility

### Controlling Variability

1. **Database state**: Always seed fresh data before benchmarks
2. **Cache effects**: Run warmup iteration (`--warmup`) or restart Postgres
3. **Background load**: Minimize other processes during benchmarking
4. **Network**: Use localhost to eliminate network variability
5. **Random seed**: Fixed seed (42) for data generation

### Cache Considerations

PostgreSQL caching affects results:

- **Cold cache**: First query after restart (realistic for batch jobs)
- **Warm cache**: Subsequent queries (data in `shared_buffers`)
- **Hot cache**: Data in OS page cache

**Recommendation**: For fairness, either:
- Restart Postgres between strategies (cold cache)
- Run warmup iteration for all strategies (warm cache)

### Running Reproducible Benchmarks

```bash
# Run containerized benchmark with multiple runs
make benchmark ROWS=1000000 RUNS=3

# View results
cat results/latest.json | jq '.results[] | {strategy, throughput: .throughput_rows_per_sec}'
```

Note: The containerized benchmark automatically:
- Starts PostgreSQL with resource constraints
- Seeds the database with specified rows
- Executes all strategies (or specific strategy if `STRATEGY=` is set)
- Cleans up containers after completion

---

## Interpreting Results

### Understanding Trade-offs

| Priority | Choose Strategy |
|----------|-----------------|
| **Simplicity** | Naive |
| **Memory efficiency** | Cursor Pagination, Async Stream |
| **Throughput (CPU-bound)** | Multiprocessing |
| **Throughput (I/O-bound)** | Async Stream |
| **Connection reuse** | Pooled Sync |

### Red Flags

- **Throughput < 1,000 rows/s**: Check network, indexes, query plan
- **Memory > 2x dataset size**: Memory leak or inefficient fetching
- **CPU > 200%** (single strategy): Unexpected parallelism or contention
- **High stddev (> 20%)**: Inconsistent environment or cache effects

### Example Analysis

```json
{
  "strategy": "multiprocessing",
  "duration_seconds": {
    "median": 12.5,
    "mean": 12.8,
    "stddev": 0.6
  },
  "throughput_rows_per_sec": {
    "median": 80000,
    "mean": 78125,
    "stddev": 3750
  }
}
```

**Interpretation**:
- Median duration: 12.5s (robust metric)
- Low stddev: Consistent performance
- Throughput: 80K rows/s (good for 4-core CPU)

---

## Limitations

1. **Read-only**: No write benchmark scenarios
2. **Single table**: No join or aggregation benchmarks
3. **Localhost only**: No network latency simulation
4. **Fixed schema**: No variable column width/cardinality tests