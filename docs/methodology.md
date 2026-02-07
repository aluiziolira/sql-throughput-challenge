# Benchmark Internals & Methodology

This document details the technical design and measurement mechanics of the SQL Throughput Challenge. It functions as a lab manual for developers looking to understand, replicate, or modify the benchmark suite.

## 1. Experiment Design

### Dataset Characteristics

The benchmark operates on a synthetic table designed to mimic production reporting workloads. To ensure distinct performance characteristics across strategies, the schema includes mixed data types rather than simple integers.

- **Scale:** 100K to 1M+ rows (default).
- **Determinism:** Data generation uses fixed RNG seeds (`seed=42`) ensuring identical datasets across runs.
- **Complexity:**
  - `JSONB` payloads (semi-structured data parsing cost).
  - `TIMESTAMPTZ` (timezone-aware parsing cost).
  - `NUMERIC` (high-precision decimal handling).
  - `TEXT` categorical data.

**Loading Strategy:**
Data is generated in Python but loaded via PostgreSQL `COPY FROM STDIN` (binary/CSV) rather than individual `INSERT` statements, effectively isolating the **read** benchmark from write performance variability.

### Limitations

- **Localhost Limits:** Network latency is negligible (container-to-container), emphasizing driver/deserialization CPU overhead over network I/O.
- **Read-Only:** Pure throughput test; does not measure write contention or locking.
- **Single Table:** No `JOIN` overhead; measures raw row fetching speed.

---

## 2. Measurement System

The project uses a custom profiler (`src/utils/profiler.py`) designed to catch transient spikes often missed by simple start/end snapshots.

### Metrics Captured

1. **Wall-Clock Duration:** `time.perf_counter()` (nanosecond precision).
2. **Peak RSS (OS Memory):**
    - Measured via a **daemon thread** polling `psutil.Process.memory_info()` every **50ms**.
    - *Why:* Batch processing creates "sawtooth" memory patterns. Snapshotting at the end often reports the "low" point (after GC), missing the true peak.
3. **CPU Utilization:** `psutil.cpu_percent()` aggregated across the main process and all child workers (essential for Multiprocessing strategy).
4. **Allocations (Python):** `tracemalloc` (optional) to track internal Python object overhead.

### Statistical Aggregation

Single runs are noisy due to OS scheduling and JIT warmups. We employ:

- **Median** (primary metric) to filter outliers.
- **Standard Deviation** to detect stability issues.
- **Warmup Runs** (optional via `--warmup`) to prime OS page cache and Postgres shared buffers.

### Statistical Caveats

The default configuration uses **5 runs** per strategy to provide a reasonable balance between statistical confidence and execution time. However, this should be considered a minimum:

- **3 runs** provides limited statistical confidence and may not adequately capture variance from OS scheduling, background processes, or transient system load. Results from 3 runs should be treated as preliminary and may vary significantly between executions.
- **5+ runs** are recommended for publishable numbers or when making performance comparisons between strategies. This provides better outlier detection and more reliable median/standard deviation metrics.
- For rigorous academic or production benchmarking, consider **10+ runs** to achieve tighter confidence intervals.

When comparing strategies, ensure both use the same number of runs and that `--warmup` is enabled to minimize cold-start effects.

### Strategy Lifecycle Hygiene

Strategy instances are created per warmup/measurement run and executed by the orchestrator. After each run, the orchestrator performs best-effort cleanup by calling a strategy `close()` method when that method exists.

- This keeps resource ownership explicit for strategies that manage pools or other long-lived handles.
- Cleanup is attempted in a safe `finally` path, so it also runs when strategy execution fails.
- Strategies that do not own external resources are not required to implement `close()`.

### Run-Level Failure Policy

Orchestrator execution supports a run-level failure policy via `RunConfig.failure_policy`:

- `tolerant` (default): strategy execution exceptions are captured into the result payload (`error`, plus failure metadata under `extra`) so remaining runs/strategies continue.
- `strict`: strategy execution exceptions are re-raised immediately (fail-fast), stopping execution at the first measurement failure.

The default remains `tolerant` for backward compatibility with existing CLI and benchmark workflows.

---

## 3. Implementation Specifications

This section defines the precise drivers, libraries, and SQL commands used by each strategy. For high-level architectural patterns (like the "Two-Phase Fan-Out"), refer to the [Strategy Mechanics](../README.md#strategy-mechanics) section in the README.

### Naive

- **Mechanism:** `cursor.fetchall()` loading all rows into a single Python list.
- **Bottleneck:** Memory Pressure. Large datasets trigger OS swapping or OOM kills.
- **Driver:** `psycopg3`.

### Cursor Pagination

- **Mechanism:** Server-side named cursor (`DECLARE ... CURSOR`).
- **Flow:** Fetches constant `batch_size` (default 10k) chunks.
- **Constraint:** Requires a long-lived transaction (`state=idle_in_transaction`) to keep the cursor valid.
- **Bottleneck:** Network round-trips and serialized processing.

### Pooled Sync

- **Mechanism:** Connection pool (`psycopg_pool.ConnectionPool`) checking out connections for short bursts.
- **Optimization:** Reduces TCP handshake overhead, though less effective for a single long-running bulk read than for many short queries.

### Multiprocessing

- **Mechanism:** Parallel worker pool using `multiprocessing.get_context("spawn")`.
- **Partitioning:** ID chunking based on `SELECT id ... ORDER BY id LIMIT N`.
  - *Why:* Avoids `OFFSET` scans and does not assume contiguous IDs. Each worker fetches `WHERE id = ANY(...)` over its chunk.
- **Process Model:** Uses `spawn` instead of `fork` for thread safety and cross-platform compatibility (Windows/macOS), keeping interactions isolated.

### Async Stream

- **Mechanism:** `asyncpg` connection pool with concurrent cursor fetching.
- **Driver Choice:** Uses `asyncpg` over `psycopg3` (async mode) due to its native binary protocol implementation.
- **Flow:**
  - **Single Concurrency:** Simple asynchronous iteration via `cursor.__aiter__`.
  - **High Concurrency:** Implements the same **Two-Phase Partitioning** as Multiprocessing (fetch IDs -> fan-out queries) to utilize multiple connections in the event loop.
  - **Bounded-ID Guardrail:** For large limits, concurrent mode selects IDs in fixed windows (default threshold: `50_000`, window size: `20_000`) and processes each window before selecting the next, reducing avoidable in-memory ID buildup while preserving behavior for small/medium limits.

### Statement Timeouts

- **Mechanism:** Optional `SET LOCAL statement_timeout = <ms>` per transaction.
- **Control:** Configure via `DB_STATEMENT_TIMEOUT_MS` (0 disables).

---

## 4. Controlled Environment

Reproducibility is enforced via Docker Compose resource limits.

| Component         | CPU Limit | RAM Limit | Rationale                                             |
| :---------------- | :-------- | :-------- | :---------------------------------------------------- |
| **Benchmark App** | 4.0 Cores | 4 GB      | Prevents infinite parallel scaling; limits heap size. |
| **PostgreSQL**    | 2.0 Cores | 2 GB      | Simulates a constrained DB instance.                  |

**Database Tuning:**
The Benchmark container applies modest tuning to the Postgres instance (`shared_buffers=512MB`, `work_mem=64MB`) to simulate a configured production environment rather than a raw default installation. Also explicitly disables JIT (`jit=off`) to avoid overhead on short query streams.

---

## 5. Running & Verifying

To run the full suite under these strict constraints:

```bash
# Full benchmark (all strategies, 100k rows)
make benchmark ROWS=100000

# Targeted debug run
make benchmark STRATEGY=multiprocessing ROWS=10000 RUNS=1
```

Results are dumped to `results/latest.json` containing the raw raw metrics for every run, allowing for external analysis.

---

## 6. Benchmark Comparability Assumptions

When interpreting benchmark results across strategies, the following assumptions ensure fair comparison:

### Row Count Equivalence

All strategies return the **same row count** for a given `limit` parameter. The benchmark validates this by counting rows returned by each strategy and ensuring they match. This guarantees that throughput differences reflect genuine performance variations, not data volume differences.

### Non-Contiguous ID Handling

The **Multiprocessing** strategy uses ID-based chunking rather than range assumptions:

- It first queries `SELECT id ... ORDER BY id LIMIT N` to get the exact IDs
- Workers fetch `WHERE id = ANY(...)` over their assigned chunk
- This approach works correctly even with gaps in the ID sequence (e.g., after deletions)
- No assumption is made that IDs are contiguous or start at 1

### Timeout Behavior Impact

When `DB_STATEMENT_TIMEOUT_MS` is configured:

- Each query within a strategy is subject to the timeout
- Strategies with multiple queries (e.g., Multiprocessing with many chunks) may encounter timeouts on individual chunks while others succeed
- In `tolerant` mode (default), timeout exceptions are captured in run results (`error` + failure metadata) so remaining runs/strategies continue
- In `strict` mode, timeout exceptions are re-raised immediately and fail fast
- For fair comparison, use the same timeout value (or disable timeouts) when comparing strategies

These assumptions ensure that benchmark results reflect genuine architectural differences between strategies rather than data or configuration artifacts.
