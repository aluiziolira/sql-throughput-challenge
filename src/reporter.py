from __future__ import annotations

import os
from typing import Any

from rich import box
from rich.console import Console
from rich.table import Table

# Constants for cgroup memory limit checks
CGROUP_V1_MEMORY_UNLIMITED = 9223372036854771712
# Expected number of parts in cgroup cpu.max format (quota period)
CGROUP_CPU_MAX_PARTS = 2


def _read_cgroup_v2_cpu() -> str | None:
    """Read CPU limit from cgroup v2."""
    try:
        with open("/sys/fs/cgroup/cpu.max") as f:
            content = f.read().strip()
            parts = content.split()
            if len(parts) == CGROUP_CPU_MAX_PARTS and parts[0] != "max":
                quota = int(parts[0])
                period = int(parts[1])
                cpus = quota / period
                return f"{cpus:.1f}"
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return None


def _read_cgroup_v1_cpu() -> str | None:
    """Read CPU limit from cgroup v1."""
    try:
        with open("/sys/fs/cgroup/cpu/cpu.cfs_quota_us") as f:
            quota = int(f.read().strip())
        with open("/sys/fs/cgroup/cpu/cpu.cfs_period_us") as f:
            period = int(f.read().strip())
        if quota > 0:
            cpus = quota / period
            return f"{cpus:.1f}"
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return None


def _format_memory_bytes(mem_bytes: int) -> str | None:
    """Format memory bytes to human-readable string (GB or MB)."""
    mem_gb = mem_bytes / (1024**3)
    if mem_gb >= 1:
        return f"{mem_gb:.1f}GB"
    mem_mb = mem_bytes / (1024**2)
    return f"{mem_mb:.0f}MB"


def _read_cgroup_v2_memory() -> str | None:
    """Read memory limit from cgroup v2."""
    try:
        with open("/sys/fs/cgroup/memory.max") as f:
            content = f.read().strip()
            if content != "max":
                mem_bytes = int(content)
                return _format_memory_bytes(mem_bytes)
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return None


def _read_cgroup_v1_memory() -> str | None:
    """Read memory limit from cgroup v1."""
    try:
        with open("/sys/fs/cgroup/memory/memory.limit_in_bytes") as f:
            mem_bytes = int(f.read().strip())
            if mem_bytes < CGROUP_V1_MEMORY_UNLIMITED:
                return _format_memory_bytes(mem_bytes)
    except (FileNotFoundError, PermissionError, ValueError):
        pass
    return None


def get_container_resources() -> dict[str, str | None]:
    """
    Get container resource constraints.

    Reads from environment variables or cgroup files when running in a container.
    Returns dict with 'cpus' and 'memory' keys.
    """
    resources: dict[str, str | None] = {"cpus": None, "memory": None}

    # Try environment variables first (set by docker-compose or manually)
    if os.environ.get("BENCHMARK_CPU_LIMIT"):
        resources["cpus"] = os.environ["BENCHMARK_CPU_LIMIT"]
    if os.environ.get("BENCHMARK_MEMORY_LIMIT"):
        resources["memory"] = os.environ["BENCHMARK_MEMORY_LIMIT"]

    # Try reading from cgroup v2 (modern containers)
    if resources["cpus"] is None:
        resources["cpus"] = _read_cgroup_v2_cpu()

    # Try cgroup v1 fallback
    if resources["cpus"] is None:
        resources["cpus"] = _read_cgroup_v1_cpu()

    # Try reading memory limit from cgroup v2
    if resources["memory"] is None:
        resources["memory"] = _read_cgroup_v2_memory()

    # Try cgroup v1 fallback for memory
    if resources["memory"] is None:
        resources["memory"] = _read_cgroup_v1_memory()

    return resources


def _build_resource_info(resources: dict[str, str | None]) -> str:
    """Build resource info string from resources dict."""
    resource_parts = []
    if resources["cpus"]:
        resource_parts.append(f"CPU: {resources['cpus']} cores")
    if resources["memory"]:
        resource_parts.append(f"Memory: {resources['memory']}")
    return " │ ".join(resource_parts)


def _is_aggregated_results(results: list[dict[str, Any]]) -> bool:
    """Check if results are aggregated (multi-run) or single-run."""
    return "runs" in results[0] and isinstance(results[0]["runs"], int) and results[0]["runs"] > 1


def _build_table(is_aggregated: bool, title: str) -> Table:
    """Build and configure the results table."""
    table = Table(
        title=title,
        box=box.ROUNDED,
        caption="Sorted by Throughput (descending)",
    )

    table.add_column("Strategy", style="cyan", no_wrap=True)
    table.add_column("Rows", justify="right", style="magenta")

    if is_aggregated:
        table.add_column("Runs", justify="right", style="blue")
        table.add_column(
            "Duration (s)\n[dim](Median ± StdDev)[/dim]", justify="right", style="green"
        )
        table.add_column(
            "Throughput (rows/s)\n[dim](Median)[/dim]",
            justify="right",
            style="bold green",
        )
        table.add_column("Peak Memory (MB)\n[dim](Median)[/dim]", justify="right", style="yellow")
        table.add_column("CPU %\n[dim](Median)[/dim]", justify="right", style="red")
    else:
        table.add_column("Duration (s)", justify="right", style="green")
        table.add_column("Throughput (rows/s)", justify="right", style="bold green")
        table.add_column("Peak Memory (MB)", justify="right", style="yellow")
        table.add_column("CPU %", justify="right", style="red")

    return table


def _format_aggregated_row(
    res: dict[str, Any],
) -> tuple[str, str, str, str, str, str, str]:
    """Format a single aggregated result row."""
    strategy = res.get("strategy", "Unknown")
    rows = f"{res.get('rows', 0):,}"
    runs = str(res.get("runs", 0))

    dur_median = res["duration_seconds"]["median"]
    dur_std = res["duration_seconds"]["stddev"]
    duration_str = f"{dur_median:.1f} ± {dur_std:.1f}"

    throughput = res["throughput_rows_per_sec"]["median"]
    throughput_str = f"{throughput:,.2f}"

    mem_str = "N/A"
    if "peak_rss_bytes" in res:
        mem_bytes = res["peak_rss_bytes"]["median"]
        mem_mb = mem_bytes / (1024 * 1024)
        mem_str = f"{mem_mb:.2f}"

    cpu_str = "N/A"
    if "cpu_percent" in res:
        cpu = res["cpu_percent"]["median"]
        cpu_str = f"{cpu:.1f}"

    return (strategy, rows, runs, duration_str, throughput_str, mem_str, cpu_str)


def _format_single_row(res: dict[str, Any]) -> tuple[str, str, str, str, str, str]:
    """Format a single-run result row."""
    strategy = res.get("strategy", "Unknown")
    rows = f"{res.get('rows', 0):,}"

    duration = res.get("duration_seconds", 0.0)
    duration_str = f"{duration:.1f}"

    throughput = res.get("throughput_rows_per_sec", 0.0)
    throughput_str = f"{throughput:,.2f}"

    mem_bytes = res.get("peak_rss_bytes") or 0
    mem_mb = mem_bytes / (1024 * 1024)
    mem_str = f"{mem_mb:.2f}"

    cpu = res.get("cpu_percent") or 0.0
    cpu_str = f"{cpu:.1f}"

    return (strategy, rows, duration_str, throughput_str, mem_str, cpu_str)


def print_results(results: list[dict[str, Any]]) -> None:
    """
    Render benchmark results as a rich table.

    Handles both single-run results and aggregated multi-run results.
    Displays container resource constraints when available.
    """
    console = Console()

    if not results:
        console.print("[yellow]No results to display.[/yellow]")
        return

    # Get container resource constraints
    resources = get_container_resources()

    # Determine if we have aggregated results or single run results
    is_aggregated = _is_aggregated_results(results)

    # Build title with resource info
    title = "SQL Throughput Challenge Results"
    resource_info = _build_resource_info(resources)
    if resource_info:
        title = f"{title}\n[dim]Container Resources: {resource_info}[/dim]"

    table = _build_table(is_aggregated, title)

    # Sort results by throughput (descending) to show best performers first
    def get_sort_key(r: dict[str, Any]) -> float:
        if is_aggregated:
            return float(r["throughput_rows_per_sec"]["median"])
        return float(r.get("throughput_rows_per_sec", 0.0))

    sorted_results = sorted(results, key=get_sort_key, reverse=True)

    for res in sorted_results:
        if is_aggregated:
            strategy, rows, runs, duration_str, throughput_str, mem_str, cpu_str = (
                _format_aggregated_row(res)
            )
            table.add_row(strategy, rows, runs, duration_str, throughput_str, mem_str, cpu_str)
        else:
            strategy, rows, duration_str, throughput_str, mem_str, cpu_str = _format_single_row(res)
            table.add_row(strategy, rows, duration_str, throughput_str, mem_str, cpu_str)

    console.print(table)
