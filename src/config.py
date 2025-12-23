"""
Configuration settings for the SQL Throughput Challenge.

Uses Pydantic Settings to load environment variables for database connections,
logging, and benchmark defaults. This is a minimal stub to be expanded with
additional fields as the project evolves.
"""
from __future__ import annotations

from functools import lru_cache
from pydantic import Field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    # Database
    db_host: str = Field("localhost", alias="DB_HOST")
    db_port: int = Field(5432, alias="DB_PORT")
    db_user: str = Field("postgres", alias="DB_USER")
    db_password: str = Field("postgres", alias="DB_PASSWORD")
    db_name: str = Field("throughput_challenge", alias="DB_NAME")

    # Application
    app_env: str = Field("development", alias="APP_ENV")
    log_level: str = Field("INFO", alias="LOG_LEVEL")

    # Benchmark defaults
    benchmark_rows: int = Field(1_000_000, alias="BENCHMARK_ROWS")
    benchmark_batch_size: int = Field(10_000, alias="BENCHMARK_BATCH_SIZE")
    benchmark_concurrency: int = Field(4, alias="BENCHMARK_CONCURRENCY")

    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )


@lru_cache(maxsize=1)
def get_settings() -> Settings:
    """
    Retrieve a cached instance of Settings to avoid repeated env parsing.
    """
    return Settings()


__all__ = ["Settings", "get_settings"]
