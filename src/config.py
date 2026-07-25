"""
Environment-aware configuration loader.

Reads conf/<env>.yml and exposes a typed, dot-accessible config object so the
rest of the codebase never hardcodes a path, catalog, or table name.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

import yaml

CONF_DIR = Path(__file__).resolve().parent.parent / "conf"


@dataclass(frozen=True)
class SourceConfig:
    raw_path: str
    raw_format: str
    schema_location: str


@dataclass(frozen=True)
class BronzeConfig:
    table: str
    checkpoint_path: str


@dataclass(frozen=True)
class SilverConfig:
    table: str
    quarantine_table: str
    checkpoint_path: str


@dataclass(frozen=True)
class GoldConfig:
    daily_sales_table: str


@dataclass(frozen=True)
class ProcessingConfig:
    shuffle_partitions: int
    max_files_per_trigger: int


@dataclass(frozen=True)
class PipelineConfig:
    env: str
    catalog: str
    schema: str
    source: SourceConfig
    bronze: BronzeConfig
    silver: SilverConfig
    gold: GoldConfig
    processing: ProcessingConfig

    def full_table_name(self, table: str) -> str:
        """Return a 3-level Unity Catalog name, e.g. retail_dev.sales.bronze_sales_raw."""
        return f"{self.catalog}.{self.schema}.{table}"


def load_config(env: str | None = None) -> PipelineConfig:
    """
    Load configuration for the given environment.

    env resolution order: explicit arg -> $DATABRICKS_BUNDLE_TARGET env var -> "dev"
    """
    env = env or os.environ.get("DATABRICKS_BUNDLE_TARGET", "dev")
    conf_path = CONF_DIR / f"{env}.yml"
    if not conf_path.exists():
        raise FileNotFoundError(
            f"No config found for env='{env}' at {conf_path}. "
            f"Available: {[p.stem for p in CONF_DIR.glob('*.yml')]}"
        )

    with open(conf_path) as f:
        raw = yaml.safe_load(f)

    return PipelineConfig(
        env=env,
        catalog=raw["catalog"],
        schema=raw["schema"],
        source=SourceConfig(**raw["source"]),
        bronze=BronzeConfig(**raw["bronze"]),
        silver=SilverConfig(**raw["silver"]),
        gold=GoldConfig(**raw["gold"]),
        processing=ProcessingConfig(**raw["processing"]),
    )
