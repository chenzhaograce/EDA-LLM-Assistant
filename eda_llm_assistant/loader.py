from __future__ import annotations

from pathlib import Path

import pandas as pd

from data_connector import DataConnector
from eda_llm_assistant.config import DataSourceConfig


def load_dataframe(cfg: DataSourceConfig) -> pd.DataFrame:
    dc = DataConnector()

    if not cfg.path:
        raise ValueError("config.data.path is required")

    if cfg.type == "sqlite":
        if cfg.query:
            return dc.read_sqlite_query(cfg.path, cfg.query, **(cfg.pandas_kwargs or {}))
        if cfg.table:
            return dc.read_sqlite_table(cfg.path, cfg.table, **(cfg.pandas_kwargs or {}))
        # Auto: pick first table
        tables = dc.list_sqlite_tables(cfg.path)
        if not tables:
            raise ValueError(f"No tables found in sqlite db: {cfg.path}")
        return dc.read_sqlite_table(cfg.path, tables[0], **(cfg.pandas_kwargs or {}))

    # file
    ext = Path(cfg.path).suffix.lower()
    if ext in [".db", ".sqlite", ".sqlite3"]:
        # treat sqlite as sqlite unless user forces file type
        tables = dc.list_sqlite_tables(cfg.path)
        if not tables:
            raise ValueError(f"No tables found in sqlite db: {cfg.path}")
        return dc.read_sqlite_table(cfg.path, tables[0], **(cfg.pandas_kwargs or {}))

    return dc.auto_detect_and_read(cfg.path, **(cfg.pandas_kwargs or {}))

