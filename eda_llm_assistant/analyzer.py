from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

from eda_llm_assistant.config import ColumnConfig
from eda_llm_assistant.utils import ensure_dir


@dataclass
class ColumnTypes:
    numeric: list[str]
    categorical: list[str]
    boolean: list[str]
    datetime: list[str]


def is_id_like_column(name: str) -> bool:
    """
    Heuristic: columns that are usually record identifiers, not modeling features.
    Matches: `id`, `*_id`, camelCase `*Id`, `*ID` (e.g. CustomerID). Avoids loose `*id` suffix
    (e.g. 'paid') by requiring Id/ID boundary or underscore before id.
    """
    if re.fullmatch(r"(?i)id", name):
        return True
    if re.search(r"(?i)_id$", name):
        return True
    if re.search(r"Id$", name):  # customerId, rowId
        return True
    if re.search(r"ID$", name):  # CustomerID, orderID
        return True
    return False


def columns_excluded_from_analysis(
    column_names: list[str],
    *,
    manual: list[str] | None,
    auto_id: bool,
    target: str | None,
) -> list[str]:
    """Columns dropped from stats/plots/correlations/outliers; never drops `target`."""
    out: set[str] = set()
    for c in manual or []:
        if c in column_names:
            out.add(c)
    tgt = target
    if auto_id:
        for c in column_names:
            if tgt and c == tgt:
                continue
            if is_id_like_column(c):
                out.add(c)
    return sorted(out)


def analysis_frame(df: pd.DataFrame, excluded: list[str]) -> pd.DataFrame:
    cols = [c for c in df.columns if c not in set(excluded)]
    return df[cols].copy()


def apply_column_selection(df: pd.DataFrame, cfg: ColumnConfig) -> pd.DataFrame:
    cols = list(df.columns)
    if cfg.include:
        cols = [c for c in cols if c in set(cfg.include)]
    if cfg.exclude:
        cols = [c for c in cols if c not in set(cfg.exclude)]
    return df[cols].copy()


def infer_column_types(df: pd.DataFrame, cfg: ColumnConfig) -> ColumnTypes:
    numeric = df.select_dtypes(include=[np.number]).columns.tolist()
    boolean = df.select_dtypes(include=["bool"]).columns.tolist()
    datetime = df.select_dtypes(include=["datetime64[ns]", "datetime64"]).columns.tolist()
    categorical = df.select_dtypes(include=["object", "category"]).columns.tolist()

    # user-specified datetime columns
    if cfg.datetime_columns:
        for c in cfg.datetime_columns:
            if c in df.columns and c not in datetime:
                datetime.append(c)
                if c in categorical:
                    categorical.remove(c)

    return ColumnTypes(
        numeric=sorted(set(numeric)),
        categorical=sorted(set(categorical)),
        boolean=sorted(set(boolean)),
        datetime=sorted(set(datetime)),
    )


def coerce_datetimes(df: pd.DataFrame, datetime_cols: list[str]) -> pd.DataFrame:
    if not datetime_cols:
        return df
    out = df.copy()
    for c in datetime_cols:
        if c in out.columns and not pd.api.types.is_datetime64_any_dtype(out[c]):
            out[c] = pd.to_datetime(out[c], errors="coerce")
    return out


def dataset_overview(df: pd.DataFrame) -> dict[str, Any]:
    mem_mb = float(df.memory_usage(deep=True).sum()) / (1024**2)
    return {
        "shape": {"rows": int(df.shape[0]), "cols": int(df.shape[1])},
        "memory_mb": round(mem_mb, 3),
        "dtypes": {c: str(t) for c, t in df.dtypes.items()},
    }


def missing_values(df: pd.DataFrame) -> pd.DataFrame:
    miss = df.isna().sum()
    pct = (miss / max(len(df), 1)) * 100
    out = (
        pd.DataFrame({"missing_count": miss, "missing_pct": pct})
        .sort_values(["missing_pct", "missing_count"], ascending=False)
        .reset_index(names="column")
    )
    return out


def duplicates_info(df: pd.DataFrame) -> dict[str, Any]:
    dup = int(df.duplicated().sum())
    return {"duplicate_rows": dup, "duplicate_pct": (dup / max(len(df), 1)) * 100.0}


def numeric_summary(df: pd.DataFrame, numeric_cols: list[str]) -> dict[str, Any]:
    if not numeric_cols:
        return {"columns": [], "describe": {}}
    desc = df[numeric_cols].describe(percentiles=[0.01, 0.05, 0.5, 0.95, 0.99]).to_dict()
    skew = df[numeric_cols].skew(numeric_only=True).to_dict()
    kurt = df[numeric_cols].kurtosis(numeric_only=True).to_dict()
    return {"columns": numeric_cols, "describe": desc, "skewness": skew, "kurtosis": kurt}


def categorical_summary(df: pd.DataFrame, cat_cols: list[str], max_categories: int) -> dict[str, Any]:
    out: dict[str, Any] = {"columns": cat_cols, "top_values": {}, "nunique": {}}
    for c in cat_cols:
        vc = df[c].astype("object").value_counts(dropna=False).head(max_categories)
        out["top_values"][c] = [{"value": _stringify_index(v), "count": int(n)} for v, n in vc.items()]
        out["nunique"][c] = int(df[c].nunique(dropna=False))
    return out


def correlations(df: pd.DataFrame, numeric_cols: list[str], threshold: float) -> dict[str, Any]:
    if len(numeric_cols) < 2:
        return {"columns": numeric_cols, "high_pairs": []}
    corr = df[numeric_cols].corr(numeric_only=True)
    pairs = []
    for i, c1 in enumerate(corr.columns):
        for j in range(i + 1, len(corr.columns)):
            c2 = corr.columns[j]
            v = corr.iloc[i, j]
            if pd.notna(v) and abs(float(v)) >= threshold:
                pairs.append({"feature_1": c1, "feature_2": c2, "corr": float(v)})
    pairs = sorted(pairs, key=lambda x: abs(x["corr"]), reverse=True)
    return {"columns": numeric_cols, "high_pairs": pairs, "matrix": corr}


def correlations_with_target(
    df: pd.DataFrame,
    numeric_cols: list[str],
    target: str | None,
) -> pd.DataFrame | None:
    """Pearson correlation of numeric features with a numeric target (absolute values sorted)."""
    if not target or target not in df.columns or target not in numeric_cols:
        return None
    others = [c for c in numeric_cols if c != target]
    if not others:
        return None
    sub = df[[target, *others]].corr(numeric_only=True)
    if target not in sub.index:
        return None
    s = sub[target].drop(labels=[target], errors="ignore").dropna()
    out = (
        s.to_frame(name="pearson_r")
        .assign(abs_r=lambda x: x["pearson_r"].abs())
        .sort_values("abs_r", ascending=False)
        .reset_index(names="feature")
    )
    return out


def outliers_iqr(df: pd.DataFrame, numeric_cols: list[str]) -> pd.DataFrame:
    rows = []
    n = max(len(df), 1)
    for c in numeric_cols:
        s = df[c]
        if s.dropna().empty:
            rows.append({"column": c, "outlier_count": 0, "outlier_pct": 0.0})
            continue
        q1 = s.quantile(0.25)
        q3 = s.quantile(0.75)
        iqr = q3 - q1
        if pd.isna(iqr) or iqr == 0:
            rows.append({"column": c, "outlier_count": 0, "outlier_pct": 0.0})
            continue
        lower = q1 - 1.5 * iqr
        upper = q3 + 1.5 * iqr
        cnt = int(((s < lower) | (s > upper)).sum(skipna=True))
        rows.append({"column": c, "outlier_count": cnt, "outlier_pct": (cnt / n) * 100.0})
    return pd.DataFrame(rows).sort_values(["outlier_count", "outlier_pct"], ascending=False)


def data_quality_issues(
    df: pd.DataFrame,
    high_missing_pct: float = 50.0,
) -> pd.DataFrame:
    """Heuristic checks: constants, high missing, mixed-type object columns."""
    rows: list[dict[str, Any]] = []

    for col in df.columns:
        s = df[col]
        nunq = s.nunique(dropna=True)
        if nunq <= 1 and s.notna().any():
            rows.append(
                {
                    "check": "constant_column",
                    "column": col,
                    "detail": f"At most one distinct non-null value (nunique={nunq}).",
                }
            )

    miss_pct = df.isna().mean() * 100
    for col, pct in miss_pct.items():
        if pct >= high_missing_pct:
            rows.append(
                {
                    "check": "high_missing_rate",
                    "column": col,
                    "detail": f"Missing {pct:.1f}% of rows (threshold {high_missing_pct}%).",
                }
            )

    for col in df.select_dtypes(include=["object", "string"]).columns.tolist():
        sample = df[col].dropna().head(500)
        if sample.empty:
            continue
        parsed = pd.to_numeric(sample.astype(str), errors="coerce")
        ok = parsed.notna()
        if ok.any() and (~ok).any():
            rows.append(
                {
                    "check": "mixed_numeric_and_text",
                    "column": col,
                    "detail": "Some values parse as numbers and some do not; likely mixed formats or dirty text.",
                }
            )

    return pd.DataFrame(rows)


def suggested_transformations(df: pd.DataFrame, col_types: ColumnTypes) -> list[str]:
    """Non-invasive suggestions only (no automatic transformation beyond reporting)."""
    suggestions: list[str] = []
    n = max(len(df), 1)

    for c in col_types.numeric:
        if c not in df.columns:
            continue
        sk = df[c].skew()
        if pd.notna(sk) and abs(float(sk)) > 1.0:
            suggestions.append(
                f"`{c}`: skewness ≈ {float(sk):.2f} (|skew|>1). For modeling, consider log1p, Box-Cox, or Yeo–Johnson if values are positive-only / appropriate."
            )

    for c in col_types.categorical:
        if c not in df.columns:
            continue
        card = df[c].nunique(dropna=False)
        ratio = card / n
        if ratio > 0.5 and card > 10:
            suggestions.append(
                f"`{c}`: high cardinality (n_unique={card}, n_unique/n≈{ratio:.2f}). Consider rare-level grouping, frequency encoding, or hash embedding for models."
            )

    if col_types.datetime:
        suggestions.append(
            "Datetime columns: consider derived features (year/month/day-of-week/hour) for seasonality, or time-based train/test splits."
        )

    return suggestions


def sample_dataframe(df: pd.DataFrame, sample_rows: int | None) -> pd.DataFrame:
    if not sample_rows or sample_rows <= 0 or len(df) <= sample_rows:
        return df
    return df.sample(sample_rows, random_state=42)


def write_sample_csv(df: pd.DataFrame, out_dir: Path, n: int = 20) -> str:
    ensure_dir(out_dir)
    p = out_dir / "sample_rows.csv"
    df.head(n).to_csv(p, index=False)
    return str(p)


def _stringify_index(v: Any) -> str:
    if v is None:
        return "null"
    if isinstance(v, float) and np.isnan(v):
        return "NaN"
    return str(v)

