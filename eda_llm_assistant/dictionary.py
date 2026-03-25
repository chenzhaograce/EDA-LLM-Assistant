from __future__ import annotations

import re
import pandas as pd

from eda_llm_assistant.analyzer import ColumnTypes

_TOKEN_HINTS: list[tuple[str, str]] = [
    (r"id$", "Identifier (surrogate or business key)"),
    (r"^id$", "Identifier"),
    (r"age", "Age or aging-related measure"),
    (r"date|time|timestamp|ts_", "Date or time of an event"),
    (r"spend|amount|cost|price|revenue|income|salary|fee", "Monetary amount or income"),
    (r"count|num|number|n_|qty|quantity", "Count or numeric quantity"),
    (r"score|rating|sat|satisfaction", "Score or satisfaction level"),
    (r"channel|source|medium", "Channel or source category"),
    (r"country|city|state|region|zip|postal", "Geographic or location field"),
    (r"gender|sex", "Gender or sex category"),
    (r"email|phone|address", "Contact or PII — handle with care"),
    (r"flag|is_|has_", "Boolean or binary indicator"),
    (r"category|type|class|segment", "Category or segment label"),
    (r"visit|session|order", "Event count or transactional unit"),
]


def _tokens_from_name(name: str) -> str:
    s = re.sub(r"([a-z])([A-Z])", r"\1 \2", name)
    s = s.replace("_", " ").replace("-", " ")
    return s.lower()


def infer_meaning_from_name(col: str) -> str:
    t = _tokens_from_name(col)
    for pattern, gloss in _TOKEN_HINTS:
        if re.search(pattern, t, re.I):
            return f"Inferred: likely {gloss} (from column name pattern)."
    return "Inferred: no strong keyword match; inspect sample values and domain documentation."


def _role_in_eda(
    col: str,
    col_types: ColumnTypes,
    target: str | None,
    excluded_from_analysis: set[str],
) -> str:
    if col in excluded_from_analysis:
        return "excluded from analysis (identifier / manual)"
    if target and col == target:
        return "dependent (target)"
    if col in col_types.numeric:
        return "numeric (candidate independent)"
    if col in col_types.categorical:
        return "categorical (candidate independent)"
    if col in col_types.boolean:
        return "boolean (candidate independent)"
    if col in col_types.datetime:
        return "datetime (time index / feature)"
    return "other"


def build_data_dictionary_table(
    df: pd.DataFrame,
    col_types: ColumnTypes,
    target: str | None,
    user_meanings: dict[str, str],
    excluded_from_analysis: set[str] | None = None,
) -> pd.DataFrame:
    rows = []
    n = len(df)
    exc = excluded_from_analysis or set()
    for col in df.columns:
        series = df[col]
        dtype = str(series.dtype)
        nunique = int(series.nunique(dropna=False))
        missing_pct = float(series.isna().mean() * 100) if n else 0.0
        if col in user_meanings:
            meaning = user_meanings[col]
        else:
            meaning = infer_meaning_from_name(col)
        rows.append(
            {
                "column": col,
                "dtype": dtype,
                "n_unique": nunique,
                "missing_pct": round(missing_pct, 2),
                "in_analysis": "no" if col in exc else "yes",
                "meaning": meaning,
                "role_in_eda": _role_in_eda(col, col_types, target, exc),
            }
        )
    return pd.DataFrame(rows)
