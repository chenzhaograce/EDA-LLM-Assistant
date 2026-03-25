"""
Heuristic column intelligence: semantic role guesses and PII pattern flags.
Not a substitute for legal/compliance review — patterns can false-positive or miss encodings.
"""

from __future__ import annotations

import re
from typing import Any

import pandas as pd

from eda_llm_assistant.analyzer import ColumnTypes

# --- PII / sensitive pattern detectors (sample-based) ---
_EMAIL_RE = re.compile(
    r"^[a-zA-Z0-9._%+-]+@[a-zA-Z0-9.-]+\.[a-zA-Z]{2,}$",
)
_PHONE_RE = re.compile(
    r"^\+?\d[\d\s().-]{8,}\d$",
)
_SSN_LIKE_RE = re.compile(r"^\d{3}-\d{2}-\d{4}$|^\d{9}$")
# Rough card-like: 13–19 consecutive digits (spaces/dashes stripped)
_NAME_MONEY = re.compile(
    r"price|amount|cost|revenue|income|salary|spend|payment|fee|usd|eur|gbp|\$",
    re.I,
)
_NAME_PCT = re.compile(r"pct|percent|percentage|rate|ratio|share", re.I)
_NAME_COUNT = re.compile(r"count|num_|n_|number|qty|quantity|visit|frequency|trials", re.I)


def _luhn_valid(digits: str) -> bool:
    d = [int(x) for x in digits if x.isdigit()]
    if len(d) < 13 or len(d) > 19:
        return False
    s = 0
    alt = False
    for x in reversed(d):
        if alt:
            x = x * 2
            if x > 9:
                x -= 9
        s += x
        alt = not alt
    return s % 10 == 0


def _pii_flags_from_strings(values: pd.Series) -> str:
    """Return semicolon-separated flags from string sample."""
    s = values.dropna().astype(str).str.strip()
    s = s[s != ""]
    if s.empty:
        return "none"
    sample = s.head(800)
    n = len(sample)
    if n == 0:
        return "none"
    flags: list[str] = []

    em = sample.str.match(_EMAIL_RE, na=False).mean()
    if em >= 0.5:
        flags.append("possible_email")

    ph = sample.str.match(_PHONE_RE, na=False).mean()
    if ph >= 0.4:
        flags.append("possible_phone")

    ss = sample.str.match(_SSN_LIKE_RE, na=False).mean()
    if ss >= 0.3:
        flags.append("possible_ssn_like_pattern")

    card_hits = 0
    for v in sample.head(200):
        raw = re.sub(r"[\s-]", "", str(v))
        if raw.isdigit() and 13 <= len(raw) <= 19 and _luhn_valid(raw):
            card_hits += 1
    if card_hits >= 3 and card_hits / min(len(sample), 200) >= 0.05:
        flags.append("possible_payment_card_luhn")

    return "; ".join(flags) if flags else "none"


def _semantic_guess(
    col: str,
    series: pd.Series,
    col_types: ColumnTypes,
    excluded_from_analysis: bool,
) -> tuple[str, str]:
    """Return (semantic_guess, notes)."""
    name = col
    n = max(len(series), 1)
    nunq = int(series.nunique(dropna=False))
    ratio = nunq / n

    if excluded_from_analysis:
        return "surrogate_or_manual_excluded", "Column excluded from analysis (often an ID)."

    if col in col_types.datetime:
        return "datetime", "Parsed or native datetime."

    if col in col_types.boolean:
        return "boolean", "Boolean dtype."

    if col in col_types.numeric:
        if _NAME_MONEY.search(name):
            return "monetary_like", "Name suggests money; verify currency/units."
        if _NAME_PCT.search(name):
            s = pd.to_numeric(series, errors="coerce").dropna()
            if not s.empty and s.between(0, 1).mean() > 0.85:
                return "proportion_0_1", "Values mostly in [0,1]; may be proportions."
            if not s.empty and s.between(0, 100).mean() > 0.85:
                return "percentage_0_100", "Values mostly in [0,100]; may be percentages."
        if _NAME_COUNT.search(name) and pd.api.types.is_integer_dtype(series):
            return "count_like_integer", "Integer with count-like name."
        if pd.api.types.is_integer_dtype(series) and nunq < 15:
            return "small_integer_codes", "Few distinct integers; could be ordinal codes."
        return "continuous_or_count_numeric", "Generic numeric; inspect domain."

    if col in col_types.categorical:
        if ratio > 0.5 and nunq > 20:
            return "high_cardinality_text", "Many unique values vs rows; possible free text or IDs as strings."
        return "nominal_or_low_cardinality_categorical", "Typical categorical / string labels."

    return "unknown", "Could not classify beyond dtype."


def build_column_intelligence_table(
    df_full: pd.DataFrame,
    col_types: ColumnTypes,
    excluded_from_analysis: set[str],
) -> pd.DataFrame:
    """
    One row per column in df_full.
    PII scan uses string conversion on a sample (may be slow on huge text columns — capped).
    """
    out: list[dict[str, Any]] = []
    for col in df_full.columns:
        series = df_full[col]
        exc = col in excluded_from_analysis
        semantic, sem_note = _semantic_guess(col, series, col_types, exc)
        in_ana = "no" if exc else "yes"
        nunq = int(series.nunique(dropna=False))

        if series.dtype == object or pd.api.types.is_string_dtype(series):
            pii = _pii_flags_from_strings(series)
            pii_note = (
                "Pattern scan on string sample; verify before redaction."
                if pii != "none"
                else ""
            )
        else:
            pii = "none"
            pii_note = ""

        out.append(
            {
                "column": col,
                "in_analysis": in_ana,
                "semantic_guess": semantic,
                "semantic_note": sem_note,
                "pii_risk_flags": pii,
                "pii_scan_note": pii_note,
                "n_unique": nunq,
                "cardinality_ratio": round(nunq / max(len(series), 1), 4),
            }
        )
    return pd.DataFrame(out)
