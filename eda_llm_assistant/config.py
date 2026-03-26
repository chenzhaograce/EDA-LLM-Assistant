from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Literal


DataSourceType = Literal["file", "sqlite"]


@dataclass
class DataSourceConfig:
    type: DataSourceType = "file"
    path: str = ""

    # sqlite-only
    table: str | None = None
    query: str | None = None

    # pandas read_* kwargs
    pandas_kwargs: dict[str, Any] = field(default_factory=dict)


@dataclass
class EDASectionConfig:
    basic_overview: bool = True
    missing_values: bool = True
    duplicates: bool = True
    numeric_summary: bool = True
    categorical_summary: bool = True
    correlations: bool = True
    outliers_iqr: bool = True
    time_series: bool = True

    # optional automated EDA engines
    ydata_profiling_html: bool = False
    sweetviz_html: bool = False

    # semantic / PII heuristics (full table in report)
    column_intelligence: bool = True
    # target-focused tables & tests (requires columns.target); ignored if target unset
    supervised_eda: bool = True


@dataclass
class ColumnConfig:
    include: list[str] | None = None
    exclude: list[str] | None = None
    target: str | None = None
    datetime_columns: list[str] | None = None
    # Drop identifier-like columns from statistical EDA (correlations, outliers, plots, etc.)
    auto_exclude_id_columns: bool = True
    # Always exclude these from analysis (in addition to auto rule when enabled)
    exclude_from_analysis: list[str] | None = None


@dataclass
class ReportConfig:
    title: str = "EDA Report"
    output_dir: str = "outputs"
    save_markdown: bool = True
    save_html: bool = True
    max_categories: int = 20
    corr_threshold: float = 0.7
    sample_rows: int | None = None
    # Flag columns with missing_pct >= this threshold in validation
    high_missing_pct_threshold: float = 50.0
    # Chi-square / group plots skipped if either dimension exceeds this many levels
    supervised_max_category_levels: int = 25


@dataclass
class LLMConfig:
    enabled: bool = False
    provider: Literal["openai", "gemini"] = "openai"
    model: str = "gpt-4o-mini"
    api_key_env: str = "OPENAI_API_KEY"

    # prompts
    system_prompt: str = (
        "You are a senior data analyst. Given EDA results, write concise, actionable insights. "
        "Be clear about uncertainty and avoid hallucinating data not in the summary."
    )
    user_prompt: str = (
        "Please summarize the dataset and key issues, then provide 5-10 business/analysis recommendations."
    )


@dataclass
class AppConfig:
    data: DataSourceConfig = field(default_factory=DataSourceConfig)
    columns: ColumnConfig = field(default_factory=ColumnConfig)
    sections: EDASectionConfig = field(default_factory=EDASectionConfig)
    report: ReportConfig = field(default_factory=ReportConfig)
    llm: LLMConfig = field(default_factory=LLMConfig)
    # Optional: column_name -> human definition (overrides inferred dictionary)
    dictionary: dict[str, str] = field(default_factory=dict)


def load_config(path: str | Path) -> AppConfig:
    import yaml

    raw = yaml.safe_load(Path(path).read_text(encoding="utf-8")) or {}

    data = DataSourceConfig(**(raw.get("data") or {}))
    columns = ColumnConfig(**(raw.get("columns") or {}))
    sections = EDASectionConfig(**(raw.get("sections") or {}))
    report = ReportConfig(**(raw.get("report") or {}))
    llm = LLMConfig(**(raw.get("llm") or {}))
    dictionary = raw.get("dictionary") or {}
    if not isinstance(dictionary, dict):
        dictionary = {}
    dictionary = {str(k): str(v) for k, v in dictionary.items()}

    return AppConfig(
        data=data,
        columns=columns,
        sections=sections,
        report=report,
        llm=llm,
        dictionary=dictionary,
    )

