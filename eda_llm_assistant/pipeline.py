from __future__ import annotations

from pathlib import Path
from typing import Any

import pandas as pd

from eda_llm_assistant.analyzer import (
    analysis_frame,
    apply_column_selection,
    categorical_summary,
    coerce_datetimes,
    columns_excluded_from_analysis,
    correlations,
    correlations_with_target,
    data_quality_issues,
    dataset_overview,
    duplicates_info,
    infer_column_types,
    missing_values,
    numeric_summary,
    outliers_iqr,
    sample_dataframe,
    suggested_transformations,
    write_sample_csv,
)
from eda_llm_assistant.config import AppConfig
from eda_llm_assistant.dictionary import build_data_dictionary_table
from eda_llm_assistant.llm import llm_summarize
from eda_llm_assistant.column_intelligence import build_column_intelligence_table
from eda_llm_assistant.plots import (
    plot_boxplots,
    plot_categorical_top,
    plot_corr_heatmap,
    plot_missing_bar,
    plot_missing_heatmap,
    plot_numeric_distributions,
    plot_supervised_bivariate_boxplots,
    plot_target_distribution,
    plot_time_series_counts,
)
from eda_llm_assistant.supervised import run_supervised_eda
from eda_llm_assistant.reporting import markdown_to_html, render_markdown, write_html, write_markdown
from eda_llm_assistant import __version__ as pkg_version
from eda_llm_assistant.utils import ensure_dir, runtime_provenance, to_jsonable
from eda_llm_assistant.loader import load_dataframe


def run_pipeline(cfg: AppConfig) -> dict[str, Any]:
    out_dir = Path(cfg.report.output_dir)
    assets_dir = ensure_dir(out_dir / "assets")

    df_full = load_dataframe(cfg.data)
    df_full = apply_column_selection(df_full, cfg.columns)

    excluded = columns_excluded_from_analysis(
        list(df_full.columns),
        manual=cfg.columns.exclude_from_analysis,
        auto_id=cfg.columns.auto_exclude_id_columns,
        target=cfg.columns.target,
    )
    df = analysis_frame(df_full, excluded)

    if cfg.report.sample_rows:
        df_for_plots = sample_dataframe(df, cfg.report.sample_rows)
    else:
        df_for_plots = df

    col_types = infer_column_types(df, cfg.columns)
    df = coerce_datetimes(df, col_types.datetime)
    df_for_plots = coerce_datetimes(df_for_plots, col_types.datetime)

    artifacts: dict[str, str] = {}
    sample_csv = write_sample_csv(df, out_dir=out_dir)
    artifacts["sample_rows.csv"] = _relpath(sample_csv, out_dir)

    overview = dataset_overview(df)

    miss_df = None
    dup_info = None
    numeric = None
    categorical = None
    corr_pairs = None
    corr_matrix = None
    outliers_df = None
    corr_with_target_df = None

    applied_transformations: list[str] = []
    if excluded:
        applied_transformations.append(
            "**Identifier / manual exclusion:** dropped from analysis (still in dictionary): "
            + ", ".join(f"`{c}`" for c in excluded)
            + "."
        )
    if cfg.columns.datetime_columns:
        applied_transformations.append(
            "Datetime coercion: columns "
            + ", ".join(f"`{c}`" for c in cfg.columns.datetime_columns)
            + " → `pandas.to_datetime(..., errors='coerce')` (invalid → `NaT`)."
        )
    if cfg.columns.include:
        applied_transformations.append(
            "Column filter: **include** only " + ", ".join(f"`{c}`" for c in cfg.columns.include) + "."
        )
    if cfg.columns.exclude:
        applied_transformations.append(
            "Column filter: **exclude** " + ", ".join(f"`{c}`" for c in cfg.columns.exclude) + "."
        )
    if not applied_transformations:
        applied_transformations.append(
            "No additional transforms beyond loading via `DataConnector` / pandas read functions."
        )

    quality_df = data_quality_issues(df, high_missing_pct=cfg.report.high_missing_pct_threshold)
    suggested_transformations_list = suggested_transformations(df, col_types)

    data_dictionary_df = build_data_dictionary_table(
        df_full,
        col_types,
        cfg.columns.target,
        cfg.dictionary,
        excluded_from_analysis=set(excluded),
    )
    dict_csv = out_dir / "data_dictionary.csv"
    data_dictionary_df.to_csv(dict_csv, index=False)
    artifacts["data_dictionary.csv"] = _relpath(str(dict_csv), out_dir)

    column_intel_df: pd.DataFrame | None = None
    if cfg.sections.column_intelligence:
        column_intel_df = build_column_intelligence_table(df_full, col_types, set(excluded))
        ci_csv = out_dir / "column_intelligence.csv"
        column_intel_df.to_csv(ci_csv, index=False)
        artifacts["column_intelligence.csv"] = _relpath(str(ci_csv), out_dir)

    supervised_bundle: dict[str, Any] | None = None
    if cfg.sections.supervised_eda and cfg.columns.target and cfg.columns.target in df.columns:
        supervised_bundle = run_supervised_eda(
            df,
            cfg.columns.target,
            col_types,
            max_chi2_levels=cfg.report.supervised_max_category_levels,
        )
        if supervised_bundle and supervised_bundle.get("kind") in ("numeric", "categorical"):
            is_num = supervised_bundle["kind"] == "numeric"
            p = plot_target_distribution(
                df, cfg.columns.target, is_numeric=is_num, out_dir=assets_dir
            )
            if p:
                artifacts["supervised_target_distribution.png"] = _relpath(p, out_dir)
            p2 = plot_supervised_bivariate_boxplots(
                df,
                cfg.columns.target,
                target_is_numeric=is_num,
                numeric_cols=col_types.numeric,
                categorical_cols=col_types.categorical,
                out_dir=assets_dir,
            )
            if p2:
                artifacts["supervised_bivariate_boxplots.png"] = _relpath(p2, out_dir)
        elif supervised_bundle.get("kind") == "other_dtype":
            p = plot_target_distribution(
                df, cfg.columns.target, is_numeric=False, out_dir=assets_dir
            )
            if p:
                artifacts["supervised_target_distribution.png"] = _relpath(p, out_dir)

    if cfg.sections.duplicates:
        dup_info = duplicates_info(df)

    if cfg.sections.missing_values:
        miss_df = missing_values(df)
        p = plot_missing_bar(df_for_plots, assets_dir)
        if p:
            artifacts["missing_values_bar.png"] = _relpath(p, out_dir)
        p = plot_missing_heatmap(df_for_plots, assets_dir)
        if p:
            artifacts["missing_values_heatmap.png"] = _relpath(p, out_dir)

    if cfg.sections.numeric_summary:
        numeric = numeric_summary(df, col_types.numeric)
        p = plot_numeric_distributions(df_for_plots, col_types.numeric, assets_dir)
        if p:
            artifacts["numeric_distributions.png"] = _relpath(p, out_dir)
        p = plot_boxplots(df_for_plots, col_types.numeric, assets_dir)
        if p:
            artifacts["numeric_boxplots.png"] = _relpath(p, out_dir)

    if cfg.sections.categorical_summary:
        categorical = categorical_summary(df, col_types.categorical, max_categories=cfg.report.max_categories)
        p = plot_categorical_top(df_for_plots, col_types.categorical, assets_dir)
        if p:
            artifacts["categorical_top_values.png"] = _relpath(p, out_dir)

    if cfg.sections.correlations:
        corr_res = correlations(df, col_types.numeric, threshold=cfg.report.corr_threshold)
        corr_pairs = corr_res.get("high_pairs") or []
        corr_matrix = corr_res.get("matrix")
        corr_with_target_df = correlations_with_target(df, col_types.numeric, cfg.columns.target)
        if isinstance(corr_matrix, pd.DataFrame):
            p = plot_corr_heatmap(corr_matrix, assets_dir)
            if p:
                artifacts["correlation_heatmap.png"] = _relpath(p, out_dir)

    if cfg.sections.outliers_iqr:
        outliers_df = outliers_iqr(df, col_types.numeric)

    if cfg.sections.time_series and col_types.datetime:
        paths = plot_time_series_counts(df_for_plots, col_types.datetime, assets_dir)
        for p in paths:
            name = Path(p).name
            artifacts[name] = _relpath(p, out_dir)

    if cfg.sections.ydata_profiling_html:
        try:
            from ydata_profiling import ProfileReport

            prof = ProfileReport(df_for_plots, title=cfg.report.title, explorative=True)
            p = out_dir / "ydata_profiling.html"
            prof.to_file(str(p))
            artifacts["ydata_profiling.html"] = _relpath(str(p), out_dir)
        except Exception as e:
            artifacts["ydata_profiling_error.txt"] = _relpath(
                str(_write_text(out_dir, "ydata_profiling_error.txt", str(e))), out_dir
            )

    if cfg.sections.sweetviz_html:
        try:
            import sweetviz as sv

            report = sv.analyze(df_for_plots)
            p = out_dir / "sweetviz.html"
            report.show_html(str(p), open_browser=False)
            artifacts["sweetviz.html"] = _relpath(str(p), out_dir)
        except Exception as e:
            artifacts["sweetviz_error.txt"] = _relpath(
                str(_write_text(out_dir, "sweetviz_error.txt", str(e))), out_dir
            )

    llm_text = None
    col_types_dict = {
        "numeric": col_types.numeric,
        "categorical": col_types.categorical,
        "boolean": col_types.boolean,
        "datetime": col_types.datetime,
    }
    eda_payload = {
        "overview": overview,
        "column_types": col_types_dict,
        "duplicates": dup_info,
        "missing_top": None if miss_df is None else miss_df.head(50).to_dict(orient="records"),
        "high_correlations": corr_pairs[:50] if corr_pairs else [],
        "outliers_top": None if outliers_df is None else outliers_df.head(50).to_dict(orient="records"),
        "quality_flags": quality_df.head(80).to_dict(orient="records") if not quality_df.empty else [],
    }
    llm_text = llm_summarize(cfg.llm, to_jsonable(eda_payload))

    plot_sample_note = None
    if cfg.report.sample_rows and len(df) > len(df_for_plots):
        plot_sample_note = (
            f"plots use a random sample of up to {cfg.report.sample_rows} rows; "
            f"tables and validation use all {len(df)} rows (analysis columns only; IDs excluded)."
        )

    provenance = runtime_provenance()
    provenance["eda_llm_assistant"] = pkg_version

    md = render_markdown(
        title=cfg.report.title,
        data_dictionary_df=data_dictionary_df,
        overview=overview,
        col_types=col_types_dict,
        target=cfg.columns.target,
        user_dictionary_count=len(cfg.dictionary),
        duplicates=dup_info,
        missing_df=miss_df,
        quality_df=quality_df,
        outliers_df=outliers_df,
        numeric=numeric,
        categorical=categorical,
        corr_high_pairs=corr_pairs,
        corr_with_target_df=corr_with_target_df,
        applied_transformations=applied_transformations,
        suggested_transformations=suggested_transformations_list,
        artifacts=artifacts,
        llm_text=llm_text,
        provenance=provenance,
        sections=cfg.sections,
        plot_sample_note=plot_sample_note,
        data_source_path=cfg.data.path or "(none)",
        excluded_from_analysis=excluded,
        column_intelligence_df=column_intel_df,
        supervised_bundle=supervised_bundle,
    )

    report_paths: dict[str, str] = {}
    if cfg.report.save_markdown:
        p = write_markdown(md, out_dir=out_dir)
        report_paths["markdown"] = str(p)
    if cfg.report.save_html:
        html = markdown_to_html(md, title=cfg.report.title)
        p = write_html(html, out_dir=out_dir)
        report_paths["html"] = str(p)

    return {
        "output_dir": str(out_dir),
        "reports": report_paths,
        "artifacts": artifacts,
    }


def _relpath(p: str, out_dir: Path) -> str:
    try:
        return str(Path(p).resolve().relative_to(out_dir.resolve()))
    except Exception:
        return str(p)


def _write_text(out_dir: Path, filename: str, text: str) -> Path:
    ensure_dir(out_dir)
    p = out_dir / filename
    p.write_text(text, encoding="utf-8")
    return p
