"""
EDA LLM Assistant — web UI for non-technical users.

Run from the project root:
    streamlit run streamlit_app.py
"""

from __future__ import annotations

import io
import shutil
import sys
import tempfile
import zipfile
from pathlib import Path

import pandas as pd
import streamlit as st
import yaml

# Project root (directory containing this file)
_ROOT = Path(__file__).resolve().parent
if str(_ROOT) not in sys.path:
    sys.path.insert(0, str(_ROOT))

from data_connector import DataConnector
from eda_llm_assistant import __version__
from eda_llm_assistant.config import (
    AppConfig,
    ColumnConfig,
    DataSourceConfig,
    EDASectionConfig,
    LLMConfig,
    ReportConfig,
)
from eda_llm_assistant.pdf_export import markdown_to_pdf_bytes
from eda_llm_assistant.pipeline import run_pipeline

# Multipage helper path (must match a file under `pages/`, run `streamlit run streamlit_app.py` from repo root).
_REPORT_VIEWER_PAGE = "pages/2_Report_viewer.py"


def _parse_dictionary(text: str) -> dict[str, str]:
    text = (text or "").strip()
    if not text:
        return {}
    try:
        data = yaml.safe_load(text)
        if isinstance(data, dict):
            return {str(k): str(v) for k, v in data.items()}
    except yaml.YAMLError:
        pass
    out: dict[str, str] = {}
    for line in text.splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        if ":" in line:
            k, _, v = line.partition(":")
            out[k.strip()] = v.strip()
    return out


def _preview_dataframe(path: str, source_type: str, table: str | None) -> pd.DataFrame:
    dc = DataConnector()
    if source_type == "sqlite":
        if table:
            return dc.read_sqlite_table(path, table)
        tables = dc.list_sqlite_tables(path)
        if not tables:
            raise ValueError("No tables in SQLite file.")
        return dc.read_sqlite_table(path, tables[0])
    return dc.auto_detect_and_read(path)


def _list_sqlite_tables(path: str) -> list[str]:
    return DataConnector().list_sqlite_tables(path)


def main() -> None:
    st.set_page_config(
        page_title="EDA Report Studio",
        page_icon="📊",
        layout="wide",
        initial_sidebar_state="expanded",
    )

    st.title("EDA Report Studio")
    st.caption(
        f"Upload a data file, choose what to include in the report, then download HTML / Markdown / a ZIP of all outputs. "
        f"(Assistant v{__version__})"
    )

    with st.sidebar:
        st.header("How it works")
        st.markdown(
            """
1. **Upload** a spreadsheet or database file.  
2. **Preview** the first rows and pick options.  
3. **Generate** — we build the same report as the command-line tool.  
4. **Download** the HTML report (best for sharing) or the full ZIP.
            """
        )
        st.divider()
        st.markdown("[Documentation](README.md) · Uses your machine only (no cloud upload).")
        st.divider()
        st.subheader("Navigation")
        # Explicit links so users always see Report viewer (sidebar auto-list can be easy to miss).
        st.page_link("streamlit_app.py", label="EDA Report Studio", icon="📊")
        st.page_link(_REPORT_VIEWER_PAGE, label="Report viewer", icon="📄")
        if st.session_state.get("last_out_root") and st.button(
            "Open Report viewer now",
            use_container_width=True,
            key="nav_open_report_viewer_sidebar",
        ):
            st.switch_page(_REPORT_VIEWER_PAGE)

    # --- Upload ---
    st.subheader("1. Your data")
    uploaded = st.file_uploader(
        "Upload a file",
        type=["csv", "xlsx", "xls", "json", "parquet", "db", "sqlite", "sqlite3"],
        help="CSV, Excel, JSON, Parquet, or SQLite. Large files may take longer.",
    )

    if uploaded is None:
        st.info("Upload a file to continue. Supported: CSV, Excel, JSON, Parquet, SQLite.")
        return

    suffix = Path(uploaded.name).suffix.lower() or ".csv"
    sqlite_exts = {".db", ".sqlite", ".sqlite3"}
    is_sqlite = suffix in sqlite_exts

    raw = uploaded.getvalue()
    sig = hash(raw)
    if st.session_state.get("upload_sig") != sig:
        tdir = Path(tempfile.mkdtemp(prefix="eda_upload_"))
        tmp_path = tdir / f"dataset{suffix}"
        tmp_path.write_bytes(raw)
        st.session_state["upload_path"] = str(tmp_path)
        st.session_state["upload_name"] = uploaded.name
        st.session_state["upload_sig"] = sig
        st.session_state["upload_sqlite_table"] = None
        st.session_state.pop("last_out_root", None)
        st.session_state.pop("last_result", None)

    path = st.session_state["upload_path"]

    table_choice: str | None = None
    if is_sqlite:
        try:
            tables = _list_sqlite_tables(path)
        except Exception as e:
            st.error(f"Could not read SQLite: {e}")
            return
        if not tables:
            st.error("No tables found in this database.")
            return
        table_choice = st.selectbox("Which table to analyze?", options=tables, index=0)
        st.session_state["upload_sqlite_table"] = table_choice

    try:
        df_preview = _preview_dataframe(path, "sqlite" if is_sqlite else "file", table_choice)
    except Exception as e:
        st.error(f"Could not load data: {e}")
        return

    columns = list(df_preview.columns)
    st.success(f"Loaded **{uploaded.name}** — **{len(df_preview):,}** rows × **{len(columns)}** columns.")
    with st.expander("Preview (first 15 rows)", expanded=False):
        st.dataframe(df_preview.head(15), use_container_width=True)

    st.subheader("2. Customize the report")
    c1, c2, c3 = st.columns(3)
    with c1:
        report_title = st.text_input("Report title", value=f"EDA Report — {Path(uploaded.name).stem}")
    with c2:
        target_col = st.selectbox(
            "Outcome / target column (optional)",
            options=["— None —"] + columns,
            help="If set, the report treats this as the dependent variable and adds extra correlation context when numeric.",
        )
        target = None if target_col == "— None —" else target_col
    with c3:
        auto_drop_ids = st.checkbox("Exclude ID-like columns from analysis", value=True)

    dt_cols = st.multiselect(
        "Columns to parse as dates (optional)",
        options=columns,
        help="We use pandas to_datetime; invalid values become missing.",
    )
    extra_exclude = st.multiselect(
        "Also exclude these columns from analysis (optional)",
        options=[c for c in columns if c != target],
        help="IDs and keys you do not want in charts or correlations.",
    )

    st.markdown("**What to include in the report**")
    sc1, sc2, sc3 = st.columns(3)
    with sc1:
        sec_missing = st.checkbox("Missing values", value=True)
        sec_dup = st.checkbox("Duplicates", value=True)
        sec_num = st.checkbox("Numeric summaries & plots", value=True)
    with sc2:
        sec_cat = st.checkbox("Categorical summaries & plots", value=True)
        sec_corr = st.checkbox("Correlations", value=True)
        sec_out = st.checkbox("Outliers (IQR)", value=True)

    # Optional EDA engines (may be missing in deployment environments).
    try:
        import ydata_profiling as _ydata_profiling  # noqa: F401

        ydata_ok = True
    except Exception:
        ydata_ok = False

    try:
        import sweetviz as _sweetviz  # noqa: F401

        sweetviz_ok = True
    except Exception:
        sweetviz_ok = False

    with sc3:
        sec_ts = st.checkbox("Time series (if date columns exist)", value=False)
        sec_ydata = st.checkbox(
            "Extra: ydata-profiling HTML (slow)",
            value=False,
            disabled=not ydata_ok,
            help="Disabled because `ydata-profiling` is not installed in this environment.",
        )
        sec_sv = st.checkbox(
            "Extra: Sweetviz HTML (slow)",
            value=False,
            disabled=not sweetviz_ok,
            help="Disabled because `sweetviz` is not installed in this environment.",
        )

    sc4_1, sc4_2 = st.columns(2)
    with sc4_1:
        sec_col_intel = st.checkbox(
            "Column intelligence (semantic hints, PII-style patterns)",
            value=True,
        )
    with sc4_2:
        sec_supervised = st.checkbox(
            "Target-driven EDA (tables/tests when outcome column is set)",
            value=True,
        )

    r1, r2, r3 = st.columns(3)
    with r1:
        corr_thr = st.slider("Flag correlations when |r| ≥", min_value=0.3, max_value=0.95, value=0.7, step=0.05)
    with r2:
        sample_rows = st.number_input(
            "Max rows used for plots (blank = all)",
            min_value=0,
            value=5000,
            help="0 means use all rows for plots. Tables still use the full dataset.",
        )
    with r3:
        high_miss = st.slider("Flag columns missing above (%)", 0, 100, 50)

    sup_levels = st.number_input(
        "Max category levels for supervised chi-square / group plots",
        min_value=5,
        max_value=100,
        value=25,
        help="Skip very high-cardinality categoricals to avoid huge contingency tables.",
    )

    dict_text = st.text_area(
        "Column meanings (optional)",
        height=100,
        placeholder="One per line: ColumnName: Human readable meaning\nOr paste a small YAML mapping.",
    )

    st.subheader("3. Optional: AI summary (LLM)")
    use_llm = st.checkbox("Add an LLM-written summary section (needs API key)", value=False)
    llm_provider = st.selectbox("LLM provider", options=["openai", "gemini"], index=0, disabled=not use_llm)
    default_model = "gpt-4o-mini" if llm_provider == "openai" else "gemini-2.5-pro"
    llm_model = st.text_input("Model", value=default_model, disabled=not use_llm)
    llm_key = st.text_input(
        "API key",
        type="password",
        disabled=not use_llm,
        help="Used only for this session in your browser; not stored on disk.",
    )

    st.subheader("4. Generate & download")
    if st.button("Generate report", type="primary", use_container_width=True):
        out_root = Path(tempfile.mkdtemp(prefix="eda_report_"))
        out_dir = str(out_root / "outputs")

        sections = EDASectionConfig(
            missing_values=sec_missing,
            duplicates=sec_dup,
            numeric_summary=sec_num,
            categorical_summary=sec_cat,
            correlations=sec_corr,
            outliers_iqr=sec_out,
            time_series=sec_ts,
            ydata_profiling_html=sec_ydata,
            sweetviz_html=sec_sv,
            column_intelligence=sec_col_intel,
            supervised_eda=sec_supervised,
        )
        cols_cfg = ColumnConfig(
            target=target,
            datetime_columns=dt_cols if dt_cols else None,
            auto_exclude_id_columns=auto_drop_ids,
            exclude_from_analysis=extra_exclude if extra_exclude else None,
        )
        report_cfg = ReportConfig(
            title=report_title,
            output_dir=out_dir,
            corr_threshold=float(corr_thr),
            high_missing_pct_threshold=float(high_miss),
            sample_rows=None if sample_rows <= 0 else int(sample_rows),
            supervised_max_category_levels=int(sup_levels),
        )
        data_cfg = DataSourceConfig(
            type="sqlite" if is_sqlite else "file",
            path=path,
            table=table_choice if is_sqlite else None,
            query=None,
        )
        llm_cfg = LLMConfig(enabled=use_llm, provider=llm_provider, model=llm_model)
        if use_llm and llm_key.strip():
            import os

            # Map provider -> env var expected by LLMConfig.
            api_env = "OPENAI_API_KEY" if llm_provider == "openai" else "GEMINI_API_KEY"
            llm_cfg.api_key_env = api_env
            os.environ[api_env] = llm_key.strip()

        cfg = AppConfig(
            data=data_cfg,
            columns=cols_cfg,
            sections=sections,
            report=report_cfg,
            llm=llm_cfg,
            dictionary=_parse_dictionary(dict_text),
        )

        with st.spinner("Running analysis and building charts… This may take a minute."):
            try:
                result = run_pipeline(cfg)
            except Exception as e:
                st.error(f"Report failed: {e}")
                shutil.rmtree(out_root, ignore_errors=True)
                return

        st.session_state["last_out_root"] = str(out_root)
        st.session_state["last_result"] = result
        st.success("Report ready.")
        st.info("Open **Report viewer** from the sidebar (or **Open Report viewer now**) to preview the HTML report and download files.")

    out_root_s = st.session_state.get("last_out_root")
    result = st.session_state.get("last_result")
    if out_root_s and result:
        root = Path(out_root_s)
        out_path = root / "outputs"
        html_p = out_path / "report.html"
        md_p = out_path / "report.md"

        dcol1, dcol2, dcol3, dcol4 = st.columns(4)
        if html_p.is_file():
            with dcol1:
                st.download_button(
                    "Download HTML report",
                    data=html_p.read_bytes(),
                    file_name="eda_report.html",
                    mime="text/html",
                    use_container_width=True,
                )
        if md_p.is_file():
            with dcol2:
                st.download_button(
                    "Download Markdown",
                    data=md_p.read_bytes(),
                    file_name="eda_report.md",
                    mime="text/markdown",
                    use_container_width=True,
                )
            with dcol3:
                try:
                    pdf_bytes = markdown_to_pdf_bytes(
                        md_p.read_text(encoding="utf-8"),
                        title="EDA Report",
                    )
                    st.download_button(
                        "Download PDF",
                        data=pdf_bytes,
                        file_name="eda_report.pdf",
                        mime="application/pdf",
                        use_container_width=True,
                    )
                except Exception as e:
                    st.caption(f"PDF export unavailable: {e}")
        buf = io.BytesIO()
        with zipfile.ZipFile(buf, "w", zipfile.ZIP_DEFLATED) as zf:
            for f in out_path.rglob("*"):
                if f.is_file():
                    zf.write(f, arcname=f.relative_to(out_path))
        buf.seek(0)
        with dcol4:
            st.download_button(
                "Download ZIP (all outputs)",
                data=buf.getvalue(),
                file_name="eda_outputs.zip",
                mime="application/zip",
                use_container_width=True,
            )

        if html_p.is_file():
            st.info(
                "Charts in the HTML report use **image files** next to the report. "
                "After you download `eda_report.html`, open it from the same folder as the `assets` folder "
                "(use the **ZIP** download to keep everything together), or open the HTML from inside the unzipped folder."
            )
            st.caption("Full-page preview: sidebar → **Report viewer**.")
            if st.button(
                "Open Report viewer (full page)",
                use_container_width=True,
                key="open_report_viewer_main",
            ):
                st.switch_page(_REPORT_VIEWER_PAGE)


if __name__ == "__main__":
    main()
