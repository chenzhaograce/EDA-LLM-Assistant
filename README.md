# EDA LLM Assistant

Configurable one-shot exploratory data analysis for **tabular data** (CSV, Excel, JSON, Parquet, SQLite). Runs analyses and charts from your settings and exports **Markdown** and **HTML** reports. Optional **LLM** narrative section (OpenAI or Gemini).

---

## Quick start

### 1) Install dependencies

```bash
pip install -r requirements.txt
```

### 2) Copy and edit configuration

```bash
cp config.example.yaml config.yaml
```

Set `data.path` in `config.yaml` to your dataset (e.g. CSV or SQLite path).

### 3) Generate a report

```bash
python eda.py --config config.yaml
```

### Web UI (non-technical users)

From the repository root:

```bash
streamlit run streamlit_app.py
```

Your browser opens **EDA Report Studio**: upload a file, adjust options, then download HTML, Markdown, or a full ZIP of outputs.

### Default output folder (`outputs/`)

- `outputs/report.md` / `outputs/report.html` — Structured report (data dictionary, structure, validation, transformations, visualizations, notes, provenance)
- `outputs/data_dictionary.csv` — Reusable column table (inferred meanings if you provide no official dictionary)
- `outputs/sample_rows.csv` — Sample rows
- `outputs/assets/*.png` — Charts

---

## Configuration (common options)

- **data**
  - **type**: `file` or `sqlite`
  - **path**: Path to the data file
  - **table** / **query**: SQLite only (optional)
- **dictionary** (optional)  
  - Map column name → human definition; overrides inferred text in the data dictionary
- **columns**
  - **include** / **exclude**: Restrict or drop columns (optional)
  - **auto_exclude_id_columns**: Default `true` — removes ID-like columns from stats, plots, correlations, and IQR outliers; they still appear in the dictionary with `in_analysis: no`
  - **exclude_from_analysis**: Extra columns to drop from analysis only
  - **target**: Dependent variable for supervised framing; **never auto-dropped** as an ID
  - **datetime_columns**: Force selected columns to parse as datetimes (optional)
- **report**
  - **corr_threshold**: High-correlation flag threshold (Pearson |r|)
  - **high_missing_pct_threshold**: High-missing column warning threshold (%)
  - **sample_rows**: Row cap for heavy plots (tables still use the full analysis frame)
- **sections**
  - Toggle analysis blocks (missing values, correlations, outliers, etc.)
- **llm**
  - **enabled**: `true` / `false`
  - **provider**: `openai` or `gemini`
  - **api_key_env**: Defaults to `OPENAI_API_KEY`

---

## Project layout (important files)

```text
.
├── eda.py                 # CLI entry (reads config YAML, generates report)
├── streamlit_app.py       # Web UI (Streamlit: upload, customize, download)
├── config.example.yaml    # Example configuration
├── data_connector.py      # Unified loaders (CSV, Excel, JSON, SQLite, DB)
└── eda_llm_assistant/     # Library (analysis, plots, reporting, LLM)
```

## License

See `LICENSE`.
