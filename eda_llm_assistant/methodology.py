"""Short, reusable explanations of methods used in EDA reports (what + how)."""


def paragraph_missing_values() -> str:
    return (
        "**Method — missing values.** We count missing entries with `pandas.isna()` "
        "on each column (treats `NaN`/`None` and nullable types as missing). "
        "For each column we report `missing_count` and `missing_pct = missing_count / n_rows * 100`. "
        "Bar and heatmap visuals help scan which columns and rows are affected."
    )


def paragraph_duplicates() -> str:
    return (
        "**Method — duplicate rows.** We use `DataFrame.duplicated()` with default settings "
        "(all columns, keep first occurrence as non-duplicate). The count is the number of rows "
        "that repeat an earlier row exactly."
    )


def paragraph_type_inference() -> str:
    return (
        "**Method — numeric vs categorical vs datetime.** We rely on pandas dtypes after load: "
        "`select_dtypes` for numeric (`int`, `float`), boolean, `datetime64`, and object/category "
        "strings. Columns listed under `columns.datetime_columns` in the config are coerced with "
        "`pandas.to_datetime(..., errors='coerce')`, which parses valid dates and turns failures into "
        "missing (`NaT`)."
    )


def paragraph_identifier_exclusion() -> str:
    return (
        "**Method — excluding identifiers from analysis.** Surrogate keys (e.g. `CustomerID`, `id`, "
        "`order_id`) are usually **not modeling features**: they inflate numeric summaries, distort "
        "correlations/outliers, and leak train/test identity if misused. By default we **drop** "
        "columns that match ID-like name patterns **before** validation plots, correlations, IQR "
        "outliers, and distributional plots. They still appear in the **data dictionary** with "
        "`in_analysis = no`. The configured `columns.target` is never auto-dropped. Disable with "
        "`columns.auto_exclude_id_columns: false` or add explicit drops with `exclude_from_analysis`."
    )


def paragraph_describe() -> str:
    return (
        "**Method — numeric summaries.** For numeric columns we use `DataFrame.describe()` with extra "
        "percentiles (e.g. 1%, 5%, 50%, 95%, 99%). That gives count, mean, std, min/max, and "
        "distribution quantiles. Skewness and kurtosis use the usual pandas `.skew()` and "
        "`.kurtosis()` on numeric columns."
    )


def paragraph_categorical_top() -> str:
    return (
        "**Method — categorical top values.** For object/category columns we compute `value_counts` "
        "(including missing as its own bucket) and show the top N categories by frequency."
    )


def paragraph_iqr_outliers() -> str:
    return (
        "**Method — outliers (Tukey / IQR rule).** For each numeric column we compute:\n\n"
        "- **Q1** = 25th percentile, **Q3** = 75th percentile.\n"
        "- **IQR** = Q3 − Q1 (interquartile range: spread of the middle 50% of the data).\n"
        "- **Lower fence** = Q1 − 1.5 × IQR, **upper fence** = Q3 + 1.5 × IQR.\n\n"
        "Any row where the value is **below the lower fence or above the upper fence** is counted "
        "as an IQR-based outlier for that column. "
        "If IQR is 0 (or undefined), we skip flagging for that column to avoid degenerate fences. "
        "This rule is descriptive, not a claim that those points are data entry errors."
    )


def paragraph_pearson_correlation() -> str:
    return (
        "**Method — Pearson correlation.** For numeric columns we compute the pairwise Pearson "
        "correlation matrix with `DataFrame.corr()` (default method `pearson`). "
        "Each entry is between −1 and 1: linear association, not causation. "
        "We flag pairs with absolute correlation ≥ `report.corr_threshold` as “high” for review."
    )


def paragraph_plots_numeric() -> str:
    return (
        "**Plots — numeric.** Histograms with KDE (`seaborn.histplot`, `kde=True`) show shape and "
        "density. Horizontal boxplots summarize quartiles and show points beyond the whiskers "
        "(useful alongside the IQR counts above)."
    )


def paragraph_plots_categorical() -> str:
    return (
        "**Plots — categorical.** Bar charts show the **top categories** per column (same logic as "
        "value_counts, limited to a max number of columns for readability)."
    )


def paragraph_plots_correlation() -> str:
    return (
        "**Plots — correlation.** The heatmap shows the Pearson matrix; the **upper triangle is masked** "
        "so each pair appears once and the diagonal is easier to scan."
    )


def paragraph_plots_time_series() -> str:
    return (
        "**Plots — time indexing.** For datetime columns we `floor` timestamps to a daily frequency "
        "and count rows per bucket, then plot a line chart of counts over time (volume / recording "
        "pattern), not a metric aggregation unless a value column is chosen separately."
    )


def paragraph_quality_heuristics() -> str:
    return (
        "**Method — basic error / consistency checks (heuristic).** We do not know your domain rules, "
        "so we flag:\n\n"
        "- **Constant columns** (≤1 distinct non-null value) — often IDs wrongly typed or placeholders.\n"
        "- **High missing rate** — columns above a configurable fraction of missing cells.\n"
        "- **Mixed-type object columns** — sample of values converts to numeric for some but not all; "
        "may indicate dirty strings or combined formats.\n\n"
        "These are starting points for manual or business validation, not automatic corrections."
    )


def paragraph_transformation_notes() -> str:
    return (
        "**Transformations.** This report separates: (1) **applied in this run** — only what the "
        "pipeline actually changed (e.g. datetime coercion); (2) **suggested** — common next steps "
        "from skew, missingness, or cardinality (you choose whether to implement in modeling pipelines)."
    )


def paragraph_other_considerations() -> str:
    return (
        "**Other considerations.** "
        "• **Sampling:** if `report.sample_rows` is set, plots use a random sample for speed; "
        "tables and validation use the full dataframe unless noted. "
        "• **Correlation:** Pearson assumes roughly linear relationships; for skewed or ordinal data "
        "consider Spearman or dedicated tests. "
        "• **Causality:** association in EDA does not imply cause. "
        "• **Privacy:** reports may contain sample rows; scrub sensitive columns before sharing."
    )


def paragraph_dictionary_inference(user_provided_count: int) -> str:
    base = (
        "We build a **data dictionary** table for every column: dtype, distinct count, missing rate, "
        "an **inferred meaning** from name patterns (token heuristics, not domain truth), and "
        "whether the column is treated as a candidate **independent** variable or the configured **target**. "
        "You can override any column with `dictionary:` in the YAML config (column name → definition)."
    )
    if user_provided_count > 0:
        base += f" **This run uses {user_provided_count} user-provided definition(s)**; other columns stay inferred."
    return base


def paragraph_dependent_independent(target: str | None) -> str:
    if target:
        return (
            f"**Dependent vs independent variables.** The config marks **`{target}`** as the "
            "**dependent (target) variable** for supervised analysis. All other analyzed columns are "
            "treated as **candidate independent variables** (predictors / features). "
            "Causal interpretation still requires study design, not EDA alone."
        )
    return (
        "**Dependent vs independent variables.** No `columns.target` is set. "
        "All columns are summarized symmetrically; for supervised tasks, set `target` to distinguish "
        "the outcome variable from predictors."
    )


def paragraph_column_intelligence() -> str:
    return (
        "**Column intelligence (heuristic).** We label each loaded column with a **semantic guess** "
        "(money-like, count-like, high-cardinality text, etc.) using **dtype**, **name tokens**, and "
        "**cardinality**. Separately, on **string** columns we scan a sample for **PII-like patterns** "
        "(email, phone, SSN-shaped strings, Luhn-passing digit strings that may resemble payment cards). "
        "These are **screening hints**, not legal classification—expect false positives/negatives."
    )


def paragraph_supervised_eda() -> str:
    return (
        "**Supervised / target-driven EDA.** With `columns.target` set, we add tables that relate "
        "the target to other features: for a **numeric** target, group summaries and Kruskal–Wallis "
        "tests vs categoricals (nonparametric, independent groups); for a **categorical** target, "
        "numeric summaries by target level and chi-square tests vs other categoricals (independence on "
        "contingency tables). **Assumptions** are stated with each test. High-cardinality categoricals "
        "are skipped for chi-square (configurable cap) to avoid unstable tables."
    )
