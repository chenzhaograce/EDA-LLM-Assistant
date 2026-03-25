from __future__ import annotations

from pathlib import Path

import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
import seaborn as sns

from eda_llm_assistant.utils import ensure_dir


def _savefig(path: Path) -> str:
    ensure_dir(path.parent)
    plt.tight_layout()
    plt.savefig(path, dpi=160, bbox_inches="tight")
    plt.close()
    return str(path)


def plot_missing_bar(df: pd.DataFrame, out_dir: Path) -> str | None:
    miss = df.isna().sum()
    miss = miss[miss > 0].sort_values(ascending=False)
    if miss.empty:
        return None
    plt.figure(figsize=(max(8, min(18, 0.6 * len(miss))), 5))
    sns.barplot(x=miss.index.astype(str), y=miss.values, color="#ff7f50")
    plt.title("Missing values count by column")
    plt.xticks(rotation=45, ha="right")
    plt.ylabel("Missing count")
    return _savefig(out_dir / "missing_values_bar.png")


def plot_missing_heatmap(df: pd.DataFrame, out_dir: Path, max_cols: int = 40) -> str | None:
    miss_cols = df.columns[df.isna().any()].tolist()
    if not miss_cols:
        return None
    miss_cols = miss_cols[:max_cols]
    plt.figure(figsize=(12, max(4, min(10, 0.2 * len(df)))))
    sns.heatmap(df[miss_cols].isna(), cbar=True, cmap="viridis")
    plt.title("Missing values heatmap (subset of columns)")
    plt.xlabel("Columns")
    plt.ylabel("Rows")
    return _savefig(out_dir / "missing_values_heatmap.png")


def plot_numeric_distributions(df: pd.DataFrame, numeric_cols: list[str], out_dir: Path, max_cols: int = 12) -> str | None:
    cols = [c for c in numeric_cols if c in df.columns][:max_cols]
    if not cols:
        return None
    n = len(cols)
    n_cols = 3
    n_rows = int(np.ceil(n / n_cols))
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(5 * n_cols, 3.8 * n_rows))
    axes = axes.flatten()
    for i, c in enumerate(cols):
        ax = axes[i]
        s = df[c].dropna()
        if s.empty:
            ax.set_visible(False)
            continue
        sns.histplot(s, bins=30, kde=True, ax=ax, color="#4c72b0")
        ax.set_title(f"{c}")
    for j in range(n, len(axes)):
        axes[j].set_visible(False)
    fig.suptitle("Numeric distributions (hist + KDE)", y=1.02)
    return _savefig(out_dir / "numeric_distributions.png")


def plot_categorical_top(df: pd.DataFrame, cat_cols: list[str], out_dir: Path, top_n: int = 15, max_cols: int = 8) -> str | None:
    cols = [c for c in cat_cols if c in df.columns][:max_cols]
    if not cols:
        return None
    n = len(cols)
    n_cols = 2
    n_rows = int(np.ceil(n / n_cols))
    fig, axes = plt.subplots(n_rows, n_cols, figsize=(8 * n_cols, 3.8 * n_rows))
    axes = axes.flatten()
    for i, c in enumerate(cols):
        ax = axes[i]
        vc = df[c].astype("object").value_counts(dropna=False).head(top_n)
        sns.barplot(x=vc.values, y=vc.index.astype(str), ax=ax, color="#55a868")
        ax.set_title(f"Top {min(top_n, len(vc))}: {c}")
        ax.set_xlabel("Count")
        ax.set_ylabel("")
    for j in range(n, len(axes)):
        axes[j].set_visible(False)
    fig.suptitle("Categorical top values", y=1.02)
    return _savefig(out_dir / "categorical_top_values.png")


def plot_corr_heatmap(corr: pd.DataFrame, out_dir: Path, max_features: int = 30) -> str | None:
    if corr is None or corr.empty:
        return None
    c = corr.copy()
    if c.shape[0] > max_features:
        c = c.iloc[:max_features, :max_features]
    plt.figure(figsize=(12, 9))
    mask = np.triu(np.ones_like(c, dtype=bool))
    sns.heatmap(c, cmap="coolwarm", center=0, mask=mask, square=True, cbar_kws={"shrink": 0.8})
    plt.title("Correlation heatmap (upper triangle masked)")
    return _savefig(out_dir / "correlation_heatmap.png")


def plot_boxplots(df: pd.DataFrame, numeric_cols: list[str], out_dir: Path, max_features: int = 25) -> str | None:
    cols = [c for c in numeric_cols if c in df.columns][:max_features]
    if not cols:
        return None
    plt.figure(figsize=(max(10, 0.6 * len(cols)), 5))
    sns.boxplot(data=df[cols], orient="h", color="#c44e52")
    plt.title("Boxplots (numeric columns)")
    return _savefig(out_dir / "numeric_boxplots.png")


def plot_time_series_counts(
    df: pd.DataFrame,
    datetime_cols: list[str],
    out_dir: Path,
    max_cols: int = 3,
    freq: str = "D",
) -> list[str]:
    cols = [c for c in datetime_cols if c in df.columns][:max_cols]
    paths: list[str] = []
    if not cols:
        return paths

    for c in cols:
        s = pd.to_datetime(df[c], errors="coerce").dropna()
        if s.empty:
            continue

        # bucket by freq
        ts = s.dt.floor(freq).value_counts().sort_index()
        plt.figure(figsize=(12, 4))
        ts.plot(kind="line", color="#4c72b0")
        plt.title(f"Row count over time ({c}, freq={freq})")
        plt.xlabel("Time")
        plt.ylabel("Count")
        paths.append(_savefig(out_dir / f"time_series_count_{c}.png"))

    return paths


def plot_target_distribution(
    df: pd.DataFrame,
    target: str,
    *,
    is_numeric: bool,
    out_dir: Path,
) -> str | None:
    """Histogram for numeric target; bar counts for categorical."""
    if target not in df.columns:
        return None
    if is_numeric:
        s = pd.to_numeric(df[target], errors="coerce").dropna()
        if s.empty:
            return None
        plt.figure(figsize=(8, 4))
        sns.histplot(s, bins=min(40, max(10, s.nunique())), kde=True, color="#4c72b0")
        plt.title(f"Target distribution: {target}")
        plt.xlabel(target)
        return _savefig(out_dir / "supervised_target_distribution.png")
    vc = df[target].astype("object").value_counts(dropna=False).head(30)
    if vc.empty:
        return None
    plt.figure(figsize=(10, max(4, 0.35 * len(vc))))
    sns.barplot(x=vc.values, y=vc.index.astype(str), color="#55a868")
    plt.title(f"Target category counts: {target}")
    plt.xlabel("Count")
    return _savefig(out_dir / "supervised_target_distribution.png")


def plot_supervised_bivariate_boxplots(
    df: pd.DataFrame,
    target: str,
    *,
    target_is_numeric: bool,
    numeric_cols: list[str],
    categorical_cols: list[str],
    out_dir: Path,
    max_features: int = 4,
) -> str | None:
    """
    Categorical target: boxplot of each numeric feature by target level.
    Numeric target: boxplot of target across levels of each categorical feature.
    """
    if target_is_numeric:
        feats = [c for c in categorical_cols if c != target][:max_features]
        if not feats:
            return None
        n = len(feats)
        fig, axes = plt.subplots(n, 1, figsize=(10, 3.5 * n))
        if n == 1:
            axes = [axes]
        for ax, cat in zip(axes, feats):
            sub = df[[target, cat]].dropna()
            sub = sub.assign(_y=pd.to_numeric(sub[target], errors="coerce")).dropna(subset=["_y"])
            if sub.empty:
                ax.set_visible(False)
                continue
            sns.boxplot(data=sub, x=cat, y="_y", ax=ax, color="#c44e52")
            ax.set_ylabel(target)
            ax.set_xlabel(cat)
            ax.tick_params(axis="x", rotation=45)
        fig.suptitle(f"Numeric target `{target}` by categorical features", y=1.02)
        return _savefig(out_dir / "supervised_bivariate_boxplots.png")
    feats = [c for c in numeric_cols if c != target][:max_features]
    if not feats:
        return None
    n = len(feats)
    fig, axes = plt.subplots(n, 1, figsize=(10, 3.5 * n))
    if n == 1:
        axes = [axes]
    for ax, num in zip(axes, feats):
        sub = df[[target, num]].dropna()
        sub = sub.assign(_x=pd.to_numeric(sub[num], errors="coerce")).dropna(subset=["_x"])
        if sub.empty:
            ax.set_visible(False)
            continue
        sns.boxplot(data=sub, x=target, y="_x", ax=ax, color="#4c72b0")
        ax.set_ylabel(num)
        ax.set_xlabel(target)
        ax.tick_params(axis="x", rotation=45)
    fig.suptitle(f"Numeric features by categorical target `{target}`", y=1.02)
    return _savefig(out_dir / "supervised_bivariate_boxplots.png")

