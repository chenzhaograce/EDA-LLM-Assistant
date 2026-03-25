"""
Supervised / question-driven EDA when a target column is configured.
Uses scipy for chi-square and Kruskal–Wallis; assumptions are noted in outputs, not hidden.
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from eda_llm_assistant.analyzer import ColumnTypes


def run_supervised_eda(
    df: pd.DataFrame,
    target: str | None,
    col_types: ColumnTypes,
    *,
    max_chi2_levels: int = 25,
    min_group_size: int = 2,
) -> dict[str, Any] | None:
    """
    Returns a dict of tables and test results for the report, or None if no valid target.
    """
    if not target or target not in df.columns:
        return None

    out: dict[str, Any] = {
        "target": target,
        "kind": None,
        "target_profile": None,
        "numeric_by_categorical_target": [],
        "categorical_group_stats_numeric_target": [],
        "chi_square_tests": [],
        "kruskal_wallis_tests": [],
    }

    if target in col_types.numeric:
        out["kind"] = "numeric"
        s = pd.to_numeric(df[target], errors="coerce")
        prof = s.describe(percentiles=[0.05, 0.25, 0.5, 0.75, 0.95])
        out["target_profile"] = prof.to_frame(name=target)

        for cat in col_types.categorical:
            if cat == target:
                continue
            if df[cat].nunique(dropna=False) > max_chi2_levels:
                continue
            sub = df[[target, cat]].dropna()
            sub = sub.assign(_y=pd.to_numeric(sub[target], errors="coerce")).dropna(subset=["_y"])
            if sub.empty:
                continue
            g = (
                sub.groupby(cat, observed=True)["_y"]
                .agg(count="count", mean="mean", median="median", std="std")
                .reset_index()
                .sort_values("mean", ascending=False)
            )
            out["categorical_group_stats_numeric_target"].append({"feature": cat, "table": g})

            groups = [grp["_y"].values for _, grp in sub.groupby(cat, observed=True) if len(grp) >= min_group_size]
            if len(groups) >= 2:
                try:
                    from scipy.stats import kruskal

                    stat, pval = kruskal(*groups)
                    out["kruskal_wallis_tests"].append(
                        {
                            "feature": cat,
                            "statistic": float(stat),
                            "pvalue": float(pval),
                            "n_groups": len(groups),
                            "note": (
                                "Kruskal–Wallis H-test: nonparametric comparison of numeric target across groups. "
                                "Does not assume normality; still assumes independent observations. "
                                "Small groups or ties can affect p-values."
                            ),
                        }
                    )
                except Exception as e:
                    out["kruskal_wallis_tests"].append(
                        {"feature": cat, "error": str(e), "note": "Test not computed."}
                    )

    elif target in col_types.categorical or target in col_types.boolean:
        out["kind"] = "categorical"
        vc = df[target].astype("object").value_counts(dropna=False)
        prof = vc.reset_index()
        prof.columns = [target, "count"]
        out["target_profile"] = prof

        for num in col_types.numeric:
            if num == target:
                continue
            sub = df[[target, num]].dropna()
            if sub.empty:
                continue
            sub = sub.assign(_x=pd.to_numeric(sub[num], errors="coerce")).dropna(subset=["_x"])
            if sub.empty:
                continue
            g = (
                sub.groupby(target, observed=True)["_x"]
                .agg(count="count", mean="mean", median="median", std="std")
                .reset_index()
                .sort_values("mean", ascending=False)
            )
            out["numeric_by_categorical_target"].append({"feature": num, "table": g})

        for cat in col_types.categorical:
            if cat == target:
                continue
            nu_t = df[target].nunique(dropna=False)
            nu_c = df[cat].nunique(dropna=False)
            if nu_t > max_chi2_levels or nu_c > max_chi2_levels:
                continue
            ct = pd.crosstab(df[target], df[cat], dropna=False)
            if ct.size == 0 or ct.shape[0] < 2 or ct.shape[1] < 2:
                continue
            try:
                from scipy.stats import chi2_contingency

                chi2, p, dof, expected = chi2_contingency(ct.values)
                small_exp = float((expected < 5).sum()) / max(expected.size, 1)
                out["chi_square_tests"].append(
                    {
                        "feature": cat,
                        "chi2": float(chi2),
                        "pvalue": float(p),
                        "dof": int(dof),
                        "table_shape": f"{ct.shape[0]}×{ct.shape[1]}",
                        "pct_small_expected_lt5": round(small_exp * 100, 1),
                        "note": (
                            "Pearson chi-square test of independence on contingency table. "
                            "Assumes independent rows; expected counts should be ≥5 in most cells for "
                            "asymptotic p-values to be reliable. High cardinality inflates chi² and power."
                        ),
                    }
                )
            except Exception as e:
                out["chi_square_tests"].append({"feature": cat, "error": str(e)})
    else:
        out["kind"] = "other_dtype"
        vc = df[target].astype("object").value_counts(dropna=False).head(50)
        prof = vc.reset_index()
        prof.columns = ["target_level", "count"]
        out["target_profile"] = prof

    return out
