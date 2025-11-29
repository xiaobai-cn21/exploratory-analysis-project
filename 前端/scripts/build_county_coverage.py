"""
Generate county-level coverage metrics for the NY AP/IB project.

This script reads the cleaned assessment dataset and aggregates AP/IB
coverage indicators per county, outputting the results as
`cleaned_data/county_coverage.json`.  The JSON file is consumed by the
GeoChart on `research2.html`.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLEANED_DATA = PROJECT_ROOT / "static" / "data" / "csv" / "AP_IB_Assessment_2024_level2_County_cleaned.csv"
OUTPUT_PATH = PROJECT_ROOT / "static" / "js" / "county_coverage.json"


def _ensure_dataset() -> pd.DataFrame:
    """Load the cleaned dataset and retain the rows needed for county analysis."""
    if not CLEANED_DATA.exists():
        raise FileNotFoundError(f"Missing cleaned dataset: {CLEANED_DATA}")

    df = pd.read_csv(CLEANED_DATA, na_values="-")

    # Focus on county-level aggregates that contain county identifiers.
    df = df[df["aggregation_type"].str.lower() == "county"]
    df["COUNTY_CODE"] = pd.to_numeric(df["COUNTY_CODE"], errors="coerce")
    df = df.dropna(subset=["COUNTY_CODE"])
    df["COUNTY_CODE"] = df["COUNTY_CODE"].astype(int).astype(str).str.zfill(3)

    df["tested_student_cnt"] = (
        pd.to_numeric(df["tested_student_cnt"], errors="coerce").fillna(0)
    )

    return df


def _zscore(series: pd.Series) -> pd.Series:
    """Safely compute the z-score, even when std == 0."""
    std = series.std(ddof=0)
    if std == 0 or pd.isna(std):
        return series - series.mean()
    return (series - series.mean()) / std


def build_county_metrics(df: pd.DataFrame) -> pd.DataFrame:
    """Aggregate tested counts and course diversity per county."""
    grouped = (
        df.groupby(["COUNTY_CODE", "COUNTY_NAME"])
        .agg(
            tested=("tested_student_cnt", "sum"),
            course_count=("ITEM_DESC", pd.Series.nunique),
        )
        .reset_index()
    )

    grouped["tested_z"] = _zscore(grouped["tested"])
    grouped["courses_z"] = _zscore(grouped["course_count"])
    grouped["coverage_score"] = grouped[["tested_z", "courses_z"]].mean(axis=1)

    cov_min = grouped["coverage_score"].min()
    cov_max = grouped["coverage_score"].max()
    if cov_min == cov_max:
        grouped["coverage_score"] = 0.5  # identical coverage; show neutral color
    else:
        grouped["coverage_score"] = (
            (grouped["coverage_score"] - cov_min) / (cov_max - cov_min)
        )

    return grouped[["COUNTY_CODE", "COUNTY_NAME", "tested", "course_count", "coverage_score"]]


def export_records(df: pd.DataFrame) -> None:
    """Write the aggregated metrics to the JSON file."""
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)

    records = [
        {
            "county_code": row.COUNTY_CODE,
            "county_name": row.COUNTY_NAME,
            "tested": int(row.tested),
            "course_count": int(row.course_count),
            "coverage_score": round(float(row.coverage_score), 4),
        }
        for row in df.itertuples(index=False)
    ]

    with OUTPUT_PATH.open("w", encoding="utf-8") as fp:
        json.dump(records, fp, ensure_ascii=False, indent=2)

    print(f"Saved {len(records)} county records -> {OUTPUT_PATH}")


def main() -> None:
    dataset = _ensure_dataset()
    metrics = build_county_metrics(dataset)
    export_records(metrics)


if __name__ == "__main__":
    main()

