"""
Generate resource vs. outcome metrics for the dual-axis scatter plot.

The script aggregates county-level resource coverage (based on tested
student counts and course diversity) and proficiency outcomes from the
cleaned assessment dataset, then exports the top counties to a JSON file
consumed by the Research 02 web page.
"""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path
from typing import List

import pandas as pd

PROJECT_ROOT = Path(__file__).resolve().parent.parent
CLEANED_DATA = PROJECT_ROOT / "static" / "data" / "csv" / "AP_IB_Assessment_2024_level2_County_cleaned.csv"
OUTPUT_PATH = PROJECT_ROOT / "static" / "js" / "resource_outcome_scatter.json"
MAX_POINTS = 35  # limit to most resourced counties for visual clarity


def _ensure_dataset() -> pd.DataFrame:
    if not CLEANED_DATA.exists():
        raise FileNotFoundError(f"Missing cleaned dataset: {CLEANED_DATA}")

    df = pd.read_csv(CLEANED_DATA, na_values="-")
    df = df[df["aggregation_type"].str.lower() == "county"]
    df["COUNTY_CODE"] = pd.to_numeric(df["COUNTY_CODE"], errors="coerce")
    df = df.dropna(subset=["COUNTY_CODE"])
    df["COUNTY_CODE"] = df["COUNTY_CODE"].astype(int).astype(str).str.zfill(3)

    numeric_cols = ["tested_student_cnt", "proficient_student_cnt"]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)

    df["NRC_CODE"] = pd.to_numeric(df["NRC_CODE"], errors="coerce")

    return df


def _zscore(series: pd.Series) -> pd.Series:
    std = series.std(ddof=0)
    if std == 0 or pd.isna(std):
        return series - series.mean()
    return (series - series.mean()) / std


@dataclass
class CountyRecord:
    county_code: str
    county_name: str
    nrc_code: int | None
    nrc_desc: str | None
    tested: int
    course_count: int
    proficiency_rate: float
    coverage_score: float

    def as_dict(self, rank: int) -> dict:
        return {
            "rank": rank,
            "county_code": self.county_code,
            "county_name": self.county_name.title(),
            "nrc_code": self.nrc_code,
            "nrc_desc": self.nrc_desc.title() if isinstance(self.nrc_desc, str) else None,
            "tested": self.tested,
            "course_count": self.course_count,
            "coverage_score": round(self.coverage_score, 4),
            "resource_index": round(self.coverage_score * 100, 1),
            "proficiency_rate": round(self.proficiency_rate * 100, 1),
        }


def build_records(df: pd.DataFrame) -> List[CountyRecord]:
    grouped = (
        df.groupby(["COUNTY_CODE", "COUNTY_NAME"])
        .agg(
            tested=("tested_student_cnt", "sum"),
            proficient=("proficient_student_cnt", "sum"),
            course_count=("ITEM_DESC", pd.Series.nunique),
            nrc_code=("NRC_CODE", "first"),
            nrc_desc=("NRC_DESC", "first"),
        )
        .reset_index()
    )

    grouped["proficiency_rate"] = grouped.apply(
        lambda row: (row.proficient / row.tested) if row.tested > 0 else 0, axis=1
    )

    grouped["tested_z"] = _zscore(grouped["tested"])
    grouped["courses_z"] = _zscore(grouped["course_count"])
    grouped["coverage_score"] = grouped[["tested_z", "courses_z"]].mean(axis=1)

    cov_min = grouped["coverage_score"].min()
    cov_max = grouped["coverage_score"].max()
    if cov_min == cov_max:
        grouped["coverage_score"] = 0.5
    else:
        grouped["coverage_score"] = (
            (grouped["coverage_score"] - cov_min) / (cov_max - cov_min)
        )

    grouped = grouped.sort_values("coverage_score", ascending=False).reset_index(drop=True)

    records: List[CountyRecord] = []
    for row in grouped.itertuples(index=False):
        records.append(
            CountyRecord(
                county_code=row.COUNTY_CODE,
                county_name=row.COUNTY_NAME,
                nrc_code=int(row.nrc_code) if pd.notna(row.nrc_code) else None,
                nrc_desc=row.nrc_desc if isinstance(row.nrc_desc, str) else None,
                tested=int(row.tested),
                course_count=int(row.course_count),
                proficiency_rate=float(row.proficiency_rate),
                coverage_score=float(row.coverage_score),
            )
        )
    return records


def export_records(records: List[CountyRecord]) -> None:
    OUTPUT_PATH.parent.mkdir(parents=True, exist_ok=True)
    limited = records[:MAX_POINTS]

    payload = [record.as_dict(rank=index + 1) for index, record in enumerate(limited)]

    with OUTPUT_PATH.open("w", encoding="utf-8") as fp:
        json.dump(payload, fp, ensure_ascii=False, indent=2)

    print(f"Saved {len(payload)} scatter records -> {OUTPUT_PATH}")


def main() -> None:
    dataset = _ensure_dataset()
    records = build_records(dataset)
    export_records(records)


if __name__ == "__main__":
    main()

