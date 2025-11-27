import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List

import numpy as np
import pandas as pd


@dataclass
class FairnessEDAConfig:
    source_path: Path = Path("analysis_results/AP_IB_Assessment_2024_cleaned_20251124_170918.csv")
    output_dir: Path = Path("analysis_results/fairness")
    figures_dir: Path = Path("figures/fairness")
    min_valid_tested: int = 5


class FairnessEDA:
    def __init__(self, config: FairnessEDAConfig):
        self.config = config
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.config.figures_dir.mkdir(parents=True, exist_ok=True)
        self.df = pd.DataFrame()

    # -----------------------------------------------------
    # Data loading & feature engineering
    # -----------------------------------------------------
    def load_and_prepare(self) -> None:
        """Load cleaned CSV, normalize numeric fields, create derived metrics."""
        numeric_cols = [
            "tested_student_cnt",
            "proficient_student_cnt",
            "level1_cnt",
            "level2_cnt",
            "level3_cnt",
            "level4_cnt",
            "level5_cnt",
            "level6_cnt",
            "level7_cnt",
        ]

        dtype_overrides: Dict[str, str] = {
            "REPORT_SCHOOL_YEAR": "string",
            "aggregation_type": "string",
            "aggregation_code": "string",
            "aggregation_name": "string",
            "INST_ID": "string",
            "LEA_BEDS": "string",
            "LEA_NAME": "string",
            "NRC_DESC": "string",
            "COUNTY_CODE": "string",
            "COUNTY_NAME": "string",
            "NYC_IND": "string",
            "SUBGROUP_CODE": "string",
            "SUBGROUP_NAME": "string",
            "APIB_IND": "string",
            "SUBJECT_AREA": "string",
            "STATE_CODE": "string",
            "ITEM_DESC": "string",
            "GRADE_LEVEL": "string",
        }

        df = pd.read_csv(self.config.source_path, dtype=dtype_overrides)

        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # Derived counts
        df["is_suppressed"] = df["proficient_student_cnt"].isna()

        # Replace NaN level counts with 0 for arithmetic (suppressed rows stay NaN until fill)
        level_cols = [c for c in numeric_cols if c.startswith("level")]
        df[level_cols] = df[level_cols].fillna(0)

        def calc_proficiency(row):
            if row["APIB_IND"] == "AP":
                return row["level3_cnt"] + row["level4_cnt"] + row["level5_cnt"]
            return row["level4_cnt"] + row["level5_cnt"] + row["level6_cnt"] + row["level7_cnt"]

        def calc_high_score(row):
            if row["APIB_IND"] == "AP":
                return row["level4_cnt"] + row["level5_cnt"]
            return row["level5_cnt"] + row["level6_cnt"] + row["level7_cnt"]

        df["proficiency_cnt_calc"] = df.apply(calc_proficiency, axis=1)
        df["high_score_cnt_calc"] = df.apply(calc_high_score, axis=1)
        df["valid_perf"] = (df["tested_student_cnt"] >= self.config.min_valid_tested) & (df["proficiency_cnt_calc"] > 0)
        df["tested_cnt_valid"] = np.where(df["valid_perf"], df["tested_student_cnt"], 0)
        df["proficiency_cnt_valid"] = np.where(df["valid_perf"], df["proficiency_cnt_calc"], 0)
        df["high_score_cnt_valid"] = np.where(df["valid_perf"], df["high_score_cnt_calc"], 0)

        self.df = df

    # -----------------------------------------------------
    # Helper aggregation utilities
    # -----------------------------------------------------
    def _agg_numeric(self, group_cols: List[str], numeric_cols: List[str], df: pd.DataFrame) -> pd.DataFrame:
        agg_dict = {col: "sum" for col in numeric_cols}
        agg_df = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()
        return agg_df

    def _add_rate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        df = df.copy()
        df["proficiency_rate"] = np.where(df["tested_cnt_valid"] > 0, df["proficiency_cnt_valid"] / df["tested_cnt_valid"], np.nan)
        df["high_score_rate"] = np.where(df["tested_cnt_valid"] > 0, df["high_score_cnt_valid"] / df["tested_cnt_valid"], np.nan)
        df["valid_coverage"] = np.where(df["tested_student_cnt"] > 0, df["tested_cnt_valid"] / df["tested_student_cnt"], 0)
        return df

    def _write_dataset(self, name: str, df: pd.DataFrame) -> None:
        csv_path = self.config.output_dir / f"{name}.csv"
        json_path = self.config.output_dir / f"{name}.json"
        df.to_csv(csv_path, index=False)
        df.round(6).to_dict(orient="records")
        json_path.write_text(json.dumps(df.round(6).to_dict(orient="records"), ensure_ascii=False, indent=2), encoding="utf-8")

    # -----------------------------------------------------
    # Aggregations per analysis theme
    # -----------------------------------------------------
    def export_statewide_overview(self) -> None:
        df_state = self.df[(self.df["aggregation_index"] == 0) & (self.df["SUBGROUP_NAME"] == "All Students")]
        group_cols = ["APIB_IND"]
        numeric_cols = [
            "tested_student_cnt",
            "tested_cnt_valid",
            "proficiency_cnt_valid",
            "high_score_cnt_valid",
            "is_suppressed",
        ]
        state_df = self._agg_numeric(group_cols, numeric_cols, df_state)
        state_df = self._add_rate_columns(state_df)
        record_counts = df_state.groupby("APIB_IND")["SUBGROUP_NAME"].count().rename("record_count").reset_index()
        state_df = state_df.merge(record_counts, on="APIB_IND", how="left")
        state_df = state_df.rename(columns={"is_suppressed": "suppressed_records"})
        state_df["suppressed_share"] = np.where(
            state_df["record_count"] > 0, state_df["suppressed_records"] / state_df["record_count"], 0
        )
        rename_map = {
            "tested_student_cnt": "tested_total",
            "tested_cnt_valid": "tested_valid",
            "proficiency_cnt_valid": "proficient_valid",
            "high_score_cnt_valid": "high_score_valid",
        }
        state_df = state_df.rename(columns=rename_map)
        self._write_dataset("statewide_overview", state_df)

    def export_level_distribution(self) -> None:
        df_state = self.df[(self.df["aggregation_index"] == 0) & (self.df["SUBGROUP_NAME"] == "All Students")]
        level_cols = ["level1_cnt", "level2_cnt", "level3_cnt", "level4_cnt", "level5_cnt", "level6_cnt", "level7_cnt"]
        agg_df = df_state.groupby("APIB_IND")[level_cols].sum().reset_index()
        tidy = agg_df.melt(id_vars="APIB_IND", var_name="level", value_name="student_count")
        tidy["level_numeric"] = tidy["level"].str.extract(r"(\d+)").astype(int)
        tidy = tidy.sort_values(["APIB_IND", "level_numeric"])
        tidy["share"] = tidy.groupby("APIB_IND")["student_count"].transform(
            lambda x: np.where(x.sum() > 0, x / x.sum(), np.nan)
        )
        tidy = tidy.drop(columns=["level_numeric"])
        self._write_dataset("state_level_distribution", tidy)

    def export_demographic_summaries(self) -> None:
        df_state = self.df[self.df["aggregation_index"] == 0]
        group_cols = ["APIB_IND", "SUBGROUP_CODE", "SUBGROUP_NAME"]
        numeric_cols = [
            "tested_student_cnt",
            "tested_cnt_valid",
            "proficiency_cnt_valid",
            "high_score_cnt_valid",
            "is_suppressed",
        ]
        demo_df = self._agg_numeric(group_cols, numeric_cols, df_state)
        demo_df = self._add_rate_columns(demo_df)
        totals = demo_df[demo_df["SUBGROUP_NAME"] == "All Students"][["APIB_IND", "tested_student_cnt", "proficiency_rate"]].rename(
            columns={"tested_student_cnt": "all_students_tested", "proficiency_rate": "all_students_rate"}
        )
        demo_df = demo_df.merge(totals, on="APIB_IND", how="left")
        demo_df["participation_share"] = np.where(
            demo_df["all_students_tested"] > 0, demo_df["tested_student_cnt"] / demo_df["all_students_tested"], np.nan
        )
        demo_df["gap_vs_all"] = demo_df["proficiency_rate"] - demo_df["all_students_rate"]
        self._write_dataset("demographic_summary", demo_df)

    def export_nrc_summary(self) -> None:
        df_nrc = self.df[(self.df["aggregation_index"] == 1) & (self.df["SUBGROUP_NAME"] == "All Students")]
        group_cols = ["APIB_IND", "NRC_CODE", "NRC_DESC"]
        numeric_cols = [
            "tested_student_cnt",
            "tested_cnt_valid",
            "proficiency_cnt_valid",
            "high_score_cnt_valid",
            "is_suppressed",
        ]
        nrc_df = self._agg_numeric(group_cols, numeric_cols, df_nrc)
        nrc_df = self._add_rate_columns(nrc_df)
        self._write_dataset("nrc_summary", nrc_df)

    def export_county_summary(self) -> None:
        df_county = self.df[(self.df["aggregation_index"] == 2) & (self.df["SUBGROUP_NAME"] == "All Students")]
        group_cols = ["APIB_IND", "COUNTY_CODE", "COUNTY_NAME"]
        numeric_cols = [
            "tested_student_cnt",
            "tested_cnt_valid",
            "proficiency_cnt_valid",
            "high_score_cnt_valid",
            "is_suppressed",
        ]
        county_df = self._agg_numeric(group_cols, numeric_cols, df_county)
        county_df = self._add_rate_columns(county_df)
        county_df["record_count"] = self.df[(self.df["aggregation_index"] == 2) & (self.df["SUBGROUP_NAME"] == "All Students")].groupby(
            ["APIB_IND", "COUNTY_CODE"]
        )["SUBGROUP_NAME"].count().values
        self._write_dataset("county_summary", county_df)

    def export_subject_summary(self) -> None:
        df_subject = self.df[(self.df["aggregation_index"] == 0) & (self.df["SUBGROUP_NAME"] == "All Students")]
        group_cols = ["APIB_IND", "SUBJECT_AREA"]
        numeric_cols = [
            "tested_student_cnt",
            "tested_cnt_valid",
            "proficiency_cnt_valid",
            "high_score_cnt_valid",
            "is_suppressed",
        ]
        subject_df = self._agg_numeric(group_cols, numeric_cols, df_subject)
        subject_df = self._add_rate_columns(subject_df)
        self._write_dataset("subject_summary", subject_df)

    def export_course_leaders(self, top_n: int = 15) -> None:
        df_course = self.df[(self.df["aggregation_index"] == 0) & (self.df["SUBGROUP_NAME"] == "All Students")]
        group_cols = ["APIB_IND", "ITEM_DESC", "SUBJECT_AREA"]
        numeric_cols = [
            "tested_student_cnt",
            "tested_cnt_valid",
            "proficiency_cnt_valid",
            "high_score_cnt_valid",
        ]
        course_df = self._agg_numeric(group_cols, numeric_cols, df_course)
        course_df = self._add_rate_columns(course_df)
        course_df = course_df.sort_values(["APIB_IND", "tested_student_cnt"], ascending=[True, False])
        top_df = course_df.groupby("APIB_IND").head(top_n).reset_index(drop=True)
        self._write_dataset("top_courses", top_df)

    def export_resource_scatter(self) -> None:
        df_inst = self.df[
            (self.df["aggregation_index"].isin([3, 4])) & (self.df["SUBGROUP_NAME"] == "All Students")
        ].copy()
        group_cols = [
            "APIB_IND",
            "aggregation_index",
            "aggregation_code",
            "aggregation_name",
            "NRC_CODE",
            "NRC_DESC",
            "COUNTY_CODE",
            "COUNTY_NAME",
            "NYC_IND",
        ]
        numeric_cols = [
            "tested_student_cnt",
            "tested_cnt_valid",
            "proficiency_cnt_valid",
            "high_score_cnt_valid",
            "is_suppressed",
        ]
        resource_df = self._agg_numeric(group_cols, numeric_cols, df_inst)
        resource_df = self._add_rate_columns(resource_df)
        resource_df["resource_class"] = resource_df["NRC_CODE"].map(
            {
                1: "纽约市",
                2: "大城市",
                3: "城市-郊区",
                4: "农村",
                5: "平均",
                6: "低N/RC",
                7: "特许学校",
            }
        )
        self._write_dataset("resource_scatter", resource_df)

    def export_suppression_overview(self) -> None:
        group_cols = ["APIB_IND", "aggregation_index", "SUBGROUP_CODE", "SUBGROUP_NAME"]
        numeric_cols = ["is_suppressed"]
        sup_df = self._agg_numeric(group_cols, numeric_cols, self.df)
        sup_df = sup_df.rename(columns={"is_suppressed": "suppressed_records"})
        record_counts = (
            self.df.groupby(group_cols, dropna=False)["SUBGROUP_NAME"]
            .count()
            .rename("record_count")
            .reset_index()
        )
        sup_df = sup_df.merge(record_counts, on=group_cols, how="left")
        sup_df["suppressed_share_records"] = np.where(
            sup_df["record_count"] > 0, sup_df["suppressed_records"] / sup_df["record_count"], np.nan
        )
        self._write_dataset("suppression_overview", sup_df)

    def export_chart_specs(self) -> None:
        specs: Dict[str, Dict] = {}
        out = self.config.output_dir

        def load(name: str) -> pd.DataFrame:
            return pd.read_csv(out / f"{name}.csv")

        state = load("statewide_overview")
        specs["statewide_overview"] = {
            "title": "AP vs IB 参与与成绩总览",
            "description": "展示州级 All Students 在 AP/IB 考试中的参与规模、达标率与高分率，可用于 research1.html 的头部对比柱状图。",
            "data": state.to_dict(orient="records"),
        }

        level = load("state_level_distribution")
        specs["state_level_distribution"] = {
            "title": "AP/IB 等级分布",
            "description": "用于堆叠条形/甜甜圈，体现不同体系各等级人数占比。",
            "data": level.to_dict(orient="records"),
        }

        demo = load("demographic_summary")
        part_focus = [
            "Economically Disadvantaged",
            "Not Economically Disadvantaged",
            "Female",
            "Male",
            "Nonbinary",
            "Black",
            "Hispanic",
            "White",
            "Asian/Pacific Islander",
            "English Language Learner",
            "Not English Language Learner",
        ]
        demo_part = demo[demo["SUBGROUP_NAME"].isin(part_focus)]
        specs["demographic_participation"] = {
            "title": "人口分组参与占比",
            "description": "用于分组条形图，字段 participation_share / proficiency_rate。",
            "data": demo_part.to_dict(orient="records"),
        }
        gap_df = demo[(demo["SUBGROUP_NAME"] != "All Students") & demo["gap_vs_all"].notna()]
        top_pos = gap_df.sort_values("gap_vs_all", ascending=False).groupby("APIB_IND").head(5)
        top_neg = gap_df.sort_values("gap_vs_all", ascending=True).groupby("APIB_IND").head(5)
        specs["demographic_gap"] = {
            "title": "公平性差距 Top/Bottom",
            "description": "象限/瀑布图可引用的前5优势与劣势群体。",
            "positive": top_pos.to_dict(orient="records"),
            "negative": top_neg.to_dict(orient="records"),
        }

        nrc = load("nrc_summary")
        specs["nrc_summary"] = {
            "title": "N/RC 分类表现",
            "description": "供雷达/箱线使用，展示资源类型与表现关系。",
            "data": nrc.to_dict(orient="records"),
        }

        county = load("county_summary")
        def league(df: pd.DataFrame, api: str, metric: str = "proficiency_rate", n: int = 10) -> Dict[str, List[Dict]]:
            subset = df[(df["APIB_IND"] == api) & df[metric].notna()]
            top = subset.sort_values(metric, ascending=False).head(n)
            bottom = subset.sort_values(metric, ascending=True).head(n)
            return {"top": top.to_dict(orient="records"), "bottom": bottom.to_dict(orient="records")}

        specs["county_league"] = {
            "title": "县级达标率 Top/Bottom",
            "description": "表格或条形榜单。",
            "ap": league(county, "AP"),
            "ib": league(county, "IB"),
        }

        subject = load("subject_summary")
        specs["subject_summary"] = {
            "title": "学科热度与表现",
            "description": "热力图/气泡图数据源。",
            "data": subject.to_dict(orient="records"),
        }

        courses = load("top_courses")
        specs["top_courses"] = {
            "title": "Top 15 课程榜",
            "description": "用于 long-tail 条形图或卡片列表。",
            "data": courses.to_dict(orient="records"),
        }

        resource = load("resource_scatter")
        specs["resource_scatter"] = {
            "title": "资源-结果象限",
            "description": "散点图数据，字段 resource_class/NYC_IND 用于编码颜色。",
            "data": resource.to_dict(orient="records"),
        }

        suppression = load("suppression_overview")
        suppression_focus = suppression[suppression["suppressed_records"] > 0]
        specs["suppression_hotspots"] = {
            "title": "抑制热点",
            "description": "提取存在抑制记录的群体/层级，用于风险提示。",
            "data": suppression_focus.to_dict(orient="records"),
        }

        (self.config.output_dir / "chart_specs.json").write_text(
            json.dumps(specs, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    # -----------------------------------------------------
    # Orchestration
    # -----------------------------------------------------
    def run(self) -> None:
        self.load_and_prepare()
        self.export_statewide_overview()
        self.export_level_distribution()
        self.export_demographic_summaries()
        self.export_nrc_summary()
        self.export_county_summary()
        self.export_subject_summary()
        self.export_course_leaders()
        self.export_resource_scatter()
        self.export_suppression_overview()
        self.export_chart_specs()


def main() -> None:
    config = FairnessEDAConfig()
    runner = FairnessEDA(config)
    runner.run()


if __name__ == "__main__":
    main()

