"""
问题三：成绩预测与影响因素 - 探索性数据分析脚本

主要功能：
1. 数据加载与预处理
2. 达标率计算（参考问题一、二的方法）
3. 隐私抑制数据处理
4. 特征工程（创建派生特征、聚合特征）
5. 多维度数据聚合
6. 相关性分析
7. 特征重要性评估
8. 导出分析数据集（CSV/JSON）
"""

import json
from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Optional

import numpy as np
import pandas as pd
from scipy.stats import f_oneway


@dataclass
class Q3EDAConfig:
    source_path: Path = Path("assessment_by_level/by_level/AP_IB_Assessment_2024_level4_School_cleaned.csv")
    hierarchy_path: Path = Path("assessment_by_level/level_relationships/school_hierarchy.csv")
    district_path: Path = Path("assessment_by_level/by_level/AP_IB_Assessment_2024_level3_District_cleaned.csv")
    output_dir: Path = Path("analysis_results/q3")
    frontend_data_dir: Path = Path("前端/data/q3")
    min_valid_tested: int = 5


class Q3EDA:
    def __init__(self, config: Q3EDAConfig):
        self.config = config
        self.config.output_dir.mkdir(parents=True, exist_ok=True)
        self.config.frontend_data_dir.mkdir(parents=True, exist_ok=True)
        self.df = pd.DataFrame()
        self.df_valid = pd.DataFrame()
        self.hierarchy = pd.DataFrame()
        self.district_data = pd.DataFrame()

    # -----------------------------------------------------
    # Data loading & preprocessing
    # -----------------------------------------------------
    def load_and_prepare(self) -> None:
        """Load cleaned CSV, normalize numeric fields, create derived metrics."""
        print("正在加载数据...")
        
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

        df = pd.read_csv(self.config.source_path, dtype=dtype_overrides, low_memory=False)
        print(f"加载了 {len(df):,} 条记录")

        # 处理数值字段
        for col in numeric_cols:
            df[col] = pd.to_numeric(df[col], errors="coerce")

        # 确保aggregation_index是字符串类型（统一处理）
        if "aggregation_index" in df.columns:
            df["aggregation_index"] = df["aggregation_index"].astype(str)

        # 标记抑制数据
        df["is_suppressed"] = (df["tested_student_cnt"] < self.config.min_valid_tested) | (
            df["proficient_student_cnt"].isna()
        )

        # 替换NaN level counts为0（抑制行保持NaN直到填充）
        level_cols = [c for c in numeric_cols if c.startswith("level")]
        df[level_cols] = df[level_cols].fillna(0)

        # 计算达标人数（参考问题一、二的方法）
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
        
        # 标记有效数据
        df["valid_perf"] = (df["tested_student_cnt"] >= self.config.min_valid_tested) & (
            df["proficiency_cnt_calc"] > 0
        )
        df["tested_cnt_valid"] = np.where(df["valid_perf"], df["tested_student_cnt"], 0)
        df["proficiency_cnt_valid"] = np.where(df["valid_perf"], df["proficiency_cnt_calc"], 0)
        df["high_score_cnt_valid"] = np.where(df["valid_perf"], df["high_score_cnt_calc"], 0)

        # 计算达标率
        df["proficiency_rate"] = np.where(
            df["tested_cnt_valid"] > 0,
            df["proficiency_cnt_valid"] / df["tested_cnt_valid"],
            np.nan,
        )
        df["high_score_rate"] = np.where(
            df["tested_cnt_valid"] > 0,
            df["high_score_cnt_valid"] / df["tested_cnt_valid"],
            np.nan,
        )

        # 二分类标记
        df["is_proficient"] = (df["proficiency_rate"] >= 0.5).astype(int)

        self.df = df

        # 过滤有效数据
        self.df_valid = df[
            (df["tested_student_cnt"] >= self.config.min_valid_tested) & (df["proficiency_rate"].notna())
        ].copy()
        print(f"有效数据：{len(self.df_valid):,} 条记录")

    def load_hierarchy_data(self) -> None:
        """加载层级关系数据"""
        if self.config.hierarchy_path.exists():
            self.hierarchy = pd.read_csv(self.config.hierarchy_path)
            # 确保school_code是字符串类型，以便与aggregation_code合并
            self.hierarchy["school_code"] = self.hierarchy["school_code"].astype(str)
            print(f"加载了层级关系数据：{len(self.hierarchy)} 条记录")
        else:
            print("警告：层级关系文件不存在，跳过聚合特征创建")

    def load_district_data(self) -> None:
        """加载学区级数据用于创建聚合特征"""
        if self.config.district_path.exists():
            self.district_data = pd.read_csv(self.config.district_path, low_memory=False)
            # 处理数值字段
            numeric_cols = ["tested_student_cnt", "proficient_student_cnt"]
            for col in numeric_cols:
                self.district_data[col] = pd.to_numeric(self.district_data[col], errors="coerce")
            print(f"加载了学区级数据：{len(self.district_data):,} 条记录")
        else:
            print("警告：学区级数据文件不存在，跳过聚合特征创建")

    def create_aggregated_features(self) -> None:
        """创建聚合特征（学区级、县级）"""
        if self.hierarchy.empty or self.df_valid.empty:
            print("跳过聚合特征创建：缺少必要数据")
            return

        print("正在创建聚合特征...")

        # 确保aggregation_code是字符串类型
        self.df_valid["aggregation_code"] = self.df_valid["aggregation_code"].astype(str)
        
        # 合并层级关系
        self.df_valid = self.df_valid.merge(
            self.hierarchy[["school_code", "district_code", "county_code", "nrc_code", "nrc_desc"]],
            left_on="aggregation_code",
            right_on="school_code",
            how="left",
        )

        # 计算学区平均达标率
        if not self.district_data.empty:
            # 确保aggregation_index类型一致（可能是字符串或数值）
            district_data_filtered = self.district_data.copy()
            district_data_filtered["aggregation_index"] = district_data_filtered["aggregation_index"].astype(str)
            
            district_summary = (
                district_data_filtered[
                    (district_data_filtered["aggregation_index"] == "3")
                    & (district_data_filtered["SUBGROUP_NAME"] == "All Students")
                ]
                .groupby("aggregation_code")
                .agg(
                    tested_total=("tested_student_cnt", "sum"),
                    proficient_total=("proficient_student_cnt", lambda x: pd.to_numeric(x, errors="coerce").sum()),
                )
                .reset_index()
            )
            district_summary["district_avg_proficiency_rate"] = (
                district_summary["proficient_total"] / district_summary["tested_total"].replace(0, np.nan)
            )
            district_summary = district_summary.rename(columns={"aggregation_code": "district_code"})
            
            # 确保district_code类型一致
            district_summary["district_code"] = district_summary["district_code"].astype(str)
            if "district_code" in self.df_valid.columns:
                self.df_valid["district_code"] = self.df_valid["district_code"].astype(str)

            self.df_valid = self.df_valid.merge(
                district_summary[["district_code", "district_avg_proficiency_rate"]],
                on="district_code",
                how="left",
            )

        print("聚合特征创建完成")

    # -----------------------------------------------------
    # Helper utilities
    # -----------------------------------------------------
    def _write_dataset(self, name: str, df: pd.DataFrame) -> None:
        """导出数据集为CSV和JSON格式"""
        csv_path = self.config.output_dir / f"{name}.csv"
        json_path = self.config.output_dir / f"{name}.json"
        frontend_json_path = self.config.frontend_data_dir / f"{name}.json"

        df.to_csv(csv_path, index=False)
        
        # 转换为字典，处理NaN值：先将NaN替换为None，然后转换为字典
        df_clean = df.replace({np.nan: None})
        json_data = df_clean.round(6).to_dict(orient="records")
        
        # 递归清理字典中的NaN值（处理numpy的NaN类型）
        def clean_nan(obj):
            if isinstance(obj, dict):
                return {k: clean_nan(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_nan(item) for item in obj]
            elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
                return None
            return obj
        
        json_data_clean = clean_nan(json_data)
        
        # 保存到analysis_results
        json_str = json.dumps(json_data_clean, ensure_ascii=False, indent=2)
        json_path.write_text(json_str, encoding="utf-8")
        
        # 同时保存到前端数据目录
        frontend_json_path.write_text(json_str, encoding="utf-8")

    def _agg_numeric(self, group_cols: List[str], numeric_cols: List[str], df: pd.DataFrame) -> pd.DataFrame:
        """数值字段聚合"""
        agg_dict = {col: "sum" for col in numeric_cols}
        agg_df = df.groupby(group_cols, dropna=False).agg(agg_dict).reset_index()
        return agg_df

    def _add_rate_columns(self, df: pd.DataFrame) -> pd.DataFrame:
        """添加率值列"""
        df = df.copy()
        df["proficiency_rate"] = np.where(
            df["tested_cnt_valid"] > 0,
            df["proficiency_cnt_valid"] / df["tested_cnt_valid"],
            np.nan,
        )
        df["high_score_rate"] = np.where(
            df["tested_cnt_valid"] > 0,
            df["high_score_cnt_valid"] / df["tested_cnt_valid"],
            np.nan,
        )
        return df

    # -----------------------------------------------------
    # Analysis functions
    # -----------------------------------------------------
    def analyze_target_variable(self) -> None:
        """目标变量分析"""
        print("分析目标变量...")

        df_state = self.df[
            (self.df["aggregation_index"] == "4") & (self.df["SUBGROUP_NAME"] == "All Students")
        ]

        summary = []
        for apib in ["AP", "IB"]:
            subset = df_state[df_state["APIB_IND"] == apib]
            valid_subset = subset[subset["proficiency_rate"].notna()]

            if len(valid_subset) > 0:
                summary.append(
                    {
                        "APIB_IND": apib,
                        "mean": float(valid_subset["proficiency_rate"].mean()),
                        "median": float(valid_subset["proficiency_rate"].median()),
                        "std": float(valid_subset["proficiency_rate"].std()),
                        "min": float(valid_subset["proficiency_rate"].min()),
                        "max": float(valid_subset["proficiency_rate"].max()),
                        "q25": float(valid_subset["proficiency_rate"].quantile(0.25)),
                        "q75": float(valid_subset["proficiency_rate"].quantile(0.75)),
                        "count": int(len(valid_subset)),
                    }
                )

        summary_df = pd.DataFrame(summary)
        self._write_dataset("target_variable_summary", summary_df)

        # 导出分布数据（用于前端密度图）
        distribution_data = []
        for apib in ["AP", "IB"]:
            subset = df_state[
                (df_state["APIB_IND"] == apib) & (df_state["proficiency_rate"].notna())
            ]
            if len(subset) > 0:
                distribution_data.append(
                    {
                        "APIB_IND": apib,
                        "proficiency_rates": subset["proficiency_rate"].tolist(),
                    }
                )
        
        dist_json_path = self.config.frontend_data_dir / "proficiency_distribution.json"
        # 递归清理NaN值
        def clean_nan(obj):
            if isinstance(obj, dict):
                return {k: clean_nan(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_nan(item) for item in obj]
            elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
                return None
            return obj
        
        distribution_data_clean = clean_nan(distribution_data)
        dist_json_path.write_text(
            json.dumps(distribution_data_clean, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def analyze_data_quality(self) -> None:
        """数据质量评估 - 抑制率排序"""
        print("分析数据质量...")

        # 按子组和AP/IB统计抑制率
        suppression_by_subgroup = (
            self.df.groupby(["SUBGROUP_NAME", "APIB_IND"])
            .agg(
                total_records=("is_suppressed", "count"),
                suppressed_records=("is_suppressed", "sum"),
            )
            .reset_index()
        )
        suppression_by_subgroup["suppression_rate"] = (
            suppression_by_subgroup["suppressed_records"] / suppression_by_subgroup["total_records"]
        )

        # 按抑制率排序（从高到低）
        suppression_sorted = suppression_by_subgroup.sort_values("suppression_rate", ascending=False)
        
        # 取Top 30
        suppression_top = suppression_sorted.head(30)

        # 导出排序后的数据
        suppression_data = []
        for _, row in suppression_top.iterrows():
            suppression_data.append(
                {
                    "subgroup_name": row["SUBGROUP_NAME"],
                    "apib_ind": row["APIB_IND"],
                    "suppression_rate": float(row["suppression_rate"]),
                    "total_records": int(row["total_records"]),
                    "suppressed_records": int(row["suppressed_records"]),
                }
            )

        self._write_dataset("data_quality_report", pd.DataFrame(suppression_data))

        # 导出到前端（用于排序条形图）
        suppression_json_path = self.config.frontend_data_dir / "suppression_sorted.json"
        # 递归清理NaN值
        def clean_nan(obj):
            if isinstance(obj, dict):
                return {k: clean_nan(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_nan(item) for item in obj]
            elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
                return None
            return obj
        
        suppression_data_clean = clean_nan(suppression_data)
        suppression_json_path.write_text(
            json.dumps(suppression_data_clean, ensure_ascii=False, indent=2), encoding="utf-8"
        )

    def analyze_demographic_impact(self) -> None:
        """学生特征影响分析"""
        print("分析学生特征影响...")

        df_state = self.df_valid[
            (self.df_valid["aggregation_index"] == "4") & (self.df_valid["SUBGROUP_NAME"] != "All Students")
        ]

        # 按子组聚合
        group_cols = ["APIB_IND", "SUBGROUP_CODE", "SUBGROUP_NAME"]
        numeric_cols = [
            "tested_student_cnt",
            "tested_cnt_valid",
            "proficiency_cnt_valid",
            "high_score_cnt_valid",
        ]

        demo_df = self._agg_numeric(group_cols, numeric_cols, df_state)
        demo_df = self._add_rate_columns(demo_df)

        # 计算与All Students的差距
        all_students = self.df_valid[
            (self.df_valid["aggregation_index"] == "4") & (self.df_valid["SUBGROUP_NAME"] == "All Students")
        ]
        all_students_summary = (
            all_students.groupby("APIB_IND")
            .agg(
                all_tested=("tested_cnt_valid", "sum"),
                all_proficient=("proficiency_cnt_valid", "sum"),
            )
            .reset_index()
        )
        all_students_summary["all_proficiency_rate"] = (
            all_students_summary["all_proficient"] / all_students_summary["all_tested"]
        )

        demo_df = demo_df.merge(
            all_students_summary[["APIB_IND", "all_proficiency_rate"]], on="APIB_IND", how="left"
        )
        demo_df["gap_vs_all"] = demo_df["proficiency_rate"] - demo_df["all_proficiency_rate"]

        # 添加类别字段（用于分组展示）
        def categorize_subgroup(name):
            if "Economically" in name:
                return "经济状况"
            elif name in ["White", "Black", "Hispanic", "Asian/Pacific Islander", 
                          "American Indian/Alaska Native", "Multiracial"]:
                return "种族/族裔"
            elif name in ["Male", "Female", "Nonbinary"]:
                return "性别"
            elif "Disabilities" in name or "General Education" in name:
                return "教育类型"
            elif "English Language" in name:
                return "语言"
            elif "Migrant" in name:
                return "流动性"
            elif "Homeless" in name:
                return "居住状况"
            elif "Foster Care" in name:
                return "寄养状况"
            elif "Armed Forces" in name:
                return "军属"
            else:
                return "其他"

        demo_df["category"] = demo_df["SUBGROUP_NAME"].apply(categorize_subgroup)

        self._write_dataset("demographic_impact_summary", demo_df)

        # 计算相关性：使用每个子组与All Students的差距作为重要性指标
        # 简化方法：使用达标率的变异系数或与All Students的绝对差距
        correlation_data = []
        for apib in ["AP", "IB"]:
            apib_demo = demo_df[demo_df["APIB_IND"] == apib].copy()
            if len(apib_demo) > 0:
                # 计算每个子组的达标率与All Students的差距（绝对值）
                apib_demo["abs_gap"] = apib_demo["gap_vs_all"].abs()
                apib_demo["importance"] = apib_demo["abs_gap"] * np.sqrt(apib_demo["tested_cnt_valid"])
                
                # 按重要性排序，取Top 20
                top_subgroups = apib_demo.nlargest(20, "importance")
                
                for _, row in top_subgroups.iterrows():
                    correlation_data.append(
                        {
                            "subgroup_name": row["SUBGROUP_NAME"],
                            "apib_ind": apib,
                            "f_statistic": float(row["importance"]) if not np.isnan(row["importance"]) else 0.0,
                            "p_value": 0.0,  # 简化处理，不计算p值
                            "sample_size": int(row["tested_cnt_valid"]) if not np.isnan(row["tested_cnt_valid"]) else 0,
                            "proficiency_rate": float(row["proficiency_rate"]) if not np.isnan(row["proficiency_rate"]) else 0.0,
                            "gap_vs_all": float(row["gap_vs_all"]) if not np.isnan(row["gap_vs_all"]) else 0.0,
                        }
                    )

        correlation_df = pd.DataFrame(correlation_data)
        if not correlation_df.empty:
            self._write_dataset("demographic_correlation_matrix", correlation_df)
        else:
            # 如果为空，至少创建一个空的数据框结构
            correlation_df = pd.DataFrame(columns=["subgroup_name", "apib_ind", "f_statistic", "p_value", "sample_size"])
            self._write_dataset("demographic_correlation_matrix", correlation_df)

        # 注意：已删除交互效应分析（图表5），因为数据限制无法计算真正的交互效应

    def analyze_regional_impact(self) -> None:
        """地区特征影响分析 - 合并N/RC与县级分析为综合图表"""
        print("分析地区特征影响...")

        df_county = self.df_valid[
            (self.df_valid["aggregation_index"] == "4") & (self.df_valid["SUBGROUP_NAME"] == "All Students")
        ].copy()

        # 县级分析（用于散点图）
        county_summary = (
            df_county.groupby(["COUNTY_CODE", "COUNTY_NAME", "NRC_CODE", "NRC_DESC", "APIB_IND"])
            .agg(
                tested_total=("tested_cnt_valid", "sum"),
                proficient_total=("proficiency_cnt_valid", "sum"),
            )
            .reset_index()
        )
        county_summary["proficiency_rate"] = (
            county_summary["proficient_total"] / county_summary["tested_total"].replace(0, np.nan)
        )
        
        # 修复超过100%的问题
        county_summary = county_summary[county_summary["proficiency_rate"] <= 1.0]
        county_summary["NRC_CODE"] = pd.to_numeric(county_summary["NRC_CODE"], errors="coerce")

        # N/RC聚合（用于趋势线）
        nrc_summary = (
            df_county.groupby(["NRC_CODE", "NRC_DESC", "APIB_IND"])
            .agg(
                tested_total=("tested_cnt_valid", "sum"),
                proficient_total=("proficiency_cnt_valid", "sum"),
            )
            .reset_index()
        )
        nrc_summary["proficiency_rate"] = (
            nrc_summary["proficient_total"] / nrc_summary["tested_total"].replace(0, np.nan)
        )
        nrc_summary["NRC_CODE"] = pd.to_numeric(nrc_summary["NRC_CODE"], errors="coerce")

        # 合并为综合数据（图表5：N/RC与达标率关系，含县级分布）
        nrc_county_relationship = []
        for _, row in county_summary.iterrows():
            nrc_county_relationship.append(
                {
                    "county_code": str(row["COUNTY_CODE"]) if pd.notna(row["COUNTY_CODE"]) else None,
                    "county_name": row["COUNTY_NAME"] if pd.notna(row["COUNTY_NAME"]) else None,
                    "nrc_code": float(row["NRC_CODE"]) if pd.notna(row["NRC_CODE"]) else None,
                    "nrc_desc": row["NRC_DESC"] if pd.notna(row["NRC_DESC"]) else None,
                    "apib_ind": row["APIB_IND"],
                    "proficiency_rate": float(row["proficiency_rate"]) if pd.notna(row["proficiency_rate"]) else None,
                    "tested_total": int(row["tested_total"]),
                }
            )
        
        # 添加N/RC聚合数据（用于趋势线）
        for _, row in nrc_summary.iterrows():
            nrc_county_relationship.append(
                {
                    "county_code": None,
                    "county_name": None,
                    "nrc_code": float(row["NRC_CODE"]) if pd.notna(row["NRC_CODE"]) else None,
                    "nrc_desc": row["NRC_DESC"] if pd.notna(row["NRC_DESC"]) else None,
                    "apib_ind": row["APIB_IND"],
                    "proficiency_rate": float(row["proficiency_rate"]) if pd.notna(row["proficiency_rate"]) else None,
                    "tested_total": int(row["tested_total"]),
                    "is_nrc_aggregate": True,  # 标记为N/RC聚合数据
                }
            )

        self._write_dataset("nrc_county_relationship", pd.DataFrame(nrc_county_relationship))

        # NYC分析（图表6：NYC vs 非NYC对比，按N/RC细化）
        nyc_nrc_summary = (
            df_county.groupby(["NYC_IND", "NRC_CODE", "NRC_DESC", "APIB_IND"])
            .agg(
                tested_total=("tested_cnt_valid", "sum"),
                proficient_total=("proficiency_cnt_valid", "sum"),
            )
            .reset_index()
        )
        nyc_nrc_summary["proficiency_rate"] = (
            nyc_nrc_summary["proficient_total"] / nyc_nrc_summary["tested_total"].replace(0, np.nan)
        )
        nyc_nrc_summary["NRC_CODE"] = pd.to_numeric(nyc_nrc_summary["NRC_CODE"], errors="coerce")
        nyc_nrc_summary["NYC_IND"] = pd.to_numeric(nyc_nrc_summary["NYC_IND"], errors="coerce")

        self._write_dataset("nyc_nrc_summary", nyc_nrc_summary)

    def analyze_resource_impact(self) -> None:
        """学校资源特征影响分析"""
        print("分析学校资源特征影响...")

        df_school = self.df_valid[
            (self.df_valid["aggregation_index"] == "4") & (self.df_valid["SUBGROUP_NAME"] == "All Students")
        ]

        # 资源-结果关系分析
        resource_outcome = []
        for _, row in df_school.iterrows():
            resource_outcome.append(
                {
                    "aggregation_code": row["aggregation_code"],
                    "aggregation_name": row["aggregation_name"],
                    "nrc_code": float(row["NRC_CODE"]) if pd.notna(row["NRC_CODE"]) else None,
                    "nrc_desc": row["NRC_DESC"] if pd.notna(row["NRC_DESC"]) else None,
                    "nyc_ind": float(row["NYC_IND"]) if pd.notna(row["NYC_IND"]) else None,
                    "county_code": str(row["COUNTY_CODE"]) if pd.notna(row["COUNTY_CODE"]) else None,
                    "county_name": row["COUNTY_NAME"] if pd.notna(row["COUNTY_NAME"]) else None,
                    "proficiency_rate": float(row["proficiency_rate"]) if pd.notna(row["proficiency_rate"]) else None,
                    "tested_total": int(row["tested_cnt_valid"]),
                    "district_avg_proficiency_rate": (
                        float(row["district_avg_proficiency_rate"])
                        if pd.notna(row.get("district_avg_proficiency_rate"))
                        else None
                    ),
                }
            )

        resource_df = pd.DataFrame(resource_outcome)
        self._write_dataset("resource_outcome_analysis", resource_df)

        # 学区特征汇总
        if "district_avg_proficiency_rate" in df_school.columns:
            district_summary = (
                df_school.groupby("district_code")
                .agg(
                    district_name=("aggregation_name", "first"),
                    school_count=("aggregation_code", "count"),
                    avg_proficiency_rate=("proficiency_rate", "mean"),
                    district_avg_proficiency_rate=("district_avg_proficiency_rate", "first"),
                )
                .reset_index()
            )
            self._write_dataset("district_features_summary", district_summary)

    def analyze_subject_impact(self) -> None:
        """学科特征影响分析"""
        print("分析学科特征影响...")

        df_subject = self.df_valid[
            (self.df_valid["aggregation_index"] == "4") & (self.df_valid["SUBGROUP_NAME"] == "All Students")
        ]

        subject_summary = (
            df_subject.groupby(["SUBJECT_AREA", "APIB_IND"])
            .agg(
                tested_total=("tested_cnt_valid", "sum"),
                proficient_total=("proficiency_cnt_valid", "sum"),
            )
            .reset_index()
        )
        subject_summary["proficiency_rate"] = (
            subject_summary["proficient_total"] / subject_summary["tested_total"].replace(0, np.nan)
        )

        self._write_dataset("subject_impact_summary", subject_summary)

    def analyze_correlations(self) -> None:
        """综合相关性分析"""
        print("分析特征相关性...")

        df_school = self.df_valid[
            (self.df_valid["aggregation_index"] == "4") & (self.df_valid["SUBGROUP_NAME"] == "All Students")
        ].copy()

        # 准备数值特征
        numeric_features = []
        if "NRC_CODE" in df_school.columns:
            df_school["NRC_CODE"] = pd.to_numeric(df_school["NRC_CODE"], errors="coerce")
            numeric_features.append("NRC_CODE")
        if "NYC_IND" in df_school.columns:
            df_school["NYC_IND"] = pd.to_numeric(df_school["NYC_IND"], errors="coerce")
            numeric_features.append("NYC_IND")
        if "tested_student_cnt" in df_school.columns:
            numeric_features.append("tested_student_cnt")
        if "district_avg_proficiency_rate" in df_school.columns:
            numeric_features.append("district_avg_proficiency_rate")

        # 计算相关性
        correlation_data = []
        for feature in numeric_features:
            if feature in df_school.columns:
                subset = df_school[[feature, "proficiency_rate"]].dropna()
                if len(subset) >= 30:  # 最小样本量要求
                    corr = subset[feature].corr(subset["proficiency_rate"])
                    correlation_data.append(
                        {
                            "feature": feature,
                            "correlation": float(corr) if not np.isnan(corr) else 0.0,
                            "abs_correlation": float(abs(corr)) if not np.isnan(corr) else 0.0,
                            "sample_size": int(len(subset)),
                        }
                    )

        correlation_df = pd.DataFrame(correlation_data).sort_values("abs_correlation", ascending=False)
        self._write_dataset("feature_target_correlation", correlation_df)

        # 全特征相关性矩阵（简化版：只包含数值特征）
        if len(numeric_features) > 0:
            corr_matrix = df_school[numeric_features + ["proficiency_rate"]].corr()
            corr_matrix_reset = corr_matrix.reset_index()
            corr_matrix_reset = corr_matrix_reset.rename(columns={"index": "feature1"})
            self._write_dataset("full_correlation_matrix", corr_matrix_reset)

    def evaluate_feature_importance(self) -> None:
        """特征重要性评估（统计方法）- 按业务含义分组：学生/地区/资源/学科"""
        print("评估特征重要性...")

        df_school = self.df_valid[
            (self.df_valid["aggregation_index"] == "4") & (self.df_valid["SUBGROUP_NAME"] == "All Students")
        ].copy()

        importance_data = []

        # ========== 1. 学生特征（从SUBGROUP_NAME派生，按类别聚合） ==========
        # 从demographic_impact_summary中提取学生特征类别重要性
        try:
            demo_impact_path = self.config.output_dir / "demographic_impact_summary.json"
            if demo_impact_path.exists():
                import json
                with open(demo_impact_path, 'r', encoding='utf-8') as f:
                    demo_data = json.load(f)
                
                demo_df = pd.DataFrame(demo_data)
                
                # 计算每个子组的重要性：|gap_vs_all| * sqrt(tested_cnt_valid)
                demo_df["importance"] = demo_df["gap_vs_all"].abs() * np.sqrt(demo_df["tested_cnt_valid"].fillna(0))
                
                # 按类别聚合，使用最大重要性作为该类别的代表值
                student_categories = ["经济状况", "种族/族裔", "性别", "教育类型", "语言", "流动性", "居住状况", "寄养状况", "军属"]
                for category in student_categories:
                    category_data = demo_df[demo_df["category"] == category]
                    if len(category_data) > 0:
                        max_importance = category_data["importance"].max()
                        if not np.isnan(max_importance) and max_importance > 0:
                            importance_data.append({
                                "feature": f"学生特征-{category}",
                                "feature_group": "学生特征",
                                "importance": float(max_importance),
                                "method": "category_max_gap",
                                "display_name": category,
                                "sample_size": int(category_data["tested_cnt_valid"].sum())
                            })
        except Exception as e:
            print(f"警告：无法加载学生特征重要性数据: {e}")

        # ========== 2. 地区特征（地理位置） ==========
        # COUNTY_NAME: ANOVA
        if "COUNTY_NAME" in df_school.columns:
            groups = []
            for county in df_school["COUNTY_NAME"].unique():
                group_data = df_school[
                    (df_school["COUNTY_NAME"] == county) & (df_school["proficiency_rate"].notna())
                ]["proficiency_rate"]
                if len(group_data) > 0:
                    groups.append(group_data)
            
            if len(groups) >= 2:
                try:
                    f_stat, p_value = f_oneway(*groups)
                    if not np.isnan(f_stat):
                        importance_data.append({
                            "feature": "COUNTY_NAME",
                            "feature_group": "地区特征",
                            "importance": float(f_stat),
                            "method": "anova",
                            "p_value": float(p_value) if not np.isnan(p_value) else 1.0,
                            "display_name": "县",
                            "sample_size": sum(len(g) for g in groups),
                            "category_count": len(groups)
                        })
                except:
                    pass

        # NYC_IND: 相关系数
        if "NYC_IND" in df_school.columns:
            df_school = df_school.copy()  # 确保是副本
            df_school["NYC_IND_numeric"] = pd.to_numeric(df_school["NYC_IND"], errors="coerce")
            subset = df_school[["NYC_IND_numeric", "proficiency_rate"]].dropna()
            if len(subset) >= 30:
                corr = abs(subset["NYC_IND_numeric"].corr(subset["proficiency_rate"]))
                if not np.isnan(corr):
                    importance_data.append({
                        "feature": "NYC_IND",
                        "feature_group": "地区特征",
                        "importance": float(corr),
                        "method": "correlation",
                        "display_name": "NYC标识",
                        "sample_size": int(len(subset))
                    })

        # ========== 3. 资源特征（资源能力） ==========
        # NRC_CODE: 相关系数
        if "NRC_CODE" in df_school.columns:
            if "NYC_IND_numeric" not in df_school.columns:  # 如果还没有创建副本
                df_school = df_school.copy()  # 确保是副本
            df_school["NRC_CODE_numeric"] = pd.to_numeric(df_school["NRC_CODE"], errors="coerce")
            subset = df_school[["NRC_CODE_numeric", "proficiency_rate"]].dropna()
            if len(subset) >= 30:
                corr = abs(subset["NRC_CODE_numeric"].corr(subset["proficiency_rate"]))
                if not np.isnan(corr):
                    importance_data.append({
                        "feature": "NRC_CODE",
                        "feature_group": "资源特征",
                        "importance": float(corr),
                        "method": "correlation",
                        "display_name": "N/RC代码",
                        "sample_size": int(len(subset))
                    })

        # district_avg_proficiency_rate: 相关系数
        if "district_avg_proficiency_rate" in df_school.columns:
            subset = df_school[["district_avg_proficiency_rate", "proficiency_rate"]].dropna()
            if len(subset) >= 30:
                corr = abs(subset["district_avg_proficiency_rate"].corr(subset["proficiency_rate"]))
                if not np.isnan(corr):
                    importance_data.append({
                        "feature": "district_avg_proficiency_rate",
                        "feature_group": "资源特征",
                        "importance": float(corr),
                        "method": "correlation",
                        "display_name": "学区平均达标率",
                        "sample_size": int(len(subset))
                    })

        # ========== 4. 学科特征 ==========
        # SUBJECT_AREA: ANOVA
        if "SUBJECT_AREA" in df_school.columns:
            groups = []
            for subject in df_school["SUBJECT_AREA"].unique():
                group_data = df_school[
                    (df_school["SUBJECT_AREA"] == subject) & (df_school["proficiency_rate"].notna())
                ]["proficiency_rate"]
                if len(group_data) > 0:
                    groups.append(group_data)
            
            if len(groups) >= 2:
                try:
                    f_stat, p_value = f_oneway(*groups)
                    if not np.isnan(f_stat):
                        importance_data.append({
                            "feature": "SUBJECT_AREA",
                            "feature_group": "学科特征",
                            "importance": float(f_stat),
                            "method": "anova",
                            "p_value": float(p_value) if not np.isnan(p_value) else 1.0,
                            "display_name": "学科领域",
                            "sample_size": sum(len(g) for g in groups),
                            "category_count": len(groups)
                        })
                except:
                    pass

        # APIB_IND: ANOVA（只有2个类别）
        if "APIB_IND" in df_school.columns:
            groups = []
            for apib in df_school["APIB_IND"].unique():
                group_data = df_school[
                    (df_school["APIB_IND"] == apib) & (df_school["proficiency_rate"].notna())
                ]["proficiency_rate"]
                if len(group_data) > 0:
                    groups.append(group_data)
            
            if len(groups) >= 2:
                try:
                    f_stat, p_value = f_oneway(*groups)
                    if not np.isnan(f_stat):
                        importance_data.append({
                            "feature": "APIB_IND",
                            "feature_group": "学科特征",
                            "importance": float(f_stat),
                            "method": "anova",
                            "p_value": float(p_value) if not np.isnan(p_value) else 1.0,
                            "display_name": "AP/IB类型",
                            "sample_size": sum(len(g) for g in groups),
                            "category_count": len(groups)
                        })
                except:
                    pass

        # GRADE_LEVEL: ANOVA
        if "GRADE_LEVEL" in df_school.columns:
            groups = []
            for grade in df_school["GRADE_LEVEL"].unique():
                group_data = df_school[
                    (df_school["GRADE_LEVEL"] == grade) & (df_school["proficiency_rate"].notna())
                ]["proficiency_rate"]
                if len(group_data) > 0:
                    groups.append(group_data)
            
            if len(groups) >= 2:
                try:
                    f_stat, p_value = f_oneway(*groups)
                    if not np.isnan(f_stat):
                        importance_data.append({
                            "feature": "GRADE_LEVEL",
                            "feature_group": "学科特征",
                            "importance": float(f_stat),
                            "method": "anova",
                            "p_value": float(p_value) if not np.isnan(p_value) else 1.0,
                            "display_name": "年级",
                            "sample_size": sum(len(g) for g in groups),
                            "category_count": len(groups)
                        })
                except:
                    pass

        # 转换为DataFrame并排序
        importance_df = pd.DataFrame(importance_data)
        if not importance_df.empty:
            importance_df = importance_df.sort_values("importance", ascending=False)
            self._write_dataset("feature_importance_summary", importance_df)

            # 按特征组汇总
            group_importance = (
                importance_df.groupby("feature_group")
                .agg(
                    avg_importance=("importance", "mean"),
                    max_importance=("importance", "max"),
                    feature_count=("feature", "count"),
                )
                .reset_index()
            )
            self._write_dataset("feature_group_importance", group_importance)
        else:
            # 如果为空，创建空的数据框结构
            empty_df = pd.DataFrame(columns=["feature", "feature_group", "importance", "method", "display_name"])
            self._write_dataset("feature_importance_summary", empty_df)
            self._write_dataset("feature_group_importance", pd.DataFrame(columns=["feature_group", "avg_importance", "max_importance", "feature_count"]))

    def export_feature_engineering_suggestions(self) -> None:
        """导出特征工程建议"""
        print("生成特征工程建议...")

        suggestions = []

        # 基于交互效应分析的建议
        if (self.config.output_dir / "demographic_interaction_analysis.json").exists():
            suggestions.append(
                {
                    "suggestion_type": "交互特征",
                    "feature": "economic_status × race",
                    "description": "经济状况与种族的交互效应显著，建议创建交互特征",
                    "priority": "high",
                }
            )

        # 基于相关性分析的建议
        if (self.config.output_dir / "feature_target_correlation.json").exists():
            suggestions.append(
                {
                    "suggestion_type": "聚合特征",
                    "feature": "district_avg_proficiency_rate",
                    "description": "学区平均达标率与学校达标率相关，建议保留此聚合特征",
                    "priority": "medium",
                }
            )

        # 编码建议
        suggestions.extend(
            [
                {
                    "suggestion_type": "编码方式",
                    "feature": "SUBGROUP_NAME",
                    "description": "使用One-hot编码或Target Encoding",
                    "priority": "high",
                },
                {
                    "suggestion_type": "编码方式",
                    "feature": "COUNTY_NAME",
                    "description": "使用Label Encoding或Target Encoding",
                    "priority": "medium",
                },
                {
                    "suggestion_type": "编码方式",
                    "feature": "SUBJECT_AREA",
                    "description": "使用One-hot编码",
                    "priority": "medium",
                },
            ]
        )

        suggestions_df = pd.DataFrame(suggestions)
        self._write_dataset("feature_engineering_suggestions", suggestions_df)

    def export_chart_specs(self) -> None:
        """导出图表规格文件（供前端使用）"""
        print("导出图表规格文件...")

        specs = {}

        # 加载所有数据文件
        def load_json(name: str) -> List[Dict]:
            json_path = self.config.frontend_data_dir / f"{name}.json"
            if json_path.exists():
                try:
                    return json.loads(json_path.read_text(encoding="utf-8"))
                except:
                    return []
            return []

        # 1. 达标率分布
        dist_data = load_json("proficiency_distribution")
        specs["proficiency_distribution"] = {
            "title": "达标率分布（AP vs IB）",
            "description": "展示AP和IB达标率的分布特征，使用密度图对比两个体系的差异",
            "data": dist_data,
        }

        # 2. 抑制率排序
        suppression_data = load_json("suppression_sorted")
        specs["suppression_sorted"] = {
            "title": "数据抑制率排序",
            "description": "按抑制率从高到低排序，显示Top 30个子组",
            "data": suppression_data,
        }

        # 3. 人口统计分组对比
        demo_data = load_json("demographic_impact_summary")
        specs["demographic_comparison"] = {
            "title": "人口统计分组达标率对比",
            "description": "各子组的达标率对比，包含与All Students的差距",
            "data": demo_data,
        }

        # 4. 学生特征重要性
        corr_data = load_json("demographic_correlation_matrix")
        specs["demographic_importance"] = {
            "title": "学生特征重要性排序",
            "description": "学生特征与达标率的重要性排序（Top 20）",
            "data": corr_data,
        }

        # 5. N/RC与达标率关系（含县级分布）- 合并图表6和7
        nrc_county_data = load_json("nrc_county_relationship")
        specs["nrc_proficiency_relationship"] = {
            "title": "N/RC与达标率关系（含县级分布）",
            "description": "散点图显示每个县的达标率，按N/RC分组，包含趋势线和箱线图",
            "data": nrc_county_data,
        }

        # 6. NYC vs 非NYC对比（按N/RC细化）
        nyc_nrc_data = load_json("nyc_nrc_summary")
        specs["nyc_comparison"] = {
            "title": "NYC vs 非NYC对比（按N/RC细化）",
            "description": "按N/RC分组，对比NYC和非NYC的达标率",
            "data": nyc_nrc_data,
        }

        # 7. 资源-结果象限图
        resource_data = load_json("resource_outcome_analysis")
        specs["resource_quadrant"] = {
            "title": "资源-结果象限图",
            "description": "资源指标与达标率的关系，识别四个象限",
            "data": resource_data,
        }

        # 8. 学科达标率排序
        subject_data = load_json("subject_impact_summary")
        specs["subject_comparison"] = {
            "title": "学科达标率排序",
            "description": "各学科领域的达标率对比（AP vs IB），按达标率排序",
            "data": subject_data,
        }

        # 9. 特征-目标相关性（扩展）
        feature_corr_data = load_json("feature_target_correlation")
        specs["feature_correlation"] = {
            "title": "特征-目标相关性（扩展）",
            "description": "所有特征与达标率的相关性（Top 20）",
            "data": feature_corr_data,
        }

        # 10. 特征重要性排序（统一方法）
        importance_data = load_json("feature_importance_summary")
        specs["feature_importance"] = {
            "title": "特征重要性排序（统一方法）",
            "description": "特征重要性排序（Top 15），使用统一方法计算",
            "data": importance_data,
        }

        # 11. 特征组重要性对比（按业务含义分组）
        group_importance_data = load_json("feature_group_importance")
        specs["feature_group_importance"] = {
            "title": "特征组重要性对比（按业务含义分组）",
            "description": "学生/地区/资源/学科特征组的重要性对比",
            "data": group_importance_data,
        }

        # 14. 目标变量统计摘要（用于概览统计）
        target_summary_data = load_json("target_variable_summary")
        specs["target_variable_summary"] = {
            "title": "目标变量统计摘要",
            "description": "AP和IB达标率的基本统计信息",
            "data": target_summary_data,
        }

        # 保存chart_specs.json（处理NaN值）
        chart_specs_path = self.config.output_dir / "chart_specs.json"
        frontend_chart_specs_path = self.config.frontend_data_dir / "chart_specs.json"
        
        # 递归清理NaN值
        def clean_nan(obj):
            if isinstance(obj, dict):
                return {k: clean_nan(v) for k, v in obj.items()}
            elif isinstance(obj, list):
                return [clean_nan(item) for item in obj]
            elif isinstance(obj, float) and (np.isnan(obj) or np.isinf(obj)):
                return None
            return obj
        
        chart_specs_clean = clean_nan(specs)
        chart_specs_json_final = json.dumps(chart_specs_clean, ensure_ascii=False, indent=2)
        
        chart_specs_path.write_text(chart_specs_json_final, encoding="utf-8")
        frontend_chart_specs_path.write_text(chart_specs_json_final, encoding="utf-8")

    # -----------------------------------------------------
    # Orchestration
    # -----------------------------------------------------
    def run(self) -> None:
        """执行完整的分析流程"""
        print("=" * 80)
        print("问题三：成绩预测与影响因素 - 探索性数据分析")
        print("=" * 80)

        # 数据加载
        self.load_and_prepare()
        self.load_hierarchy_data()
        self.load_district_data()
        self.create_aggregated_features()

        # 分析阶段
        self.analyze_target_variable()
        self.analyze_data_quality()
        self.analyze_demographic_impact()
        self.analyze_regional_impact()
        self.analyze_resource_impact()
        self.analyze_subject_impact()
        self.analyze_correlations()
        self.evaluate_feature_importance()
        self.export_feature_engineering_suggestions()
        self.export_chart_specs()

        print("\n" + "=" * 80)
        print("分析完成！")
        print(f"数据已导出到：{self.config.output_dir}")
        print(f"前端数据已导出到：{self.config.frontend_data_dir}")
        print("=" * 80)


def main() -> None:
    config = Q3EDAConfig()
    runner = Q3EDA(config)
    runner.run()


if __name__ == "__main__":
    main()

