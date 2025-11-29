"""
训练多个分类模型（Logistic Regression, KNN, SVM, MLP, Naive Bayes, Decision Tree, 
Random Forest, LGBM, XGBoost）来预测学校级别的 AP/IB 是否达标（二分类问题）。
输出 ROC 曲线图和性能指标对比表格。
"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Dict, List, Tuple

import joblib
import matplotlib
matplotlib.use('Agg')  # 使用非交互式后端
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from sklearn.compose import ColumnTransformer
from sklearn.dummy import DummyClassifier
from sklearn.ensemble import RandomForestClassifier
from sklearn.linear_model import LogisticRegression
from sklearn.metrics import (
    accuracy_score,
    auc,
    f1_score,
    precision_score,
    recall_score,
    roc_auc_score,
    roc_curve,
)
from sklearn.model_selection import train_test_split
from sklearn.naive_bayes import GaussianNB
from sklearn.neighbors import KNeighborsClassifier
from sklearn.neural_network import MLPClassifier
from sklearn.pipeline import Pipeline
from sklearn.preprocessing import OneHotEncoder, StandardScaler
from sklearn.svm import SVC
from sklearn.tree import DecisionTreeClassifier

try:
    from lightgbm import LGBMClassifier
except ImportError:
    LGBMClassifier = None

try:
    from xgboost import XGBClassifier
except ImportError:
    XGBClassifier = None


PROJECT_ROOT = Path(__file__).resolve().parents[1]
LEVEL4_PATH = PROJECT_ROOT / "static" / "data" / "csv" / "AP_IB_Assessment_2024_level4_School_cleaned.csv"
OUTPUT_DIR = PROJECT_ROOT / "analysis_results"
MODEL_DIR = PROJECT_ROOT / "static" / "models"

NUMERIC_COLUMNS = [
    "aggregation_index",
    "INST_ID",
    "LEA_BEDS",
    "NRC_CODE",
    "COUNTY_CODE",
    "NYC_IND",
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

NUMERIC_FEATURES = [
    "aggregation_index",
    "NRC_CODE",
    "COUNTY_CODE",
    "NYC_IND",
    "tested_student_cnt",
]

CATEGORICAL_FEATURES = [
    "aggregation_type",
    "LEA_NAME",
    "NRC_DESC",
    "COUNTY_NAME",
    "SUBGROUP_NAME",
    "APIB_IND",
    "SUBJECT_AREA",
    "GRADE_LEVEL",
]

# 达标阈值：proficient_rate >= 0.5 视为达标
PROFICIENCY_THRESHOLD = 0.5


@dataclass
class ClassificationResult:
    name: str
    accuracy_score: float
    recall_score: float
    precision: float
    f1_score: float
    area_under_curve: float


def load_level4_data() -> pd.DataFrame:
    """加载 Level4 数据"""
    if not LEVEL4_PATH.exists():
        raise FileNotFoundError(
            f"找不到 Level4 数据：{LEVEL4_PATH}\n"
            f"请确保数据文件存在于该路径，或修改脚本中的路径。"
        )
    print(f"  读取数据文件: {LEVEL4_PATH}")
    df = pd.read_csv(LEVEL4_PATH)
    print(f"  原始列数: {len(df.columns)}")
    df = df.replace("-", np.nan)
    for col in NUMERIC_COLUMNS:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")
    return df


def engineer_target(df: pd.DataFrame) -> pd.DataFrame:
    """特征工程：计算达标率并转换为二分类标签"""
    if "APIB_IND" not in df.columns:
        raise ValueError("数据中缺少 APIB_IND 列")
    
    df["APIB_IND"] = df["APIB_IND"].astype(str).str.upper().str.strip()
    
    unique_apib = df["APIB_IND"].unique()
    print(f"  APIB_IND 唯一值: {unique_apib}")
    
    ap_mask = df["APIB_IND"] == "AP"
    ib_mask = df["APIB_IND"] == "IB"
    
    print(f"  AP 记录数: {ap_mask.sum()}, IB 记录数: {ib_mask.sum()}")
    
    # 初始化 pass_cnt 列为 0
    df["pass_cnt"] = 0
    
    # 计算 AP 的 pass_cnt (level 3, 4, 5)
    if ap_mask.any():
        ap_pass = df.loc[ap_mask, ["level3_cnt", "level4_cnt", "level5_cnt"]].sum(axis=1)
        df.loc[ap_mask, "pass_cnt"] = ap_pass
    
    # 计算 IB 的 pass_cnt (level 4, 5, 6, 7)
    if ib_mask.any():
        ib_pass = df.loc[ib_mask, ["level4_cnt", "level5_cnt", "level6_cnt", "level7_cnt"]].sum(axis=1)
        df.loc[ib_mask, "pass_cnt"] = ib_pass
    
    # 计算 proficient_rate（避免除以 0）
    df["proficient_rate"] = df["pass_cnt"] / df["tested_student_cnt"].replace(0, np.nan)
    
    print(f"  计算 proficient_rate 后: {len(df)} 行")
    
    # 过滤数据
    before_dropna = len(df)
    df = df.dropna(subset=["proficient_rate"])
    print(f"  删除 proficient_rate 为 NaN 后: {len(df)} 行 (删除了 {before_dropna - len(df)} 行)")
    
    before_test_cnt = len(df)
    df = df[df["tested_student_cnt"] >= 10]
    print(f"  过滤 tested_student_cnt >= 10 后: {len(df)} 行 (删除了 {before_test_cnt - len(df)} 行)")
    
    before_range = len(df)
    df = df[df["proficient_rate"].between(0, 1, inclusive="both")]
    print(f"  过滤 proficient_rate 在 [0,1] 后: {len(df)} 行 (删除了 {before_range - len(df)} 行)")
    
    # 转换为二分类标签：proficient_rate >= 0.5 为达标（1），否则为不达标（0）
    df["is_proficient"] = (df["proficient_rate"] >= PROFICIENCY_THRESHOLD).astype(int)
    
    proficient_count = df["is_proficient"].sum()
    not_proficient_count = len(df) - proficient_count
    print(f"  达标样本数: {proficient_count} ({proficient_count/len(df)*100:.1f}%)")
    print(f"  不达标样本数: {not_proficient_count} ({not_proficient_count/len(df)*100:.1f}%)")
    
    # 填充分类特征的缺失值
    df[CATEGORICAL_FEATURES] = df[CATEGORICAL_FEATURES].fillna("UNKNOWN")
    
    return df


def build_preprocessor() -> ColumnTransformer:
    """构建特征预处理器"""
    # 兼容不同版本的 sklearn
    try:
        # sklearn >= 1.2
        ohe = OneHotEncoder(handle_unknown="ignore", sparse_output=False)
    except TypeError:
        # sklearn < 1.2
        ohe = OneHotEncoder(handle_unknown="ignore", sparse=False)
    
    return ColumnTransformer(
        transformers=[
            ("num", StandardScaler(), NUMERIC_FEATURES),
            ("cat", ohe, CATEGORICAL_FEATURES),
        ],
        remainder="drop",
    )


def get_models(random_state: int = 42) -> Dict[str, object]:
    """获取所有分类模型（包含图片中的10个模型 + train.py中的模型，支持CUDA 13 GPU加速）"""
    print("\n初始化模型（支持 CUDA 13 GPU 加速）...")
    models = {
        # 图片中的模型
        "Logistic Regression": LogisticRegression(random_state=random_state, max_iter=1000),
        "KNN Classifier": KNeighborsClassifier(n_neighbors=5),
        "SVM Classifier Linear": SVC(
            kernel="linear", 
            probability=True, 
            random_state=random_state,
            max_iter=1000,  # 限制迭代次数
            tol=0.001,  # 放宽收敛条件
            cache_size=500  # 增加缓存
        ),
        "SVM Classifier RBF": SVC(
            kernel="rbf", 
            probability=True, 
            random_state=random_state,
            max_iter=1000,  # 限制迭代次数
            tol=0.001,  # 放宽收敛条件
            cache_size=500,  # 增加缓存
            gamma='scale'  # 使用默认 gamma
        ),
        "MLP Classifier": MLPClassifier(
            hidden_layer_sizes=(100, 50),
            max_iter=500,
            random_state=random_state,
            early_stopping=True
        ),
        "Naive Bayes": GaussianNB(),
        "Decision Tree": DecisionTreeClassifier(random_state=random_state, max_depth=20),
        "Random Forest Classifier": RandomForestClassifier(
            n_estimators=100,
            max_depth=20,
            random_state=random_state,
            n_jobs=-1
        ),
        # train.py 中的模型（分类版本）
        "LinearRegression": LogisticRegression(random_state=random_state, max_iter=1000),
        "Lasso": LogisticRegression(penalty='l1', solver='liblinear', C=1000, random_state=random_state, max_iter=1000),
        "RandomForest": RandomForestClassifier(
            n_estimators=300,
            max_depth=20,
            min_samples_leaf=10,
            random_state=random_state,
            n_jobs=-1,
        ),
    }
    
    if LGBMClassifier is not None:
        # 尝试使用 GPU，如果失败则回退到 CPU
        try:
            models["LGBM Classifier"] = LGBMClassifier(
                n_estimators=100,
                random_state=random_state,
                verbose=-1,
                device='gpu',  # 使用 GPU 加速
                gpu_platform_id=0,
                gpu_device_id=0
            )
            print("  ✓ LGBM: 使用 GPU 加速")
        except Exception as e:
            # GPU 不可用时回退到 CPU
            models["LGBM Classifier"] = LGBMClassifier(
                n_estimators=100,
                random_state=random_state,
                verbose=-1
            )
            print(f"  ⚠ LGBM: GPU 不可用，使用 CPU")
    
    if XGBClassifier is not None:
        # 尝试使用 GPU，如果失败则回退到 CPU
        try:
            models["XGBoost Classifier"] = XGBClassifier(
                n_estimators=100,
                random_state=random_state,
                eval_metric="logloss",
                use_label_encoder=False,
                tree_method='gpu_hist',  # 使用 GPU 加速
                gpu_id=0
            )
            print("  ✓ XGBoost Classifier: 使用 GPU 加速")
        except Exception as e:
            # GPU 不可用时回退到 CPU
            models["XGBoost Classifier"] = XGBClassifier(
                n_estimators=100,
                random_state=random_state,
                eval_metric="logloss",
                use_label_encoder=False
            )
            print(f"  ⚠ XGBoost Classifier: GPU 不可用，使用 CPU")
        
        # train.py 中的 XGBoost（分类版本）
        try:
            models["XGBoost"] = XGBClassifier(
                n_estimators=500,
                max_depth=6,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                reg_lambda=1.0,
                reg_alpha=0.0,
                eval_metric="logloss",
                use_label_encoder=False,
                tree_method='gpu_hist',  # 使用 GPU 加速
                random_state=random_state,
                n_jobs=-1,
            )
            print("  ✓ XGBoost: 使用 GPU 加速")
        except Exception as e:
            # GPU 不可用时回退到 CPU
            models["XGBoost"] = XGBClassifier(
                n_estimators=500,
                max_depth=6,
                learning_rate=0.05,
                subsample=0.8,
                colsample_bytree=0.8,
                reg_lambda=1.0,
                reg_alpha=0.0,
                eval_metric="logloss",
                use_label_encoder=False,
                tree_method="hist",
                random_state=random_state,
                n_jobs=-1,
            )
            print(f"  ⚠ XGBoost: GPU 不可用，使用 CPU")
    
    return models


def evaluate_models(
    X_train, X_test, y_train, y_test
) -> Tuple[List[ClassificationResult], Dict[str, Tuple[np.ndarray, np.ndarray, float]], Pipeline]:
    """评估所有分类模型"""
    results: List[ClassificationResult] = []
    roc_curves: Dict[str, Tuple[np.ndarray, np.ndarray, float]] = {}
    best_pipeline = None
    best_auc = 0.0
    
    # 获取模型列表（只调用一次）
    models = get_models()
    
    for name, estimator in models.items():
        print(f"\n训练 {name}...")
        
        pipeline = Pipeline(
            steps=[
                ("preprocess", build_preprocessor()),
                ("model", estimator),
            ]
        )
        
        try:
            pipeline.fit(X_train, y_train)
            y_pred = pipeline.predict(X_test)
            y_pred_proba = pipeline.predict_proba(X_test)[:, 1]
            
            # 计算指标
            acc = accuracy_score(y_test, y_pred)
            rec = recall_score(y_test, y_pred, zero_division=0)
            prec = precision_score(y_test, y_pred, zero_division=0)
            f1 = f1_score(y_test, y_pred, zero_division=0)
            auc_score = roc_auc_score(y_test, y_pred_proba)
            
            # 计算 ROC 曲线
            fpr, tpr, _ = roc_curve(y_test, y_pred_proba)
            roc_curves[name] = (fpr, tpr, auc_score)
            
            results.append(ClassificationResult(name, acc, rec, prec, f1, auc_score))
            
            print(
                f"[{name}] Accuracy={acc:.2f} Recall={rec:.2f} "
                f"Precision={prec:.2f} F1={f1:.2f} AUC={auc_score:.3f}"
            )
            
            # 保存最佳模型（基于 AUC）
            if auc_score > best_auc:
                best_auc = auc_score
                best_pipeline = pipeline
                
        except Exception as e:
            print(f"[{name}] 训练失败: {e}")
            # 如果模型失败，使用默认值
            results.append(ClassificationResult(name, 0.0, 0.0, 0.0, 0.0, 0.0))
    
    return results, roc_curves, best_pipeline


def results_to_dataframe(results: List[ClassificationResult]) -> pd.DataFrame:
    """将结果转换为 DataFrame"""
    df = pd.DataFrame([r.__dict__ for r in results])
    # 按 AUC 降序排序
    return df.sort_values(by="area_under_curve", ascending=False)


def save_results(df: pd.DataFrame) -> None:
    """保存结果到 CSV 和 Markdown"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUTPUT_DIR / "classification_metrics.csv"
    md_path = OUTPUT_DIR / "classification_metrics.md"
    
    df.to_csv(csv_path, index=False)
    
    # 创建 Markdown 表格，最优值用粗体标出
    md_lines = ["| Model | Accuracy_score | Recall_score | Precision | f1_score | Area_under_curve |\n"]
    md_lines.append("|:------|--------------:|-------------:|----------:|---------:|-----------------:|\n")
    
    # 找到每列的最优值
    best_acc = df["accuracy_score"].max()
    best_rec = df["recall_score"].max()
    best_prec = df["precision"].max()
    best_f1 = df["f1_score"].max()
    best_auc = df["area_under_curve"].max()
    
    for _, row in df.iterrows():
        acc_str = f"**{row['accuracy_score']:.2f}**" if row['accuracy_score'] == best_acc else f"{row['accuracy_score']:.2f}"
        rec_str = f"**{row['recall_score']:.2f}**" if row['recall_score'] == best_rec else f"{row['recall_score']:.2f}"
        prec_str = f"**{row['precision']:.2f}**" if row['precision'] == best_prec else f"{row['precision']:.2f}"
        f1_str = f"**{row['f1_score']:.2f}**" if row['f1_score'] == best_f1 else f"{row['f1_score']:.2f}"
        auc_str = f"**{row['area_under_curve']:.3f}**" if row['area_under_curve'] == best_auc else f"{row['area_under_curve']:.3f}"
        
        md_lines.append(
            f"| {row['name']} | {acc_str} | {rec_str} | {prec_str} | {f1_str} | {auc_str} |\n"
        )
    
    md_path.write_text("".join(md_lines), encoding="utf-8")
    
    print(f"\n>>> 指标表已保存：\n- {csv_path}\n- {md_path}")


def create_roc_curve(roc_curves: Dict[str, Tuple[np.ndarray, np.ndarray, float]]) -> None:
    """创建 ROC 曲线图（类似图片2的样式）"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fig_path = OUTPUT_DIR / "roc_curves.png"
    
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    
    fig, ax = plt.subplots(figsize=(10, 8))
    
    # 定义颜色方案（使用tab10调色板）
    colors = plt.cm.tab10(np.linspace(0, 1, len(roc_curves)))
    
    # 绘制每个模型的 ROC 曲线
    for (name, (fpr, tpr, auc_score)), color in zip(roc_curves.items(), colors):
        ax.plot(fpr, tpr, label=f"{name}, AUC={auc_score:.3f}", linewidth=2, color=color)
    
    # 绘制对角线（随机分类器）
    ax.plot([0, 1], [0, 1], 'k--', label='Random Classifier (AUC=0.5)', linewidth=1, alpha=0.5)
    
    ax.set_xlabel('FPR', fontsize=12, fontweight='bold')
    ax.set_ylabel('TPR', fontsize=12, fontweight='bold')
    ax.set_title('ROC', fontsize=14, fontweight='bold')
    ax.legend(loc='lower right', fontsize=9, framealpha=0.9)
    ax.grid(True, alpha=0.3, linestyle='--')
    ax.set_xlim([0.0, 1.0])
    ax.set_ylim([0.0, 1.0])
    
    # 添加标题说明
    fig.text(0.5, 0.02, '图12-14 各模型 ROC 曲线', 
             ha='center', fontsize=11, style='italic')
    
    plt.tight_layout()
    plt.subplots_adjust(bottom=0.08)
    plt.savefig(fig_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f">>> ROC 曲线图已保存：\n- {fig_path}")


def create_metrics_table(df_results: pd.DataFrame) -> None:
    """创建性能指标表格图（类似图片2和3的样式）"""
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    fig_path = OUTPUT_DIR / "classification_metrics_table.png"
    
    # 设置中文字体
    plt.rcParams['font.sans-serif'] = ['SimHei', 'Arial Unicode MS', 'DejaVu Sans']
    plt.rcParams['axes.unicode_minus'] = False
    
    fig, ax = plt.subplots(figsize=(14, 10))
    ax.axis('tight')
    ax.axis('off')
    
    # 找到最优值
    best_acc = df_results["accuracy_score"].max()
    best_rec = df_results["recall_score"].max()
    best_prec = df_results["precision"].max()
    best_f1 = df_results["f1_score"].max()
    best_auc = df_results["area_under_curve"].max()
    
    # 准备表格数据（数值格式）
    cell_text = []
    for _, row in df_results.iterrows():
        cell_text.append([
            row['name'],
            f"{row['accuracy_score']:.2f}",
            f"{row['recall_score']:.2f}",
            f"{row['precision']:.2f}",
            f"{row['f1_score']:.2f}",
            f"{row['area_under_curve']:.3f}"
        ])
    
    # 创建表格
    table = ax.table(
        cellText=cell_text,
        colLabels=['Model', 'Accuracy_score', 'Recall_score', 'Precision', 'f1_score', 'Area_under_curve'],
        cellLoc='center',
        loc='center',
        bbox=[0.05, 0.05, 0.9, 0.85]  # 调整位置，留出标题空间
    )
    
    # 设置表格样式
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1, 2.0)  # 调整行高
    
    # 设置表头样式（深蓝色背景，白色文字）
    header_color = '#1a3a5c'
    for i in range(6):
        table[(0, i)].set_facecolor(header_color)
        table[(0, i)].set_text_props(weight='bold', color='white', size=12)
        table[(0, i)].set_height(0.12)
        table[(0, i)].set_edgecolor('white')
        table[(0, i)].set_linewidth(1)
    
    # 设置数据行样式（交替颜色）
    row_colors = ['#f5f5f5', '#ffffff']  # 浅灰和白色
    for i in range(1, len(cell_text) + 1):
        row_color = row_colors[i % 2]
        for j in range(6):
            table[(i, j)].set_facecolor(row_color)
            table[(i, j)].set_text_props(size=11, color='black')
            table[(i, j)].set_height(0.1)
            table[(i, j)].set_edgecolor('#e0e0e0')
            table[(i, j)].set_linewidth(0.5)
    
    # 标记最优值（粗体 + 黄色背景）
    for idx, (_, row) in enumerate(df_results.iterrows(), 1):
        if abs(row['accuracy_score'] - best_acc) < 0.001:
            table[(idx, 1)].set_text_props(weight='bold', size=11, color='black')
            table[(idx, 1)].set_facecolor('#FFD700')
        if abs(row['recall_score'] - best_rec) < 0.001:
            table[(idx, 2)].set_text_props(weight='bold', size=11, color='black')
            table[(idx, 2)].set_facecolor('#FFD700')
        if abs(row['precision'] - best_prec) < 0.001:
            table[(idx, 3)].set_text_props(weight='bold', size=11, color='black')
            table[(idx, 3)].set_facecolor('#FFD700')
        if abs(row['f1_score'] - best_f1) < 0.001:
            table[(idx, 4)].set_text_props(weight='bold', size=11, color='black')
            table[(idx, 4)].set_facecolor('#FFD700')
        if abs(row['area_under_curve'] - best_auc) < 0.001:
            table[(idx, 5)].set_text_props(weight='bold', size=11, color='black')
            table[(idx, 5)].set_facecolor('#FFD700')
    
    # 添加标题（在表格上方）
    fig.text(0.5, 0.95, '表12-16 模型各指标结果', 
             ha='center', fontsize=15, fontweight='bold')
    fig.text(0.5, 0.92, '(加粗数值为每个指标的最优模型结果)', 
             ha='center', fontsize=12, style='italic')
    plt.savefig(fig_path, dpi=300, bbox_inches='tight', facecolor='white')
    plt.close()
    
    print(f">>> 性能指标表格已保存：\n- {fig_path}")


def main() -> None:
    print("\n" + "=" * 80)
    print("Training Classification Models for AP/IB Proficiency Prediction")
    print("=" * 80 + "\n")
    
    print("Loading data...")
    df = load_level4_data()
    print(f"Original data shape: {df.shape}")
    
    print("\nEngineering features...")
    df = engineer_target(df)
    print(f"Final data shape: {df.shape}")
    
    if len(df) == 0:
        raise ValueError(
            "错误：特征工程后数据为空！\n"
            "请检查数据文件和数据质量。"
        )
    
    feature_cols = NUMERIC_FEATURES + CATEGORICAL_FEATURES
    missing_features = set(feature_cols) - set(df.columns)
    if missing_features:
        raise ValueError(f"缺少特征列：{missing_features}")
    
    print(f"Features: {feature_cols}\n")
    
    X = df[feature_cols]
    y = df["is_proficient"]
    
    X_train, X_test, y_train, y_test = train_test_split(
        X, y, test_size=0.2, random_state=42, stratify=y
    )
    
    print(
        f">>> 数据就绪：样本 {len(df):,}，训练 {len(X_train):,}，测试 {len(X_test):,}"
    )
    print(f">>> 训练集中达标率: {y_train.mean():.2%}, 测试集中达标率: {y_test.mean():.2%}\n")
    
    results, roc_curves, best_pipeline = evaluate_models(X_train, X_test, y_train, y_test)
    df_results = results_to_dataframe(results)
    
    print("\n" + "=" * 80)
    print("=== 分类模型性能指标 ===")
    print("=" * 80)
    print(df_results.to_string(index=False, float_format=lambda x: f"{x:.2f}" if x < 1 else f"{x:.3f}"))
    
    save_results(df_results)
    create_roc_curve(roc_curves)
    create_metrics_table(df_results)
    
    # 保存最佳模型
    if best_pipeline is not None:
        MODEL_DIR.mkdir(parents=True, exist_ok=True)
        model_path = MODEL_DIR / "best_classification_model.pkl"
        joblib.dump(best_pipeline, model_path)
        print(f"\n>>> 最佳分类模型已保存：\n- {model_path}")


if __name__ == "__main__":
    main()

