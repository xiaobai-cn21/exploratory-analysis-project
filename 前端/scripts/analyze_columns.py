import pandas as pd
import numpy as np

# 读取CSV文件，将"-"视为NaN
df = pd.read_csv('../../数据/AP_IB_Assessment_2024.csv', na_values=['-', ''])

# 定义应该是数值列的列名（即使它们可能包含"-"）
numeric_col_names = [
    'aggregation_index', 'aggregation_code', 'INST_ID', 'LEA_BEDS', 
    'NRC_CODE', 'COUNTY_CODE', 'NYC_IND', 'SUBGROUP_CODE',
    'tested_student_cnt', 'proficient_student_cnt', 
    'level1_cnt', 'level2_cnt', 'level3_cnt', 'level4_cnt', 
    'level5_cnt', 'level6_cnt', 'level7_cnt'
]

# 分析数值列和分类列
numeric_cols = []
categorical_cols = []

for col in df.columns:
    if col in numeric_col_names:
        # 尝试转换为数值类型
        df[col] = pd.to_numeric(df[col], errors='coerce')
        numeric_cols.append(col)
    else:
        categorical_cols.append(col)

# 生成报告
report = []
report.append("# AP_IB_Assessment_2024 数据字典\n")
report.append("## 2.1. Columns\n\n")

# 数值数据表格
report.append("### 1. Numeric Data Table\n\n")
report.append("| Column Name | Type | Min | Max | NaN | Description |\n")
report.append("|-------------|------|-----|-----|-----|-------------|\n")

for col in numeric_cols:
    # 计算统计信息（忽略NaN）
    valid_data = df[col].dropna()
    if len(valid_data) > 0:
        min_val_raw = valid_data.min()
        max_val_raw = valid_data.max()
        # 检查是否为整数类型
        is_int = valid_data.dtype in ['int64', 'int32'] or (valid_data.dtype == 'float64' and (valid_data == valid_data.astype(int)).all())
        if is_int:
            min_val = int(min_val_raw)
            max_val = int(max_val_raw)
        else:
            min_val = min_val_raw
            max_val = max_val_raw
    else:
        min_val = 'N/A'
        max_val = 'N/A'
    
    nan_count = df[col].isna().sum()
    nan_pct = (nan_count / len(df)) * 100
    
    # 确定类型
    if df[col].dtype == 'int64' or (df[col].dtype == 'float64' and len(valid_data) > 0 and (valid_data == valid_data.astype(int)).all()):
        dtype = 'Int'
    else:
        dtype = 'float'
    
    # NaN显示
    nan_display = f"{nan_pct:.2f}%" if nan_count > 0 else "None"
    
    # 描述（根据列名推断）
    desc_map = {
        'aggregation_index': 'Aggregation Level Index',
        'aggregation_code': 'Aggregation Code',
        'INST_ID': 'Institution ID',
        'LEA_BEDS': 'LEA BEDS Code',
        'NRC_CODE': 'NRC Code',
        'COUNTY_CODE': 'County Code',
        'NYC_IND': 'NYC Indicator',
        'SUBGROUP_CODE': 'Subgroup Code',
        'tested_student_cnt': 'Number of Students Tested',
        'proficient_student_cnt': 'Number of Proficient Students',
        'level1_cnt': 'Number of Level 1 Scores',
        'level2_cnt': 'Number of Level 2 Scores',
        'level3_cnt': 'Number of Level 3 Scores',
        'level4_cnt': 'Number of Level 4 Scores',
        'level5_cnt': 'Number of Level 5 Scores',
        'level6_cnt': 'Number of Level 6 Scores',
        'level7_cnt': 'Number of Level 7 Scores',
    }
    description = desc_map.get(col, col.replace('_', ' ').title())
    
    report.append(f"| `{col}` | {dtype} | {min_val} | {max_val} | {nan_display} | {description} |\n")

# 分类数据表格
report.append("\n### 2. Categorical Data Table\n\n")
report.append("| Column Name | Unique | Top | NaN | Description |\n")
report.append("|-------------|--------|-----|-----|-------------|\n")

for col in categorical_cols:
    unique_count = df[col].nunique()
    nan_count = df[col].isna().sum()
    nan_pct = (nan_count / len(df)) * 100
    
    # 最常见的值
    top_value = df[col].mode()[0] if len(df[col].mode()) > 0 else 'N/A'
    if len(str(top_value)) > 30:
        top_value = str(top_value)[:27] + "..."
    
    # NaN显示
    nan_display = f"{nan_pct:.2f}%" if nan_count > 0 else "None"
    
    # 描述（根据列名推断）
    desc_map = {
        'REPORT_SCHOOL_YEAR': 'School Year',
        'aggregation_type': 'Aggregation Type',
        'aggregation_code': 'Aggregation Code',
        'aggregation_name': 'Aggregation Name',
        'INST_ID': 'Institution ID',
        'LEA_BEDS': 'LEA BEDS Code',
        'LEA_NAME': 'LEA Name',
        'NRC_CODE': 'NRC Code',
        'NRC_DESC': 'NRC Description',
        'COUNTY_CODE': 'County Code',
        'COUNTY_NAME': 'County Name',
        'NYC_IND': 'NYC Indicator',
        'SUBGROUP_CODE': 'Subgroup Code',
        'SUBGROUP_NAME': 'Subgroup Name',
        'APIB_IND': 'AP/IB Indicator',
        'SUBJECT_AREA': 'Subject Area',
        'STATE_CODE': 'State Code',
        'ITEM_DESC': 'Item Description',
        'GRADE_LEVEL': 'Grade Level',
    }
    description = desc_map.get(col, col.replace('_', ' ').title())
    
    report.append(f"| `{col}` | {unique_count} | {top_value} | {nan_display} | {description} |\n")

# 保存报告
with open('../../文档/AP_IB_Assessment_2024_数据字典.md', 'w', encoding='utf-8') as f:
    f.write(''.join(report))

print("报告已生成: 文档/AP_IB_Assessment_2024_数据字典.md")
print(f"\n数值列数量: {len(numeric_cols)}")
print(f"分类列数量: {len(categorical_cols)}")
print(f"总列数: {len(df.columns)}")

