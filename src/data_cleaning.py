"""
数据清洗脚本 - 根据字段与约束总结.md文档进行数据验证和清洗

使用方法:
    python src/data_cleaning.py

输出:
    # - cleaned_data/course_cleaned.csv: 清洗后的课程表数据（已注释 - 不处理course表）
    - cleaned_data/assessment_cleaned.csv: 清洗后的评估表数据
    # - cleaned_data/course_violations_report.json: 课程表违规报告(JSON)（已注释 - 不处理course表）
    - cleaned_data/assessment_violations_report.json: 评估表违规报告(JSON)
    # - cleaned_data/course_violations_report.md: 课程表违规报告(Markdown)（已注释 - 不处理course表）
    - cleaned_data/assessment_violations_report.md: 评估表违规报告(Markdown)
"""

import pandas as pd
import numpy as np
from pathlib import Path
from datetime import datetime
import json

def convert_to_python_types(obj):
    """递归地将numpy/pandas类型转换为Python原生类型，以便JSON序列化"""
    if isinstance(obj, (np.integer, np.int64, np.int32, np.int16, np.int8)):
        return int(obj)
    elif isinstance(obj, (np.floating, np.float64, np.float32, np.float16)):
        return float(obj)
    elif isinstance(obj, np.bool_):
        return bool(obj)
    elif isinstance(obj, np.ndarray):
        return obj.tolist()
    elif isinstance(obj, pd.Series):
        return obj.tolist()
    elif isinstance(obj, dict):
        return {key: convert_to_python_types(value) for key, value in obj.items()}
    elif isinstance(obj, (list, tuple)):
        return [convert_to_python_types(item) for item in obj]
    else:
        return obj

# ==================== 约束定义 ====================

# 1. 非空约束
COURSE_REQUIRED_FIELDS = [
    'REPORT_SCHOOL_YEAR', 'aggregation_index', 'aggregation_type', 
    'aggregation_code', 'aggregation_name', 'SUBGROUP_CODE', 'SUBGROUP_NAME',
    'APIB_IND', 'SUBJECT_AREA', 'COURSE_ID', 'COURSE_DESC', 
    'grade_level', 'student_count'
]

ASSESSMENT_REQUIRED_FIELDS = [
    'REPORT_SCHOOL_YEAR', 'aggregation_index', 'aggregation_type',
    'aggregation_code', 'aggregation_name', 'SUBGROUP_CODE', 'SUBGROUP_NAME',
    'APIB_IND', 'SUBJECT_AREA', 'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL',
    'tested_student_cnt', 'proficient_student_cnt', 'level1_cnt', 
    'level2_cnt', 'level3_cnt', 'level4_cnt', 'level5_cnt', 
    'level6_cnt', 'level7_cnt'
]

# 2. 枚举约束
VALID_AGGREGATION_INDEX = [0, 1, 2, 3, 4]
VALID_AGGREGATION_TYPE_COURSE = [
    'Statewide', 'Need/Resource Category', 'County', 'District', 'School'
]
VALID_AGGREGATION_TYPE_ASSESSMENT = [
    'Statewide', 'Need/Resource Category', 'County', 'District', 'Public School'
]
VALID_NRC_CODE = [1, 2, 3, 4, 5, 6, 7]
VALID_NRC_DESC = [
    'NEW YORK CITY', 'LARGE CITY', 'URBAN/SUBURBAN/HIGH NEEDS',
    'RURAL HIGH NEEDS', 'AVERAGE NEEDS', 'LOW NEEDS', 'CHARTER SCHOOL'
]
VALID_APIB_IND = ['AP', 'IB']
VALID_GRADE_LEVEL = ['9th Grade', '10th Grade', '11th Grade', '12th Grade', 'Not HS']

# SUBGROUP_CODE的有效值（24个）
VALID_SUBGROUP_CODES = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 15, 16, 17, 18, 20, 21, 22, 23, 24, 25, 29]

# 课程表学科领域（13个）
VALID_SUBJECT_AREA_COURSE = [
    'ELA', 'Mathematics', 'Science', 'Social Studies', 'Second Languages',
    'Fine and Performing Arts', 'Computer Sciences', 'Business and Marketing',
    'Engineering', 'Miscellaneous', 'Nonsubject Specific', 
    'Physical Education', 'Religious Education'
]

# 评估表学科领域（11个）
VALID_SUBJECT_AREA_ASSESSMENT = [
    'ELA', 'Mathematics', 'Science', 'Social Studies', 'Second Languages',
    'Fine and Performing Arts', 'Computer Sciences', 'Business and Marketing',
    'Global Studies', 'Other', 'Religious Education'
]

# 3. 范围约束
RANGE_CONSTRAINTS = {
    'aggregation_code': (0, 680801040001),
    'INST_ID': (800000033912, 800000093014),
    'LEA_BEDS': (10100010000, 680801040000),
    'COUNTY_CODE': (1, 68),
    'COURSE_ID': (1005, 73041),
    'student_count': (1, 23036),
    'tested_student_cnt': (1, 31488),
}

# 4. 固定值约束
FIXED_VALUES = {
    'REPORT_SCHOOL_YEAR': '2023-24'
}

# 5. aggregation_index与aggregation_type的对应关系
AGGREGATION_MAPPING = {
    0: 'Statewide',
    1: 'Need/Resource Category',
    2: 'County',
    3: 'District',
    4: 'School'  # 评估表中为'Public School'
}

# ==================== 数据清洗函数 ====================

def check_null_constraints(df, required_fields, table_name):
    """检查非空约束"""
    violations = []
    for field in required_fields:
        if field not in df.columns:
            continue
        null_mask = df[field].isna() | (df[field] == '') | (df[field].astype(str).str.strip() == '')
        if null_mask.any():
            count = null_mask.sum()
            violations.append({
                'field': field,
                'count': count,
                'indices': df[null_mask].index.tolist()[:10]  # 只记录前10个
            })
    return violations

def check_enum_constraints(df, table_name):
    """检查枚举约束"""
    violations = []
    
    # aggregation_index
    if 'aggregation_index' in df.columns:
        # 转换为数值进行比较
        agg_idx = pd.to_numeric(df['aggregation_index'], errors='coerce')
        invalid = ~agg_idx.isin(VALID_AGGREGATION_INDEX)
        if invalid.any():
            violations.append({
                'field': 'aggregation_index',
                'count': invalid.sum(),
                'invalid_values': df.loc[invalid, 'aggregation_index'].unique().tolist()[:10]
            })
    
    # aggregation_type
    if 'aggregation_type' in df.columns:
        if table_name == 'course':
            valid_types = VALID_AGGREGATION_TYPE_COURSE
        else:
            valid_types = VALID_AGGREGATION_TYPE_ASSESSMENT
        invalid = ~df['aggregation_type'].isin(valid_types)
        if invalid.any():
            violations.append({
                'field': 'aggregation_type',
                'count': invalid.sum(),
                'invalid_values': df.loc[invalid, 'aggregation_type'].unique().tolist()[:10]
            })
    
    # NRC_CODE
    if 'NRC_CODE' in df.columns:
        nrc_code = pd.to_numeric(df['NRC_CODE'], errors='coerce')
        # 只检查非空值
        non_null = nrc_code.notna()
        invalid = non_null & ~nrc_code.isin(VALID_NRC_CODE)
        if invalid.any():
            violations.append({
                'field': 'NRC_CODE',
                'count': invalid.sum(),
                'invalid_values': df.loc[invalid, 'NRC_CODE'].unique().tolist()[:10]
            })
    
    # NYC_IND
    if 'NYC_IND' in df.columns:
        nyc_ind = pd.to_numeric(df['NYC_IND'], errors='coerce')
        # 只检查非空值，允许0, 1, NaN
        non_null = nyc_ind.notna()
        invalid = non_null & ~nyc_ind.isin([0, 1])
        if invalid.any():
            violations.append({
                'field': 'NYC_IND',
                'count': invalid.sum(),
                'invalid_values': df.loc[invalid, 'NYC_IND'].unique().tolist()[:10]
            })
    
    # APIB_IND
    if 'APIB_IND' in df.columns:
        invalid = ~df['APIB_IND'].isin(VALID_APIB_IND)
        if invalid.any():
            violations.append({
                'field': 'APIB_IND',
                'count': invalid.sum(),
                'invalid_values': df.loc[invalid, 'APIB_IND'].unique().tolist()[:10]
            })
    
    # SUBGROUP_CODE
    if 'SUBGROUP_CODE' in df.columns:
        subgroup_code = pd.to_numeric(df['SUBGROUP_CODE'], errors='coerce')
        invalid = ~subgroup_code.isin(VALID_SUBGROUP_CODES)
        if invalid.any():
            violations.append({
                'field': 'SUBGROUP_CODE',
                'count': invalid.sum(),
                'invalid_values': df.loc[invalid, 'SUBGROUP_CODE'].unique().tolist()[:10]
            })
    
    # grade_level / GRADE_LEVEL
    grade_field = 'grade_level' if 'grade_level' in df.columns else 'GRADE_LEVEL'
    if grade_field in df.columns:
        invalid = ~df[grade_field].isin(VALID_GRADE_LEVEL)
        if invalid.any():
            violations.append({
                'field': grade_field,
                'count': invalid.sum(),
                'invalid_values': df.loc[invalid, grade_field].unique().tolist()[:10]
            })
    
    # SUBJECT_AREA
    if 'SUBJECT_AREA' in df.columns:
        if table_name == 'course':
            valid_areas = VALID_SUBJECT_AREA_COURSE
        else:
            valid_areas = VALID_SUBJECT_AREA_ASSESSMENT
        invalid = ~df['SUBJECT_AREA'].isin(valid_areas)
        if invalid.any():
            violations.append({
                'field': 'SUBJECT_AREA',
                'count': invalid.sum(),
                'invalid_values': df.loc[invalid, 'SUBJECT_AREA'].unique().tolist()[:10]
            })
    
    return violations

def check_range_constraints(df, table_name):
    """检查范围约束"""
    violations = []
    
    for field, (min_val, max_val) in RANGE_CONSTRAINTS.items():
        if field not in df.columns:
            continue
        
        # 转换为数值
        numeric_values = pd.to_numeric(df[field], errors='coerce')
        non_null = numeric_values.notna()
        
        # 检查范围
        out_of_range = non_null & ((numeric_values < min_val) | (numeric_values > max_val))
        if out_of_range.any():
            violations.append({
                'field': field,
                'count': out_of_range.sum(),
                'range': f'[{min_val}, {max_val}]',
                'invalid_values': numeric_values[out_of_range].unique().tolist()[:10]
            })
    
    return violations

def check_fixed_value_constraints(df):
    """检查固定值约束"""
    violations = []
    
    for field, expected_value in FIXED_VALUES.items():
        if field not in df.columns:
            continue
        invalid = df[field] != expected_value
        if invalid.any():
            violations.append({
                'field': field,
                'count': invalid.sum(),
                'expected': expected_value,
                'invalid_values': df.loc[invalid, field].unique().tolist()[:10]
            })
    
    return violations

def check_conditional_constraints(df, table_name):
    """检查条件约束"""
    violations = []
    
    if 'aggregation_index' not in df.columns:
        return violations
    
    # 转换为数值
    agg_idx = pd.to_numeric(df['aggregation_index'], errors='coerce')
    
    # aggregation_index = 0: NRC_CODE, COUNTY_CODE, LEA_BEDS, INST_ID 必须为空
    mask_0 = agg_idx == 0
    if mask_0.any():
        for field in ['NRC_CODE', 'COUNTY_CODE', 'LEA_BEDS', 'INST_ID']:
            if field in df.columns:
                non_null = df.loc[mask_0, field].notna() & (df.loc[mask_0, field].astype(str).str.strip() != '')
                if non_null.any():
                    violations.append({
                        'field': field,
                        'condition': 'aggregation_index = 0',
                        'count': non_null.sum(),
                        'expected': 'NULL',
                        'message': f'{field} should be NULL when aggregation_index = 0'
                    })
    
    # aggregation_index = 1: COUNTY_CODE, LEA_BEDS, INST_ID 必须为空
    mask_1 = agg_idx == 1
    if mask_1.any():
        for field in ['COUNTY_CODE', 'LEA_BEDS', 'INST_ID']:
            if field in df.columns:
                non_null = df.loc[mask_1, field].notna() & (df.loc[mask_1, field].astype(str).str.strip() != '')
                if non_null.any():
                    violations.append({
                        'field': field,
                        'condition': 'aggregation_index = 1',
                        'count': non_null.sum(),
                        'expected': 'NULL',
                        'message': f'{field} should be NULL when aggregation_index = 1'
                    })
    
    # aggregation_index = 2: LEA_BEDS, INST_ID 必须为空
    mask_2 = agg_idx == 2
    if mask_2.any():
        for field in ['LEA_BEDS', 'INST_ID']:
            if field in df.columns:
                non_null = df.loc[mask_2, field].notna() & (df.loc[mask_2, field].astype(str).str.strip() != '')
                if non_null.any():
                    violations.append({
                        'field': field,
                        'condition': 'aggregation_index = 2',
                        'count': non_null.sum(),
                        'expected': 'NULL',
                        'message': f'{field} should be NULL when aggregation_index = 2'
                    })
    
    # aggregation_index = 3: LEA_BEDS 必须为空, INST_ID 必须有值
    mask_3 = agg_idx == 3
    if mask_3.any():
        if 'LEA_BEDS' in df.columns:
            non_null = df.loc[mask_3, 'LEA_BEDS'].notna() & (df.loc[mask_3, 'LEA_BEDS'].astype(str).str.strip() != '')
            if non_null.any():
                violations.append({
                    'field': 'LEA_BEDS',
                    'condition': 'aggregation_index = 3',
                    'count': non_null.sum(),
                    'expected': 'NULL',
                    'message': 'LEA_BEDS should be NULL when aggregation_index = 3'
                })
        if 'INST_ID' in df.columns:
            null_inst = df.loc[mask_3, 'INST_ID'].isna() | (df.loc[mask_3, 'INST_ID'].astype(str).str.strip() == '')
            if null_inst.any():
                violations.append({
                    'field': 'INST_ID',
                    'condition': 'aggregation_index = 3',
                    'count': null_inst.sum(),
                    'expected': 'NOT NULL',
                    'message': 'INST_ID should NOT be NULL when aggregation_index = 3'
                })
    
    # aggregation_index = 4: 所有地理字段必须有值
    mask_4 = agg_idx == 4
    if mask_4.any():
        geo_fields = ['NRC_CODE', 'COUNTY_CODE', 'LEA_BEDS', 'INST_ID']
        for field in geo_fields:
            if field in df.columns:
                null_geo = df.loc[mask_4, field].isna() | (df.loc[mask_4, field].astype(str).str.strip() == '')
                if null_geo.any():
                    violations.append({
                        'field': field,
                        'condition': 'aggregation_index = 4',
                        'count': null_geo.sum(),
                        'expected': 'NOT NULL',
                        'message': f'{field} should NOT be NULL when aggregation_index = 4'
                    })
    
    # 评估表特殊约束：tested_student_cnt < 5 时，成绩字段应为'-'
    if table_name == 'assessment' and 'tested_student_cnt' in df.columns:
        tested_cnt = pd.to_numeric(df['tested_student_cnt'], errors='coerce')
        small_group = tested_cnt < 5
        
        if small_group.any():
            level_fields = ['proficient_student_cnt', 'level1_cnt', 'level2_cnt', 
                          'level3_cnt', 'level4_cnt', 'level5_cnt', 'level6_cnt', 'level7_cnt']
            for field in level_fields:
                if field in df.columns:
                    not_dash = df.loc[small_group, field] != '-'
                    if not_dash.any():
                        violations.append({
                            'field': field,
                            'condition': 'tested_student_cnt < 5',
                            'count': not_dash.sum(),
                            'expected': "'-'",
                            'message': f'{field} should be "-" when tested_student_cnt < 5 (privacy protection)'
                        })
    
    return violations

def check_business_logic_constraints(df, table_name):
    """检查业务逻辑约束"""
    violations = []
    
    if table_name == 'assessment' and 'APIB_IND' in df.columns:
        # AP评估：level6和level7应为0或'-'
        ap_mask = df['APIB_IND'] == 'AP'
        if ap_mask.any():
            for field in ['level6_cnt', 'level7_cnt']:
                if field in df.columns:
                    ap_values = df.loc[ap_mask, field]
                    # 转换为数值，检查是否为0或'-'
                    numeric_values = pd.to_numeric(ap_values, errors='coerce')
                    # 违规条件：既不是0，也不是'-'，也不是NaN
                    # 即：数值不为NaN且不等于0，且原始值不等于'-'
                    invalid = (numeric_values.notna() & (numeric_values != 0)) & (ap_values != '-')
                    if invalid.any():
                        violations.append({
                            'field': field,
                            'condition': 'APIB_IND = AP',
                            'count': invalid.sum(),
                            'expected': "0 or '-'",
                            'message': f'{field} should be 0 or "-" for AP assessments'
                        })
    
    return violations

def check_aggregation_mapping(df, table_name):
    """检查aggregation_index与aggregation_type的对应关系"""
    violations = []
    
    if 'aggregation_index' not in df.columns or 'aggregation_type' not in df.columns:
        return violations
    
    agg_idx = pd.to_numeric(df['aggregation_index'], errors='coerce')
    
    for idx, expected_type in AGGREGATION_MAPPING.items():
        if table_name == 'assessment' and idx == 4:
            expected_type = 'Public School'
        
        mask = agg_idx == idx
        if mask.any():
            actual_types = df.loc[mask, 'aggregation_type']
            invalid = actual_types != expected_type
            if invalid.any():
                violations.append({
                    'field': 'aggregation_type',
                    'condition': f'aggregation_index = {idx}',
                    'count': invalid.sum(),
                    'expected': expected_type,
                    'invalid_values': actual_types[invalid].unique().tolist()[:10]
                })
    
    return violations

def preprocess_data(df):
    """数据预处理：统一数据类型、处理空值等"""
    df = df.copy()
    
    # 将空字符串转换为NaN
    df = df.replace('', np.nan)
    df = df.replace(' ', np.nan)
    
    # 去除字符串字段的前后空格
    for col in df.select_dtypes(include=['object']).columns:
        df[col] = df[col].astype(str).str.strip()
        df[col] = df[col].replace('nan', np.nan)
        df[col] = df[col].replace('', np.nan)
    
    return df

def clean_data(df, table_name, output_dir='cleaned_data'):
    """执行完整的数据清洗"""
    print(f"\n{'='*80}")
    print(f"开始清洗 {table_name} 表数据")
    print(f"{'='*80}")
    
    # 数据预处理
    print("\n[0] 数据预处理...")
    df = preprocess_data(df)
    
    original_count = len(df)
    print(f"原始记录数: {original_count:,}")
    
    # 记录所有违规
    all_violations = []
    invalid_indices = set()
    
    # 1. 检查非空约束
    print("\n[1] 检查非空约束...")
    required_fields = COURSE_REQUIRED_FIELDS if table_name == 'course' else ASSESSMENT_REQUIRED_FIELDS
    null_violations = check_null_constraints(df, required_fields, table_name)
    if null_violations:
        for v in null_violations:
            print(f"  ❌ {v['field']}: {v['count']} 条记录为空")
            all_violations.append(('NULL', v))
            # 记录违规索引
            if 'indices' in v:
                invalid_indices.update(v['indices'])
    else:
        print("  ✅ 非空约束检查通过")
    
    # 2. 检查枚举约束
    print("\n[2] 检查枚举约束...")
    enum_violations = check_enum_constraints(df, table_name)
    if enum_violations:
        for v in enum_violations:
            print(f"  ❌ {v['field']}: {v['count']} 条记录值无效")
            if 'invalid_values' in v:
                print(f"     无效值示例: {v['invalid_values'][:5]}")
            all_violations.append(('ENUM', v))
    else:
        print("  ✅ 枚举约束检查通过")
    
    # 3. 检查范围约束
    print("\n[3] 检查范围约束...")
    range_violations = check_range_constraints(df, table_name)
    if range_violations:
        for v in range_violations:
            print(f"  ❌ {v['field']}: {v['count']} 条记录超出范围 {v['range']}")
            all_violations.append(('RANGE', v))
    else:
        print("  ✅ 范围约束检查通过")
    
    # 4. 检查固定值约束
    print("\n[4] 检查固定值约束...")
    fixed_violations = check_fixed_value_constraints(df)
    if fixed_violations:
        for v in fixed_violations:
            print(f"  ❌ {v['field']}: {v['count']} 条记录值不正确 (应为 '{v['expected']}')")
            all_violations.append(('FIXED', v))
    else:
        print("  ✅ 固定值约束检查通过")
    
    # 5. 检查条件约束
    print("\n[5] 检查条件约束...")
    cond_violations = check_conditional_constraints(df, table_name)
    if cond_violations:
        for v in cond_violations:
            print(f"  ❌ {v['field']} ({v['condition']}): {v['count']} 条记录违反约束")
            print(f"     {v['message']}")
            all_violations.append(('CONDITIONAL', v))
    else:
        print("  ✅ 条件约束检查通过")
    
    # 6. 检查业务逻辑约束
    print("\n[6] 检查业务逻辑约束...")
    biz_violations = check_business_logic_constraints(df, table_name)
    if biz_violations:
        for v in biz_violations:
            print(f"  ❌ {v['field']} ({v['condition']}): {v['count']} 条记录违反约束")
            print(f"     {v['message']}")
            all_violations.append(('BUSINESS', v))
    else:
        print("  ✅ 业务逻辑约束检查通过")
    
    # 7. 检查aggregation映射
    print("\n[7] 检查aggregation映射...")
    map_violations = check_aggregation_mapping(df, table_name)
    if map_violations:
        for v in map_violations:
            print(f"  ❌ {v['field']} ({v['condition']}): {v['count']} 条记录映射不正确")
            all_violations.append(('MAPPING', v))
    else:
        print("  ✅ aggregation映射检查通过")
    
    # 重新检查所有约束并标记所有违规记录
    print("\n[8] 标记所有违规记录...")
    
    # 创建违规标记
    violation_mask = pd.Series([False] * len(df), index=df.index)
    
    # 1. 非空约束
    for field in required_fields:
        if field in df.columns:
            null_mask = df[field].isna() | (df[field].astype(str).str.strip() == '')
            violation_mask |= null_mask
    
    # 2. 枚举约束
    if 'aggregation_index' in df.columns:
        agg_idx = pd.to_numeric(df['aggregation_index'], errors='coerce')
        invalid_agg = agg_idx.isna() | ~agg_idx.isin(VALID_AGGREGATION_INDEX)
        violation_mask |= invalid_agg
    
    if 'aggregation_type' in df.columns:
        if table_name == 'course':
            valid_types = VALID_AGGREGATION_TYPE_COURSE
        else:
            valid_types = VALID_AGGREGATION_TYPE_ASSESSMENT
        violation_mask |= ~df['aggregation_type'].isin(valid_types)
    
    if 'NRC_CODE' in df.columns:
        nrc_code = pd.to_numeric(df['NRC_CODE'], errors='coerce')
        non_null = nrc_code.notna()
        invalid_nrc = non_null & ~nrc_code.isin(VALID_NRC_CODE)
        violation_mask |= invalid_nrc
    
    if 'NYC_IND' in df.columns:
        nyc_ind = pd.to_numeric(df['NYC_IND'], errors='coerce')
        non_null = nyc_ind.notna()
        invalid_nyc = non_null & ~nyc_ind.isin([0, 1])
        violation_mask |= invalid_nyc
    
    if 'APIB_IND' in df.columns:
        violation_mask |= ~df['APIB_IND'].isin(VALID_APIB_IND)
    
    if 'SUBGROUP_CODE' in df.columns:
        subgroup_code = pd.to_numeric(df['SUBGROUP_CODE'], errors='coerce')
        violation_mask |= ~subgroup_code.isin(VALID_SUBGROUP_CODES)
    
    grade_field = 'grade_level' if 'grade_level' in df.columns else 'GRADE_LEVEL'
    if grade_field in df.columns:
        violation_mask |= ~df[grade_field].isin(VALID_GRADE_LEVEL)
    
    if 'SUBJECT_AREA' in df.columns:
        if table_name == 'course':
            valid_areas = VALID_SUBJECT_AREA_COURSE
        else:
            valid_areas = VALID_SUBJECT_AREA_ASSESSMENT
        violation_mask |= ~df['SUBJECT_AREA'].isin(valid_areas)
    
    # 3. 范围约束
    for field, (min_val, max_val) in RANGE_CONSTRAINTS.items():
        if field in df.columns:
            numeric_values = pd.to_numeric(df[field], errors='coerce')
            non_null = numeric_values.notna()
            out_of_range = non_null & ((numeric_values < min_val) | (numeric_values > max_val))
            violation_mask |= out_of_range
    
    # 4. 固定值约束
    for field, expected_value in FIXED_VALUES.items():
        if field in df.columns:
            violation_mask |= df[field] != expected_value
    
    # 5. 条件约束 - aggregation_index相关的字段约束
    if 'aggregation_index' in df.columns:
        agg_idx = pd.to_numeric(df['aggregation_index'], errors='coerce')
        
        # aggregation_index = 0: NRC_CODE, COUNTY_CODE, LEA_BEDS, INST_ID 必须为空
        mask_0 = agg_idx == 0
        if mask_0.any():
            for field in ['NRC_CODE', 'COUNTY_CODE', 'LEA_BEDS', 'INST_ID']:
                if field in df.columns:
                    non_null = df.loc[mask_0, field].notna() & (df.loc[mask_0, field].astype(str).str.strip() != '')
                    violation_mask.loc[mask_0 & non_null] = True
        
        # aggregation_index = 1: COUNTY_CODE, LEA_BEDS, INST_ID 必须为空
        mask_1 = agg_idx == 1
        if mask_1.any():
            for field in ['COUNTY_CODE', 'LEA_BEDS', 'INST_ID']:
                if field in df.columns:
                    non_null = df.loc[mask_1, field].notna() & (df.loc[mask_1, field].astype(str).str.strip() != '')
                    violation_mask.loc[mask_1 & non_null] = True
        
        # aggregation_index = 2: LEA_BEDS, INST_ID 必须为空
        mask_2 = agg_idx == 2
        if mask_2.any():
            for field in ['LEA_BEDS', 'INST_ID']:
                if field in df.columns:
                    non_null = df.loc[mask_2, field].notna() & (df.loc[mask_2, field].astype(str).str.strip() != '')
                    violation_mask.loc[mask_2 & non_null] = True
        
        # aggregation_index = 3: LEA_BEDS 必须为空, INST_ID 必须有值
        mask_3 = agg_idx == 3
        if mask_3.any():
            if 'LEA_BEDS' in df.columns:
                non_null = df.loc[mask_3, 'LEA_BEDS'].notna() & (df.loc[mask_3, 'LEA_BEDS'].astype(str).str.strip() != '')
                violation_mask.loc[mask_3 & non_null] = True
            if 'INST_ID' in df.columns:
                null_inst = df.loc[mask_3, 'INST_ID'].isna() | (df.loc[mask_3, 'INST_ID'].astype(str).str.strip() == '')
                violation_mask.loc[mask_3 & null_inst] = True
        
        # aggregation_index = 4: 所有地理字段必须有值
        mask_4 = agg_idx == 4
        if mask_4.any():
            geo_fields = ['NRC_CODE', 'COUNTY_CODE', 'LEA_BEDS', 'INST_ID']
            for field in geo_fields:
                if field in df.columns:
                    null_geo = df.loc[mask_4, field].isna() | (df.loc[mask_4, field].astype(str).str.strip() == '')
                    violation_mask.loc[mask_4 & null_geo] = True
    
    # 6. 评估表特殊约束：tested_student_cnt < 5 时，成绩字段应为'-'
    if table_name == 'assessment' and 'tested_student_cnt' in df.columns:
        tested_cnt = pd.to_numeric(df['tested_student_cnt'], errors='coerce')
        small_group = tested_cnt < 5
        
        if small_group.any():
            level_fields = ['proficient_student_cnt', 'level1_cnt', 'level2_cnt', 
                          'level3_cnt', 'level4_cnt', 'level5_cnt', 'level6_cnt', 'level7_cnt']
            for field in level_fields:
                if field in df.columns:
                    not_dash = df.loc[small_group, field] != '-'
                    violation_mask.loc[small_group & not_dash] = True
    
    # 7. 业务逻辑约束：AP评估的level6和level7应为0或'-'
    if table_name == 'assessment' and 'APIB_IND' in df.columns:
        ap_mask = df['APIB_IND'] == 'AP'
        if ap_mask.any():
            for field in ['level6_cnt', 'level7_cnt']:
                if field in df.columns:
                    ap_values = df.loc[ap_mask, field]
                    numeric_values = pd.to_numeric(ap_values, errors='coerce')
                    # 不是0，也不是'-'，也不是NaN的记录违规
                    invalid = (numeric_values.notna() & (numeric_values != 0)) & (ap_values != '-')
                    violation_mask.loc[ap_mask & invalid] = True
    
    # 8. aggregation_index与aggregation_type的对应关系
    if 'aggregation_index' in df.columns and 'aggregation_type' in df.columns:
        agg_idx = pd.to_numeric(df['aggregation_index'], errors='coerce')
        for idx, expected_type in AGGREGATION_MAPPING.items():
            if table_name == 'assessment' and idx == 4:
                expected_type = 'Public School'
            mask = agg_idx == idx
            if mask.any():
                invalid = df.loc[mask, 'aggregation_type'] != expected_type
                violation_mask.loc[mask & invalid] = True
    
    # 删除违规记录
    cleaned_df = df[~violation_mask].copy()
    
    removed_count = original_count - len(cleaned_df)
    removal_rate = removed_count / original_count * 100 if original_count > 0 else 0
    
    print(f"\n{'='*80}")
    print("清洗结果汇总")
    print(f"{'='*80}")
    print(f"原始记录数: {original_count:,}")
    print(f"删除违规记录: {removed_count:,} 条")
    print(f"保留有效记录: {len(cleaned_df):,} 条")
    print(f"数据保留率: {removal_rate:.2f}%")
    print(f"违规记录占比: {removal_rate:.2f}%")
    
    # 按违规类型统计
    if all_violations:
        print(f"\n违规类型统计:")
        violation_types = {}
        for v_type, v_data in all_violations:
            if v_type not in violation_types:
                violation_types[v_type] = 0
            violation_types[v_type] += v_data.get('count', 0)
        
        for v_type, count in sorted(violation_types.items(), key=lambda x: x[1], reverse=True):
            print(f"  {v_type}: {count:,} 条违规")
    
    # 保存清洗后的数据
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    output_file = output_path / f"{table_name}_cleaned.csv"
    cleaned_df.to_csv(output_file, index=False, encoding='utf-8-sig')
    print(f"\n✅ 清洗后的数据已保存到: {output_file}")
    
    # 保存违规报告
    report_file = output_path / f"{table_name}_violations_report.json"
    report = {
        'table_name': table_name,
        'original_count': original_count,
        'cleaned_count': len(cleaned_df),
        'removed_count': removed_count,
        'removal_rate': removal_rate,
        'violations': [
            {
                'type': v_type,
                'data': v_data
            }
            for v_type, v_data in all_violations
        ],
        'timestamp': datetime.now().isoformat()
    }
    
    # 生成Markdown格式的报告
    report_md_file = output_path / f"{table_name}_violations_report.md"
    with open(report_md_file, 'w', encoding='utf-8') as f:
        f.write(f"# {table_name.upper()} 数据清洗报告\n\n")
        f.write(f"**生成时间**: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}\n\n")
        f.write(f"## 清洗结果汇总\n\n")
        f.write(f"- **原始记录数**: {original_count:,}\n")
        f.write(f"- **删除违规记录**: {removed_count:,}\n")
        f.write(f"- **保留有效记录**: {len(cleaned_df):,}\n")
        f.write(f"- **数据保留率**: {removal_rate:.2f}%\n")
        f.write(f"- **违规记录占比**: {removal_rate:.2f}%\n\n")
        
        if all_violations:
            f.write(f"## 违规详情\n\n")
            for i, (v_type, v_data) in enumerate(all_violations, 1):
                f.write(f"### {i}. {v_type} 约束违规\n\n")
                f.write(f"- **字段**: {v_data.get('field', 'N/A')}\n")
                f.write(f"- **违规记录数**: {v_data.get('count', 0):,}\n")
                if 'condition' in v_data:
                    f.write(f"- **条件**: {v_data['condition']}\n")
                if 'expected' in v_data:
                    f.write(f"- **期望值**: {v_data['expected']}\n")
                if 'message' in v_data:
                    f.write(f"- **说明**: {v_data['message']}\n")
                if 'invalid_values' in v_data:
                    f.write(f"- **无效值示例**: {v_data['invalid_values'][:5]}\n")
                f.write("\n")
    
    print(f"✅ Markdown报告已保存到: {report_md_file}")
    
    # 转换numpy/pandas类型为Python原生类型以便JSON序列化
    report_serializable = convert_to_python_types(report)
    
    with open(report_file, 'w', encoding='utf-8') as f:
        json.dump(report_serializable, f, ensure_ascii=False, indent=2)
    print(f"✅ 违规报告已保存到: {report_file}")
    
    return cleaned_df, report

def main():
    """主函数"""
    data_dir = Path('数据')
    output_dir = Path('cleaned_data')
    
    # 处理课程表（已注释 - 不需要处理course表）
    # course_file = data_dir / 'AP_IB_Course_2024.csv'
    # if course_file.exists():
    #     print(f"\n读取课程表: {course_file}")
    #     course_df = pd.read_csv(course_file, low_memory=False)
    #     clean_data(course_df, 'course', output_dir)
    # else:
    #     print(f"❌ 文件不存在: {course_file}")
    
    # 处理评估表
    assessment_file = data_dir / 'AP_IB_Assessment_2024.csv'
    if assessment_file.exists():
        print(f"\n读取评估表: {assessment_file}")
        assessment_df = pd.read_csv(assessment_file, low_memory=False)
        clean_data(assessment_df, 'assessment', output_dir)
    else:
        print(f"❌ 文件不存在: {assessment_file}")
    
    print(f"\n{'='*80}")
    print("数据清洗完成！")
    print(f"{'='*80}")

if __name__ == '__main__':
    main()

