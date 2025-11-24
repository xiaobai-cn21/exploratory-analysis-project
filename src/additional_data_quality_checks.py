"""
额外的数据质量检查 - 发现可能遗漏的脏数据

这个脚本检查当前数据清洗脚本可能遗漏的数据质量问题：
1. 数据一致性：level字段总和与tested_student_cnt的一致性
2. proficient_student_cnt与level字段的一致性
3. 数值字段的合理性（负数、小数等）
4. 字段间的逻辑关系一致性
5. 重复数据检查
"""

import pandas as pd
import numpy as np
from pathlib import Path

def check_level_sum_consistency(df):
    """检查评估表中level字段总和与tested_student_cnt的一致性
    
    只检查数值齐全的行（所有level字段都是有效数值，不包含'-'）
    包含'-'的行会被排除，因为它们可以不一致
    """
    if 'tested_student_cnt' not in df.columns:
        return []
    
    issues = []
    level_fields = ['level1_cnt', 'level2_cnt', 'level3_cnt', 'level4_cnt', 
                    'level5_cnt', 'level6_cnt', 'level7_cnt']
    
    # 检查所有level字段是否存在
    missing_fields = [f for f in level_fields if f not in df.columns]
    if missing_fields:
        return [{'type': 'missing_fields', 'fields': missing_fields}]
    
    # 过滤出数值齐全的行（所有level字段都不包含'-'且是有效数值）
    def is_valid_numeric(val):
        """检查值是否为有效数值（不是'-'、空值或NaN）"""
        if pd.isna(val) or val == '-' or val == '':
            return False
        try:
            float(val)
            return True
        except:
            return False
    
    # 检查每一行的所有level字段是否都是有效数值
    valid_mask = pd.Series([True] * len(df), index=df.index)
    for field in level_fields:
        valid_mask = valid_mask & df[field].apply(is_valid_numeric)
    
    # 只检查数值齐全的行
    valid_df = df[valid_mask].copy()
    
    if len(valid_df) == 0:
        return [{'type': 'no_valid_rows', 'note': '没有找到所有level字段都是有效数值的行'}]
    
    # 将level字段转换为数值
    def convert_to_num(val):
        try:
            return float(val)
        except:
            return 0
    
    level_sums = pd.Series([0] * len(valid_df), index=valid_df.index)
    for field in level_fields:
        level_sums += valid_df[field].apply(convert_to_num)
    
    tested_cnt = pd.to_numeric(valid_df['tested_student_cnt'], errors='coerce')
    
    # 找出不一致的记录（允许1的误差，因为可能有舍入）
    diff = abs(level_sums - tested_cnt)
    inconsistent = diff > 1  # 允许1的误差
    
    if inconsistent.any():
        issues.append({
            'type': 'level_sum_mismatch',
            'count': inconsistent.sum(),
            'total_valid_rows': len(valid_df),
            'percentage': inconsistent.sum() / len(valid_df) * 100,
            'excluded_rows_with_dash': len(df) - len(valid_df),
            'sample_indices': valid_df[inconsistent].index.tolist()[:10],
            'sample_data': valid_df.loc[valid_df[inconsistent].index[:5], 
                                 ['tested_student_cnt'] + level_fields].to_dict('records')
        })
    
    return issues

def check_proficient_consistency(df):
    """检查proficient_student_cnt与相应level字段的一致性"""
    if 'proficient_student_cnt' not in df.columns or 'APIB_IND' not in df.columns:
        return []
    
    issues = []
    
    def convert_to_num(val):
        if pd.isna(val) or val == '-' or val == '':
            return 0
        try:
            return float(val)
        except:
            return 0
    
    # AP评估：proficient = level3 + level4 + level5
    ap_mask = df['APIB_IND'] == 'AP'
    if ap_mask.any():
        ap_df = df[ap_mask]
        if all(f in ap_df.columns for f in ['level3_cnt', 'level4_cnt', 'level5_cnt']):
            ap_proficient = ap_df['proficient_student_cnt'].apply(convert_to_num)
            ap_sum = (ap_df['level3_cnt'].apply(convert_to_num) + 
                     ap_df['level4_cnt'].apply(convert_to_num) + 
                     ap_df['level5_cnt'].apply(convert_to_num))
            diff = abs(ap_proficient - ap_sum)
            inconsistent = diff > 1
            
            if inconsistent.any():
                issues.append({
                    'type': 'ap_proficient_mismatch',
                    'count': inconsistent.sum(),
                    'percentage': inconsistent.sum() / len(ap_df) * 100,
                    'sample_indices': ap_df[inconsistent].index.tolist()[:10]
                })
    
    # IB评估：proficient = level4 + level5 + level6 + level7
    ib_mask = df['APIB_IND'] == 'IB'
    if ib_mask.any():
        ib_df = df[ib_mask]
        if all(f in ib_df.columns for f in ['level4_cnt', 'level5_cnt', 'level6_cnt', 'level7_cnt']):
            ib_proficient = ib_df['proficient_student_cnt'].apply(convert_to_num)
            ib_sum = (ib_df['level4_cnt'].apply(convert_to_num) + 
                     ib_df['level5_cnt'].apply(convert_to_num) + 
                     ib_df['level6_cnt'].apply(convert_to_num) + 
                     ib_df['level7_cnt'].apply(convert_to_num))
            diff = abs(ib_proficient - ib_sum)
            inconsistent = diff > 1
            
            if inconsistent.any():
                issues.append({
                    'type': 'ib_proficient_mismatch',
                    'count': inconsistent.sum(),
                    'percentage': inconsistent.sum() / len(ib_df) * 100,
                    'sample_indices': ib_df[inconsistent].index.tolist()[:10]
                })
    
    return issues

def check_numeric_validity(df, table_name):
    """检查数值字段的合理性（负数、小数等）"""
    issues = []
    
    if table_name == 'assessment':
        numeric_fields = ['tested_student_cnt', 'proficient_student_cnt',
                         'level1_cnt', 'level2_cnt', 'level3_cnt', 'level4_cnt',
                         'level5_cnt', 'level6_cnt', 'level7_cnt']
    else:
        numeric_fields = ['student_count']
    
    for field in numeric_fields:
        if field not in df.columns:
            continue
        
        # 转换为数值
        numeric_vals = pd.to_numeric(df[field], errors='coerce')
        
        # 检查负数（排除'-'和NaN）
        valid_mask = (df[field] != '-') & df[field].notna() & (df[field].astype(str).str.strip() != '')
        negative = valid_mask & (numeric_vals < 0)
        
        if negative.any():
            issues.append({
                'type': 'negative_value',
                'field': field,
                'count': negative.sum(),
                'sample_values': df.loc[negative, field].unique().tolist()[:10]
            })
        
        # 检查是否有非整数（排除'-'和NaN）
        if negative.any() or valid_mask.any():
            non_integer = valid_mask & (numeric_vals % 1 != 0) & numeric_vals.notna()
            if non_integer.any():
                issues.append({
                    'type': 'non_integer_value',
                    'field': field,
                    'count': non_integer.sum(),
                    'sample_values': numeric_vals[non_integer].unique().tolist()[:10]
                })
    
    return issues

def check_field_consistency(df):
    """检查字段间的逻辑关系一致性"""
    issues = []
    
    # 检查aggregation_code和aggregation_name的一致性
    if 'aggregation_code' in df.columns and 'aggregation_name' in df.columns:
        # 同一aggregation_code应该有相同的aggregation_name
        code_name_map = df.groupby('aggregation_code')['aggregation_name'].nunique()
        inconsistent_codes = code_name_map[code_name_map > 1]
        
        if len(inconsistent_codes) > 0:
            # 获取每个不一致代码对应的所有名称
            inconsistent_details = {}
            for code in inconsistent_codes.index:
                names = df[df['aggregation_code'] == code]['aggregation_name'].unique()
                inconsistent_details[code] = names.tolist()
            
            issues.append({
                'type': 'code_name_inconsistency',
                'count': len(inconsistent_codes),
                'sample_codes': inconsistent_codes.index.tolist()[:10],
                'details': inconsistent_details
            })
    
    # 检查SUBGROUP_CODE和SUBGROUP_NAME的一致性
    if 'SUBGROUP_CODE' in df.columns and 'SUBGROUP_NAME' in df.columns:
        code_name_map = df.groupby('SUBGROUP_CODE')['SUBGROUP_NAME'].nunique()
        inconsistent_codes = code_name_map[code_name_map > 1]
        
        if len(inconsistent_codes) > 0:
            # 获取每个不一致代码对应的所有名称
            inconsistent_details = {}
            for code in inconsistent_codes.index:
                names = df[df['SUBGROUP_CODE'] == code]['SUBGROUP_NAME'].unique()
                inconsistent_details[code] = names.tolist()
            
            issues.append({
                'type': 'subgroup_code_name_inconsistency',
                'count': len(inconsistent_codes),
                'sample_codes': inconsistent_codes.index.tolist()[:10],
                'details': inconsistent_details
            })
    
    return issues

def check_duplicate_records(df):
    """检查完全重复的记录"""
    # 检查所有字段都相同的重复记录
    duplicates = df[df.duplicated(keep=False)]
    
    if len(duplicates) > 0:
        return [{
            'type': 'duplicate_records',
            'count': len(duplicates),
            'unique_duplicate_groups': len(duplicates) - len(duplicates.drop_duplicates()),
            'sample_indices': duplicates.index.tolist()[:20]
        }]
    
    return []

def check_ib_level_constraints(df):
    """检查IB评估的特殊约束"""
    if 'APIB_IND' not in df.columns:
        return []
    
    issues = []
    ib_mask = df['APIB_IND'] == 'IB'
    
    if ib_mask.any():
        ib_df = df[ib_mask]
        
        # IB评估中，level5, level6, level7不应该全部为0或'-'
        # 至少应该有一些IB评估有这些级别
        level_fields = ['level5_cnt', 'level6_cnt', 'level7_cnt']
        if all(f in ib_df.columns for f in level_fields):
            def has_valid_ib_level(row):
                for field in level_fields:
                    val = row[field]
                    if pd.notna(val) and val != '-' and val != '' and val != '0':
                        try:
                            if float(val) > 0:
                                return True
                        except:
                            pass
                return False
            
            # 检查是否有IB记录完全没有level5-7的有效值
            no_ib_levels = ~ib_df.apply(has_valid_ib_level, axis=1)
            if no_ib_levels.any():
                # 这可能不是错误，但值得注意
                issues.append({
                    'type': 'ib_no_high_levels',
                    'count': no_ib_levels.sum(),
                    'percentage': no_ib_levels.sum() / len(ib_df) * 100,
                    'note': 'IB评估中level5-7全部为0或"-"的记录（可能是数据问题或确实没有高分）'
                })
    
    return issues

def run_additional_checks(csv_path, table_name):
    """运行所有额外的数据质量检查"""
    print(f"\n{'='*80}")
    print(f"对 {table_name} 表进行额外的数据质量检查")
    print(f"{'='*80}")
    
    df = pd.read_csv(csv_path)
    print(f"读取数据: {len(df):,} 条记录")
    
    all_issues = []
    
    # 1. 数据一致性检查（仅评估表）
    if table_name == 'assessment':
        print("\n[1] 检查level字段总和与tested_student_cnt的一致性...")
        print("     (只检查数值齐全的行，包含'-'的行会被排除)")
        issues = check_level_sum_consistency(df)
        if issues:
            for issue in issues:
                if issue.get('type') == 'no_valid_rows':
                    print(f"  ⚠️  {issue.get('note', '')}")
                else:
                    excluded = issue.get('excluded_rows_with_dash', 0)
                    valid_rows = issue.get('total_valid_rows', 0)
                    print(f"  ⚠️  在 {valid_rows:,} 条数值齐全的行中，发现 {issue['count']:,} 条记录不一致 ({issue['percentage']:.2f}%)")
                    if excluded > 0:
                        print(f"     (已排除 {excluded:,} 条包含'-'的行)")
                all_issues.append(('consistency', issue))
        else:
            print("  ✅ level字段总和与tested_student_cnt一致")
        
        print("\n[2] 检查proficient_student_cnt与level字段的一致性...")
        issues = check_proficient_consistency(df)
        if issues:
            for issue in issues:
                print(f"  ⚠️  {issue['type']}: {issue['count']:,} 条记录不一致 ({issue['percentage']:.2f}%)")
                all_issues.append(('proficient', issue))
        else:
            print("  ✅ proficient_student_cnt与level字段一致")
    
    # 3. 数值字段合理性检查
    print("\n[3] 检查数值字段的合理性...")
    issues = check_numeric_validity(df, table_name)
    if issues:
        for issue in issues:
            print(f"  ⚠️  {issue['field']}: {issue['type']} - {issue['count']:,} 条记录")
            all_issues.append(('numeric', issue))
    else:
        print("  ✅ 数值字段合理")
    
    # 4. 字段间逻辑关系检查
    print("\n[4] 检查字段间的逻辑关系一致性...")
    issues = check_field_consistency(df)
    if issues:
        for issue in issues:
            print(f"  ⚠️  {issue['type']}: {issue['count']:,} 个不一致的代码")
            # 显示详细的不一致信息
            if 'details' in issue:
                print(f"      具体不一致的代码和名称：")
                for code, names in list(issue['details'].items())[:5]:  # 只显示前5个
                    if issue['type'] == 'code_name_inconsistency':
                        code_field = 'aggregation_code'
                        name_field = 'aggregation_name'
                    else:  # subgroup_code_name_inconsistency
                        code_field = 'SUBGROUP_CODE'
                        name_field = 'SUBGROUP_NAME'
                    
                    print(f"        - {code_field} '{code}' 对应 {len(names)} 个不同的名称:")
                    for name in names:
                        count = len(df[(df[code_field] == code) & (df[name_field] == name)])
                        print(f"          • '{name}' ({count} 条记录)")
            all_issues.append(('consistency', issue))
    else:
        print("  ✅ 字段间逻辑关系一致")
    
    # 5. 重复数据检查
    print("\n[5] 检查完全重复的记录...")
    issues = check_duplicate_records(df)
    if issues:
        for issue in issues:
            print(f"  ⚠️  发现 {issue['count']:,} 条重复记录")
            all_issues.append(('duplicate', issue))
    else:
        print("  ✅ 无完全重复的记录")
    
    # 6. IB评估特殊约束（仅评估表）
    if table_name == 'assessment':
        print("\n[6] 检查IB评估的特殊约束...")
        issues = check_ib_level_constraints(df)
        if issues:
            for issue in issues:
                print(f"  ⚠️  {issue['type']}: {issue['count']:,} 条记录 ({issue['percentage']:.2f}%)")
                print(f"      {issue.get('note', '')}")
                all_issues.append(('ib_constraint', issue))
        else:
            print("  ✅ IB评估约束检查通过")
    
    # 汇总
    print(f"\n{'='*80}")
    print("检查结果汇总")
    print(f"{'='*80}")
    if all_issues:
        print(f"发现 {len(all_issues)} 类潜在问题")
        for category, issue in all_issues:
            print(f"  - {category}: {issue.get('count', 'N/A')} 条记录/项")
    else:
        print("✅ 未发现额外的数据质量问题")
    
    return all_issues

if __name__ == '__main__':
    # 检查清洗后的数据
    # course_path = 'cleaned_data/course_cleaned.csv'  # 已注释 - 不需要处理course表
    assessment_path = 'cleaned_data/assessment_cleaned.csv'
    
    # if Path(course_path).exists():
    #     run_additional_checks(course_path, 'course')
    
    if Path(assessment_path).exists():
        run_additional_checks(assessment_path, 'assessment')

