"""
按聚合层级分离评估表数据
功能：简单筛选 -> 数据质量检查 -> 评估层级关系映射可行性
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import sys
import io

# 设置标准输出编码为UTF-8，避免Windows控制台中文乱码
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# 配置
INPUT_FILE = r"analysis_results/AP_IB_Assessment_2024_cleaned_20251125_141836.csv"
OUTPUT_DIR = Path("data/by_level")
OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# 层级定义
LEVEL_DEFINITIONS = {
    0: {
        'name': 'Statewide',
        'chinese_name': '全州汇总',
        'description': '纽约州所有公立学校的汇总数据'
    },
    1: {
        'name': 'NRC',
        'chinese_name': 'N/RC分类汇总',
        'description': '按需求/资源能力分类的汇总数据'
    },
    2: {
        'name': 'County',
        'chinese_name': '县级汇总',
        'description': '按县汇总的数据'
    },
    3: {
        'name': 'District',
        'chinese_name': '学区级',
        'description': '单个学区的数据'
    },
    4: {
        'name': 'School',
        'chinese_name': '学校级',
        'description': '单个公立学校的数据（最细粒度）'
    }
}

# 各层级应该有的字段（基于业务规则）
EXPECTED_FIELDS = {
    0: {
        'required': ['REPORT_SCHOOL_YEAR', 'aggregation_index', 'aggregation_type', 
                    'aggregation_code', 'aggregation_name', 'SUBGROUP_CODE', 'SUBGROUP_NAME',
                    'APIB_IND', 'SUBJECT_AREA', 'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL'],
        'should_be_null': ['INST_ID', 'LEA_BEDS', 'LEA_NAME', 'NRC_CODE', 'NRC_DESC', 
                          'COUNTY_CODE', 'COUNTY_NAME', 'NYC_IND']
    },
    1: {
        'required': ['REPORT_SCHOOL_YEAR', 'aggregation_index', 'aggregation_type', 
                    'aggregation_code', 'aggregation_name', 'NRC_CODE', 'NRC_DESC',
                    'SUBGROUP_CODE', 'SUBGROUP_NAME', 'APIB_IND', 'SUBJECT_AREA', 
                    'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL'],
        'should_be_null': ['INST_ID', 'LEA_BEDS', 'LEA_NAME', 'COUNTY_CODE', 
                          'COUNTY_NAME', 'NYC_IND']
    },
    2: {
        'required': ['REPORT_SCHOOL_YEAR', 'aggregation_index', 'aggregation_type', 
                    'aggregation_code', 'aggregation_name', 'COUNTY_CODE', 'COUNTY_NAME',
                    'SUBGROUP_CODE', 'SUBGROUP_NAME', 'APIB_IND', 'SUBJECT_AREA', 
                    'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL'],
        'should_be_null': ['INST_ID', 'LEA_BEDS', 'LEA_NAME', 'NRC_CODE', 'NRC_DESC']
    },
    3: {
        'required': ['REPORT_SCHOOL_YEAR', 'aggregation_index', 'aggregation_type', 
                    'aggregation_code', 'aggregation_name', 'INST_ID', 'COUNTY_CODE', 
                    'COUNTY_NAME', 'NRC_CODE', 'NRC_DESC', 'SUBGROUP_CODE', 'SUBGROUP_NAME',
                    'APIB_IND', 'SUBJECT_AREA', 'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL'],
        'should_be_null': ['LEA_BEDS', 'LEA_NAME']
    },
    4: {
        'required': ['REPORT_SCHOOL_YEAR', 'aggregation_index', 'aggregation_type', 
                    'aggregation_code', 'aggregation_name', 'INST_ID', 'LEA_BEDS', 'LEA_NAME',
                    'COUNTY_CODE', 'COUNTY_NAME', 'NRC_CODE', 'NRC_DESC', 
                    'SUBGROUP_CODE', 'SUBGROUP_NAME', 'APIB_IND', 'SUBJECT_AREA', 
                    'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL'],
        'should_be_null': []
    }
}


def load_data(file_path):
    """加载清理后的数据"""
    print(f"正在加载数据文件: {file_path}")
    df = pd.read_csv(file_path, low_memory=False)
    print(f"数据加载完成: 共 {len(df):,} 行, {len(df.columns)} 列")
    return df


def separate_by_level(df):
    """按层级分离数据"""
    print("\n" + "="*80)
    print("步骤1: 按聚合层级分离数据")
    print("="*80)
    
    separated_data = {}
    level_stats = []
    
    for level_index in sorted(LEVEL_DEFINITIONS.keys()):
        level_info = LEVEL_DEFINITIONS[level_index]
        level_data = df[df['aggregation_index'] == level_index].copy()
        
        count = len(level_data)
        percentage = (count / len(df)) * 100 if len(df) > 0 else 0
        
        print(f"\n层级 {level_index} ({level_info['chinese_name']}):")
        print(f"  记录数: {count:,} 行")
        print(f"  占比: {percentage:.2f}%")
        print(f"  说明: {level_info['description']}")
        
        separated_data[level_index] = level_data
        level_stats.append({
            'level_index': level_index,
            'level_name': level_info['name'],
            'chinese_name': level_info['chinese_name'],
            'count': count,
            'percentage': percentage
        })
    
    return separated_data, level_stats


def check_field_completeness(df_level, level_index):
    """检查字段完整性"""
    level_info = EXPECTED_FIELDS.get(level_index, {})
    required_fields = level_info.get('required', [])
    should_be_null = level_info.get('should_be_null', [])
    
    issues = []
    
    # 检查必需字段是否有值
    for field in required_fields:
        if field in df_level.columns:
            null_count = df_level[field].isnull().sum()
            if null_count > 0:
                issues.append({
                    'type': 'missing_required',
                    'field': field,
                    'null_count': null_count,
                    'null_percentage': (null_count / len(df_level)) * 100
                })
    
    # 检查应该为空的字段
    for field in should_be_null:
        if field in df_level.columns:
            non_null_count = df_level[field].notna().sum()
            if non_null_count > 0:
                issues.append({
                    'type': 'unexpected_value',
                    'field': field,
                    'non_null_count': non_null_count,
                    'non_null_percentage': (non_null_count / len(df_level)) * 100
                })
    
    return issues


def validate_separated_data(separated_data):
    """验证分离后的数据质量"""
    print("\n" + "="*80)
    print("步骤2: 数据质量检查")
    print("="*80)
    
    all_issues = {}
    
    for level_index in sorted(separated_data.keys()):
        level_data = separated_data[level_index]
        level_info = LEVEL_DEFINITIONS[level_index]
        
        print(f"\n检查层级 {level_index} ({level_info['chinese_name']}):")
        
        # 字段完整性检查
        issues = check_field_completeness(level_data, level_index)
        
        if issues:
            print(f"  发现 {len(issues)} 个问题:")
            for issue in issues:
                if issue['type'] == 'missing_required':
                    print(f"    ⚠️ 必需字段 '{issue['field']}' 有 {issue['null_count']:,} 个空值 ({issue['null_percentage']:.2f}%)")
                elif issue['type'] == 'unexpected_value':
                    print(f"    ⚠️ 字段 '{issue['field']}' 应该有 {issue['non_null_count']:,} 个非空值 ({issue['non_null_percentage']:.2f}%)")
            all_issues[level_index] = issues
        else:
            print(f"  ✓ 字段完整性检查通过")
        
        # 基本统计
        print(f"  唯一aggregation_code数: {level_data['aggregation_code'].nunique():,}")
        if 'aggregation_name' in level_data.columns:
            print(f"  唯一aggregation_name数: {level_data['aggregation_name'].nunique():,}")
    
    return all_issues


def assess_hierarchy_mapping_feasibility(separated_data):
    """评估建立层级关系映射表的可行性"""
    print("\n" + "="*80)
    print("步骤3: 评估层级关系映射表可行性")
    print("="*80)
    
    feasibility_report = {
        'can_create_mapping': True,
        'issues': [],
        'recommendations': []
    }
    
    # 检查层级4（学校）的数据
    if 4 in separated_data:
        level4_data = separated_data[4]
        print("\n检查层级4（学校级）数据:")
        
        # 检查关键字段
        required_fields = ['aggregation_code', 'INST_ID', 'LEA_BEDS', 'COUNTY_CODE', 'NRC_CODE']
        missing_fields = [f for f in required_fields if f not in level4_data.columns]
        if missing_fields:
            feasibility_report['can_create_mapping'] = False
            feasibility_report['issues'].append(f"层级4缺少关键字段: {', '.join(missing_fields)}")
            print(f"  ❌ 缺少关键字段: {', '.join(missing_fields)}")
        else:
            # 检查字段完整性
            null_counts = {}
            for field in required_fields:
                null_count = level4_data[field].isnull().sum()
                null_pct = (null_count / len(level4_data)) * 100
                null_counts[field] = {'count': null_count, 'percentage': null_pct}
                if null_count > 0:
                    print(f"  ⚠️ {field}: {null_count:,} 个空值 ({null_pct:.2f}%)")
            
            # 评估可行性
            if null_counts['aggregation_code']['count'] == 0:
                print(f"  ✓ aggregation_code: 完整")
            else:
                feasibility_report['issues'].append(f"层级4的aggregation_code有{null_counts['aggregation_code']['count']}个空值")
            
            if null_counts['LEA_BEDS']['percentage'] < 5:  # 允许5%的缺失
                print(f"  ✓ LEA_BEDS: 可用（缺失率{null_counts['LEA_BEDS']['percentage']:.2f}%）")
            else:
                feasibility_report['issues'].append(f"层级4的LEA_BEDS缺失率过高({null_counts['LEA_BEDS']['percentage']:.2f}%)")
                feasibility_report['recommendations'].append("考虑使用其他字段建立层级关系")
            
            if null_counts['COUNTY_CODE']['percentage'] < 5:
                print(f"  ✓ COUNTY_CODE: 可用（缺失率{null_counts['COUNTY_CODE']['percentage']:.2f}%）")
            else:
                feasibility_report['issues'].append(f"层级4的COUNTY_CODE缺失率过高({null_counts['COUNTY_CODE']['percentage']:.2f}%)")
    
    # 检查层级3（学区）的数据
    if 3 in separated_data:
        level3_data = separated_data[3]
        print("\n检查层级3（学区级）数据:")
        
        required_fields = ['aggregation_code', 'INST_ID', 'COUNTY_CODE']
        missing_fields = [f for f in required_fields if f not in level3_data.columns]
        if missing_fields:
            print(f"  ⚠️ 缺少字段: {', '.join(missing_fields)}")
        else:
            for field in required_fields:
                null_count = level3_data[field].isnull().sum()
                null_pct = (null_count / len(level3_data)) * 100
                if null_count > 0:
                    print(f"  ⚠️ {field}: {null_count:,} 个空值 ({null_pct:.2f}%)")
                else:
                    print(f"  ✓ {field}: 完整")
    
    # 检查层级2（县）的数据
    if 2 in separated_data:
        level2_data = separated_data[2]
        print("\n检查层级2（县级）数据:")
        
        required_fields = ['aggregation_code', 'COUNTY_CODE']
        missing_fields = [f for f in required_fields if f not in level2_data.columns]
        if missing_fields:
            print(f"  ⚠️ 缺少字段: {', '.join(missing_fields)}")
        else:
            for field in required_fields:
                null_count = level2_data[field].isnull().sum()
                null_pct = (null_count / len(level2_data)) * 100
                if null_count > 0:
                    print(f"  ⚠️ {field}: {null_count:,} 个空值 ({null_pct:.2f}%)")
                else:
                    print(f"  ✓ {field}: 完整")
    
    # 尝试建立映射关系
    print("\n尝试建立层级关系映射:")
    if 4 in separated_data and 3 in separated_data:
        level4_data = separated_data[4]
        level3_data = separated_data[3]
        
        # 检查LEA_BEDS是否可以映射到层级3的aggregation_code
        if 'LEA_BEDS' in level4_data.columns and 'aggregation_code' in level3_data.columns:
            # 获取层级4中LEA_BEDS非空的记录
            level4_with_lea = level4_data[level4_data['LEA_BEDS'].notna()].copy()
            if len(level4_with_lea) > 0:
                # 统一数据类型：转换为字符串并去除小数点
                level4_with_lea['LEA_BEDS_str'] = level4_with_lea['LEA_BEDS'].astype(str).str.replace('.0', '', regex=False)
                level3_data['aggregation_code_str'] = level3_data['aggregation_code'].astype(str).str.replace('.0', '', regex=False)
                
                # 检查LEA_BEDS是否能在层级3中找到对应的aggregation_code
                level3_codes = set(level3_data['aggregation_code_str'].unique())
                level4_lea_codes = set(level4_with_lea['LEA_BEDS_str'].unique())
                
                # 计算匹配率
                matched = level4_lea_codes.intersection(level3_codes)
                match_rate = (len(matched) / len(level4_lea_codes)) * 100 if len(level4_lea_codes) > 0 else 0
                
                print(f"  层级4的LEA_BEDS与层级3的aggregation_code匹配率: {match_rate:.2f}%")
                print(f"  可匹配的唯一LEA_BEDS数: {len(matched):,} / {len(level4_lea_codes):,}")
                
                # 检查学校级别的匹配情况
                unique_schools = level4_with_lea['aggregation_code'].nunique()
                schools_with_matching_lea = level4_with_lea[
                    level4_with_lea['LEA_BEDS_str'].isin(level3_codes)
                ]['aggregation_code'].nunique()
                school_match_rate = (schools_with_matching_lea / unique_schools) * 100 if unique_schools > 0 else 0
                print(f"  可匹配的学校数: {schools_with_matching_lea:,} / {unique_schools:,} ({school_match_rate:.2f}%)")
                
                if match_rate > 90:
                    print(f"  ✓ 匹配率良好，可以建立层级关系映射表")
                    feasibility_report['recommendations'].append("可以基于LEA_BEDS建立学校→学区的映射关系")
                elif match_rate > 70:
                    print(f"  ⚠️ 匹配率一般，建议进一步检查")
                    feasibility_report['recommendations'].append("匹配率一般，需要进一步验证数据一致性")
                else:
                    print(f"  ⚠️ 匹配率较低，但可能由于数据格式问题，建议进一步检查")
                    feasibility_report['recommendations'].append(f"LEA_BEDS与层级3的aggregation_code匹配率较低({match_rate:.2f}%)，需要检查数据格式和业务规则")
    
    return feasibility_report


def save_separated_data(separated_data):
    """保存分离后的数据"""
    print("\n" + "="*80)
    print("步骤4: 保存分离后的数据")
    print("="*80)
    
    saved_files = []
    
    for level_index in sorted(separated_data.keys()):
        level_info = LEVEL_DEFINITIONS[level_index]
        level_data = separated_data[level_index]
        
        filename = f"AP_IB_Assessment_2024_level{level_index}_{level_info['name']}_cleaned.csv"
        filepath = OUTPUT_DIR / filename
        
        level_data.to_csv(filepath, index=False, encoding='utf-8-sig')
        saved_files.append(filepath)
        
        print(f"  层级 {level_index} ({level_info['chinese_name']}): {len(level_data):,} 行")
        print(f"    保存至: {filepath}")
    
    return saved_files


def generate_report(separated_data, level_stats, all_issues, feasibility_report, saved_files):
    """生成处理报告"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = Path("analysis_results") / f"level_separation_report_{timestamp}.md"
    
    report_content = []
    report_content.append("# 评估表层级数据分离处理报告\n")
    report_content.append(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n")
    report_content.append(f"**数据文件**: AP_IB_Assessment_2024_cleaned_20251125_141836.csv\n")
    report_content.append(f"**原始数据总行数**: {sum([stats['count'] for stats in level_stats]):,} 行\n")
    
    report_content.append("\n---\n")
    report_content.append("\n## 一、层级数据分布\n")
    report_content.append("\n| 层级索引 | 层级类型 | 中文名称 | 记录数 | 占比 |\n")
    report_content.append("|---------|---------|---------|--------|------|\n")
    for stats in level_stats:
        report_content.append(f"| {stats['level_index']} | {stats['level_name']} | {stats['chinese_name']} | {stats['count']:,} | {stats['percentage']:.2f}% |\n")
    
    report_content.append("\n---\n")
    report_content.append("\n## 二、数据质量检查结果\n")
    
    if all_issues:
        for level_index, issues in all_issues.items():
            level_info = LEVEL_DEFINITIONS[level_index]
            report_content.append(f"\n### 层级 {level_index} ({level_info['chinese_name']})\n")
            report_content.append(f"发现 {len(issues)} 个问题:\n\n")
            for issue in issues:
                if issue['type'] == 'missing_required':
                    report_content.append(f"- ⚠️ 必需字段 '{issue['field']}' 有 {issue['null_count']:,} 个空值 ({issue['null_percentage']:.2f}%)\n")
                elif issue['type'] == 'unexpected_value':
                    report_content.append(f"- ⚠️ 字段 '{issue['field']}' 应该有 {issue['non_null_count']:,} 个非空值 ({issue['non_null_percentage']:.2f}%)\n")
    else:
        report_content.append("\n✓ 所有层级的字段完整性检查通过，未发现数据质量问题。\n")
    
    report_content.append("\n---\n")
    report_content.append("\n## 三、层级关系映射表可行性评估\n")
    
    if feasibility_report['can_create_mapping']:
        report_content.append("\n### 评估结论\n")
        report_content.append("✓ **可以建立层级关系映射表**\n\n")
        
        if feasibility_report['issues']:
            report_content.append("### 发现的问题\n")
            for issue in feasibility_report['issues']:
                report_content.append(f"- ⚠️ {issue}\n")
        
        if feasibility_report['recommendations']:
            report_content.append("\n### 建议\n")
            for rec in feasibility_report['recommendations']:
                report_content.append(f"- {rec}\n")
    else:
        report_content.append("\n### 评估结论\n")
        report_content.append("❌ **不建议建立层级关系映射表**\n\n")
        report_content.append("### 原因\n")
        for issue in feasibility_report['issues']:
            report_content.append(f"- {issue}\n")
    
    report_content.append("\n---\n")
    report_content.append("\n## 四、生成的文件\n")
    report_content.append("\n### 分离后的数据文件\n")
    for filepath in saved_files:
        # 从文件名中提取层级索引，例如: AP_IB_Assessment_2024_level0_Statewide_cleaned.csv
        filename_parts = filepath.stem.split('_')
        level_index = None
        for part in filename_parts:
            if part.startswith('level'):
                level_index = int(part.replace('level', ''))
                break
        if level_index is not None:
            level_info = LEVEL_DEFINITIONS[level_index]
            report_content.append(f"- **层级 {level_index} ({level_info['chinese_name']})**: `{filepath}`\n")
    
    report_content.append("\n---\n")
    report_content.append("\n## 五、使用建议\n")
    report_content.append("\n1. **独立分析**: 使用分离后的数据文件进行各层级的独立分析\n")
    report_content.append("2. **跨层级分析**: 如需跨层级分析，建议先建立层级关系映射表\n")
    report_content.append("3. **数据验证**: 使用层级关系映射表验证数据一致性\n")
    
    # 写入文件
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(''.join(report_content))
    
    print(f"\n处理报告已保存至: {report_file}")
    return report_file


def main():
    """主函数"""
    print("="*80)
    print("评估表层级数据分离处理工具")
    print("="*80)
    
    # 步骤1: 加载数据
    df = load_data(INPUT_FILE)
    
    # 步骤2: 按层级分离
    separated_data, level_stats = separate_by_level(df)
    
    # 步骤3: 数据质量检查
    all_issues = validate_separated_data(separated_data)
    
    # 步骤4: 评估层级关系映射可行性
    feasibility_report = assess_hierarchy_mapping_feasibility(separated_data)
    
    # 步骤5: 保存分离后的数据
    saved_files = save_separated_data(separated_data)
    
    # 步骤6: 生成报告
    report_file = generate_report(separated_data, level_stats, all_issues, 
                                  feasibility_report, saved_files)
    
    print("\n" + "="*80)
    print("处理完成！")
    print("="*80)
    print(f"详细报告已保存至: {report_file}")
    print(f"分离后的数据已保存至: {OUTPUT_DIR}")


if __name__ == "__main__":
    main()

