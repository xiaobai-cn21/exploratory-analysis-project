"""
评估表重复值检测和删除脚本（整合版）
功能：检测重复值 -> 业务验证 -> 删除错误记录 -> 生成完整报告
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
CSV_FILE = r"数据/AP_IB_Assessment_2024.csv"
OUTPUT_DIR = Path("analysis_results")
OUTPUT_DIR.mkdir(exist_ok=True)

def load_data(file_path):
    """加载评估表数据"""
    print(f"正在加载数据文件: {file_path}")
    df = pd.read_csv(file_path, low_memory=False)
    print(f"数据加载完成: 共 {len(df):,} 行, {len(df.columns)} 列")
    return df

def detect_complete_duplicates(df):
    """检测完全重复的行（所有字段都相同）"""
    print("\n" + "="*80)
    print("检测方法1: 完全重复行检测")
    print("="*80)
    
    # 检测完全重复
    duplicate_mask = df.duplicated(keep=False)
    duplicate_count = duplicate_mask.sum()
    unique_duplicate_groups = df[duplicate_mask].drop_duplicates().shape[0] if duplicate_count > 0 else 0
    
    print(f"检测方法: 使用pandas的duplicated()方法，检测所有字段完全相同的行")
    print(f"完全重复的行数: {duplicate_count:,} 行")
    print(f"唯一重复组数: {unique_duplicate_groups:,} 组")
    
    if duplicate_count > 0:
        # 获取重复行的示例
        duplicate_rows = df[duplicate_mask].head(10)
        print(f"\n前10个重复行示例:")
        print(duplicate_rows.to_string())
        
        # 统计每个重复组的大小
        duplicate_groups = df[df.duplicated(keep=False)].groupby(list(df.columns)).size()
        print(f"\n重复组大小统计:")
        print(f"  最小重复次数: {duplicate_groups.min()}")
        print(f"  最大重复次数: {duplicate_groups.max()}")
        print(f"  平均重复次数: {duplicate_groups.mean():.2f}")
    else:
        print("✓ 未发现完全重复的行")
        duplicate_rows = pd.DataFrame()
    
    return duplicate_mask, duplicate_rows

def detect_key_field_duplicates(df):
    """检测基于关键字段的重复（业务逻辑层面的重复）"""
    print("\n" + "="*80)
    print("检测方法2: 关键字段组合重复检测")
    print("="*80)
    
    # 定义关键字段组合（基于业务逻辑）
    key_fields_combinations = [
        {
            'name': '组合1: 聚合级别+分组+课程+年级',
            'fields': ['aggregation_index', 'aggregation_code', 'SUBGROUP_CODE', 
                      'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL', 'APIB_IND']
        },
        {
            'name': '组合2: 学校+分组+课程+年级（学校级别）',
            'fields': ['INST_ID', 'SUBGROUP_CODE', 'STATE_CODE', 'ITEM_DESC', 
                      'GRADE_LEVEL', 'APIB_IND']
        },
        {
            'name': '组合3: 所有标识字段（最严格，仅学校级别）',
            'fields': ['aggregation_index', 'aggregation_code', 'INST_ID', 
                      'LEA_BEDS', 'SUBGROUP_CODE', 'STATE_CODE', 'ITEM_DESC', 
                      'GRADE_LEVEL', 'APIB_IND']
        },
        {
            'name': '组合4: 聚合代码+分组+课程+年级（适用于所有聚合级别）',
            'fields': ['aggregation_index', 'aggregation_code', 'SUBGROUP_CODE', 
                      'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL', 'APIB_IND']
        }
    ]
    
    results = {}
    
    for combo in key_fields_combinations:
        print(f"\n{combo['name']}:")
        print(f"  关键字段: {', '.join(combo['fields'])}")
        
        # 检查字段是否存在
        missing_fields = [f for f in combo['fields'] if f not in df.columns]
        if missing_fields:
            print(f"  ⚠️ 警告: 以下字段不存在: {', '.join(missing_fields)}")
            continue
        
        # 检测基于这些字段的重复
        subset_df = df[combo['fields']].copy()
        
        # 对于包含INST_ID的组合，只检测INST_ID非空的记录（学校级别）
        if 'INST_ID' in combo['fields']:
            inst_id_not_null = df['INST_ID'].notna()
            subset_df = subset_df[inst_id_not_null]
            print(f"  仅检测INST_ID非空的记录（学校级别）: {len(subset_df):,} 行")
            if len(subset_df) == 0:
                print(f"  ⚠️ 警告: 没有INST_ID非空的记录，跳过此检测")
                results[combo['name']] = {
                    'fields': combo['fields'],
                    'duplicate_count': 0,
                    'unique_groups': 0,
                    'duplicate_mask': None
                }
                continue
        
        # 检查是否有完全为空的组合
        all_null_mask = subset_df.isnull().all(axis=1)
        if all_null_mask.any():
            print(f"  ⚠️ 警告: {all_null_mask.sum()} 行的所有关键字段都为空，跳过这些行")
            subset_df = subset_df[~all_null_mask]
        
        duplicate_mask = subset_df.duplicated(keep=False)
        duplicate_count = duplicate_mask.sum()
        unique_duplicate_groups = subset_df[duplicate_mask].drop_duplicates().shape[0] if duplicate_count > 0 else 0
        
        print(f"  重复记录数: {duplicate_count:,} 行")
        print(f"  唯一重复组数: {unique_duplicate_groups:,} 组")
        
        if duplicate_count > 0:
            # 获取重复行的示例
            duplicate_indices = subset_df[duplicate_mask].index
            # 确保索引在原始DataFrame中有效
            valid_indices = [idx for idx in duplicate_indices[:5] if idx in df.index]
            if valid_indices:
                display_fields = combo['fields'] + ['tested_student_cnt', 'proficient_student_cnt']
                # 只选择存在的字段
                display_fields = [f for f in display_fields if f in df.columns]
                duplicate_examples = df.loc[valid_indices, display_fields]
                print(f"\n  前5个重复记录示例:")
                print(duplicate_examples.to_string())
            
            # 统计重复组大小
            try:
                duplicate_groups = subset_df[duplicate_mask].groupby(combo['fields']).size()
                print(f"\n  重复组大小统计:")
                print(f"    最小重复次数: {duplicate_groups.min()}")
                print(f"    最大重复次数: {duplicate_groups.max()}")
                print(f"    平均重复次数: {duplicate_groups.mean():.2f}")
            except Exception as e:
                print(f"  ⚠️ 无法计算重复组统计: {e}")
        else:
            print(f"  ✓ 未发现基于此字段组合的重复")
        
        results[combo['name']] = {
            'fields': combo['fields'],
            'duplicate_count': duplicate_count,
            'unique_groups': unique_duplicate_groups,
            'duplicate_mask': duplicate_mask if duplicate_count > 0 else None
        }
    
    return results

def analyze_duplicate_details(df, key_field_results):
    """详细分析重复记录"""
    details = {}
    
    for combo_name, result in key_field_results.items():
        if result['duplicate_count'] > 0 and result['duplicate_mask'] is not None:
            try:
                key_fields = result['fields']
                
                # 重新检测以获取正确的索引
                if 'INST_ID' in key_fields:
                    subset_df = df[df['INST_ID'].notna()][key_fields].copy()
                else:
                    subset_df = df[key_fields].copy()
                
                duplicate_mask_subset = subset_df.duplicated(keep=False)
                duplicate_indices = subset_df[duplicate_mask_subset].index
                
                if len(duplicate_indices) == 0:
                    continue
                
                duplicate_df = df.loc[duplicate_indices].copy()
                
                # 按关键字段分组，查看每组重复记录的差异
                grouped = duplicate_df.groupby(key_fields)
                
                group_details = []
                for name, group in list(grouped)[:5]:  # 只分析前5组
                    if len(key_fields) == 1:
                        key_vals = {key_fields[0]: name}
                    else:
                        key_vals = dict(zip(key_fields, name))
                    
                    group_info = {
                        'key_values': key_vals,
                        'count': len(group),
                        'differences': {}
                    }
                    
                    # 检查非关键字段的差异
                    non_key_fields = [col for col in df.columns if col not in key_fields]
                    for field in non_key_fields:
                        unique_vals = group[field].dropna().unique()
                        if len(unique_vals) > 1:
                            vals_list = []
                            for val in unique_vals[:5]:
                                try:
                                    if pd.isna(val):
                                        vals_list.append('NaN')
                                    elif isinstance(val, (int, float)):
                                        vals_list.append(str(val))
                                    else:
                                        vals_list.append(str(val))
                                except:
                                    vals_list.append(str(val))
                            
                            group_info['differences'][field] = {
                                'unique_count': len(unique_vals),
                                'values': vals_list
                            }
                    
                    group_details.append(group_info)
                
                details[combo_name] = {
                    'total_groups': result['unique_groups'],
                    'total_duplicates': result['duplicate_count'],
                    'sample_groups': group_details
                }
            except Exception as e:
                print(f"  警告: 分析 {combo_name} 的重复详情时出错: {e}")
                continue
    
    return details

def remove_duplicate_records(df, key_field_results):
    """删除错误的重复记录"""
    print("\n" + "="*80)
    print("步骤3: 删除错误的重复记录")
    print("="*80)
    
    # 使用组合1或组合4的关键字段（它们相同）
    key_fields = ['aggregation_index', 'aggregation_code', 'SUBGROUP_CODE', 
                  'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL', 'APIB_IND']
    
    # 检测重复
    subset_df = df[key_fields].copy()
    duplicate_mask = subset_df.duplicated(keep=False)
    duplicate_count = duplicate_mask.sum()
    
    if duplicate_count == 0:
        print("未发现重复记录，无需删除")
        return df, []
    
    # 获取所有重复记录
    duplicate_records = df[duplicate_mask].copy()
    
    # 按关键字段分组
    grouped = duplicate_records.groupby(key_fields)
    
    # 识别需要删除的记录（COUNTY_NAME为DELAWARE且COUNTY_CODE为3.0的记录）
    records_to_remove = []
    
    print("分析重复组，识别需要删除的记录...")
    for name, group in grouped:
        if len(group) > 1:  # 只处理重复组
            # 检查是否有DELAWARE且COUNTY_CODE为3.0的记录
            delaware_wrong = group[
                (group['COUNTY_NAME'] == 'DELAWARE') & 
                (group['COUNTY_CODE'] == 3.0)
            ]
            
            if len(delaware_wrong) > 0:
                records_to_remove.extend(delaware_wrong.index.tolist())
                print(f"  发现 {len(delaware_wrong)} 条DELAWARE错误记录（COUNTY_CODE=3.0）")
    
    print(f"\n总共需要删除: {len(records_to_remove)} 条记录")
    
    if len(records_to_remove) == 0:
        print("未发现需要删除的记录")
        return df, []
    
    # 删除记录
    df_cleaned = df.drop(index=records_to_remove)
    
    print(f"删除前: {len(df):,} 行")
    print(f"删除后: {len(df_cleaned):,} 行")
    print(f"删除了: {len(records_to_remove)} 行")
    print(f"删除率: {len(records_to_remove)/len(df)*100:.4f}%")
    
    # 验证删除后是否还有重复
    print("\n验证删除结果...")
    subset_cleaned = df_cleaned[key_fields].copy()
    duplicate_mask_cleaned = subset_cleaned.duplicated(keep=False)
    remaining_duplicates = duplicate_mask_cleaned.sum()
    
    print(f"删除后剩余重复记录: {remaining_duplicates} 行")
    
    if remaining_duplicates == 0:
        print("✓ 所有重复记录已成功删除！")
    else:
        print(f"⚠️ 仍有 {remaining_duplicates} 行重复记录，可能需要进一步处理")
    
    return df_cleaned, records_to_remove

def generate_integrated_report(df_original, df_cleaned, complete_duplicates, key_field_results, 
                               duplicate_details, removed_indices, cleaned_file_path):
    """生成整合的检测和删除报告"""
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = OUTPUT_DIR / f"assessment_重复值检测与处理完整报告_{timestamp}.md"
    
    report_content = []
    report_content.append("# 评估表重复值检测与处理完整报告\n")
    report_content.append(f"**生成时间**: {datetime.now().strftime('%Y年%m月%d日 %H:%M:%S')}\n")
    report_content.append(f"**数据文件**: AP_IB_Assessment_2024.csv\n")
    report_content.append(f"**原始数据总行数**: {len(df_original):,} 行\n")
    report_content.append(f"**原始数据总列数**: {len(df_original.columns)} 列\n")
    
    if cleaned_file_path:
        report_content.append(f"**清理后数据文件**: `{cleaned_file_path}`\n")
        report_content.append(f"**清理后数据总行数**: {len(df_cleaned):,} 行\n")
    
    report_content.append("\n---\n")
    report_content.append("\n## 一、检测方法说明\n")
    
    report_content.append("\n### 1.1 完全重复行检测\n")
    report_content.append("**检测方法**: 使用pandas的`duplicated()`方法，检测所有字段完全相同的行。\n")
    report_content.append("**适用场景**: 识别数据导入、处理过程中产生的完全重复记录。\n")
    report_content.append("**检测逻辑**: 比较每一行的所有字段值，如果所有字段值完全相同，则标记为重复。\n")
    
    report_content.append("\n### 1.2 关键字段组合重复检测\n")
    report_content.append("**检测方法**: 基于业务逻辑定义的关键字段组合，检测可能存在的逻辑重复。\n")
    report_content.append("**适用场景**: 识别业务逻辑层面的重复，即使某些非关键字段不同，但核心业务信息相同。\n")
    report_content.append("**检测逻辑**: 根据评估表的业务含义，定义多个关键字段组合，检测这些组合的重复情况。\n")
    
    report_content.append("\n---\n")
    report_content.append("\n## 二、检测结果\n")
    
    # 完全重复结果
    report_content.append("\n### 2.1 完全重复行检测结果\n")
    duplicate_count = complete_duplicates.sum()
    if duplicate_count > 0:
        unique_duplicate_groups = df_original[complete_duplicates].drop_duplicates().shape[0]
        report_content.append(f"- **完全重复的行数**: {duplicate_count:,} 行\n")
        report_content.append(f"- **唯一重复组数**: {unique_duplicate_groups:,} 组\n")
        report_content.append(f"- **重复率**: {duplicate_count/len(df_original)*100:.2f}%\n")
    else:
        report_content.append("- **完全重复的行数**: 0 行\n")
        report_content.append("- **结论**: ✓ 未发现完全重复的行，数据质量良好\n")
    
    # 关键字段重复结果
    report_content.append("\n### 2.2 关键字段组合重复检测结果\n")
    for combo_name, result in key_field_results.items():
        report_content.append(f"\n#### {combo_name}\n")
        report_content.append(f"**关键字段**: {', '.join(result['fields'])}\n")
        report_content.append(f"**重复记录数**: {result['duplicate_count']:,} 行\n")
        report_content.append(f"**唯一重复组数**: {result['unique_groups']:,} 组\n")
        if result['duplicate_count'] > 0:
            report_content.append(f"**重复率**: {result['duplicate_count']/len(df_original)*100:.2f}%\n")
            
            # 添加详细分析
            if combo_name in duplicate_details:
                detail = duplicate_details[combo_name]
                report_content.append(f"\n**重复记录详细分析**:\n")
                report_content.append(f"- 共发现 {detail['total_groups']} 组重复记录\n")
                report_content.append(f"- 每组重复次数: {detail['total_duplicates'] / detail['total_groups']:.1f} 次\n")
                
                if detail['sample_groups']:
                    report_content.append(f"\n**前5组重复记录示例**:\n")
                    for i, group_info in enumerate(detail['sample_groups'], 1):
                        report_content.append(f"\n**重复组 {i}**:\n")
                        report_content.append(f"- 关键字段值: {group_info['key_values']}\n")
                        report_content.append(f"- 重复次数: {group_info['count']} 次\n")
                        if group_info['differences']:
                            report_content.append(f"- 非关键字段差异:\n")
                            for field, diff_info in list(group_info['differences'].items())[:5]:
                                report_content.append(f"  - `{field}`: 有 {diff_info['unique_count']} 个不同值\n")
                        else:
                            report_content.append(f"- 非关键字段: 所有记录的非关键字段值完全相同\n")
        else:
            report_content.append("**结论**: ✓ 未发现基于此字段组合的重复\n")
    
    report_content.append("\n---\n")
    report_content.append("\n## 三、重复记录处理\n")
    
    if len(removed_indices) > 0:
        report_content.append("\n### 3.1 处理摘要\n")
        report_content.append(f"- **删除记录数**: {len(removed_indices)} 行\n")
        report_content.append(f"- **删除率**: {len(removed_indices)/len(df_original)*100:.4f}%\n")
        report_content.append(f"- **清理后记录数**: {len(df_cleaned):,} 行\n")
        
        report_content.append("\n### 3.2 删除原因\n")
        report_content.append("根据业务验证，发现以下问题：\n\n")
        report_content.append("1. **数据错误**: COUNTY_NAME为DELAWARE的记录错误地使用了COUNTY_CODE=3.0（这是BROOME县的代码）\n")
        report_content.append("2. **正确值**: DELAWARE县的正确COUNTY_CODE应该是12.0\n")
        report_content.append("3. **处理决策**: 删除这些错误的重复记录，保留正确的BROOME县记录（COUNTY_CODE=3.0）\n")
        
        report_content.append("\n### 3.3 删除的记录详情\n")
        removed_records = df_original.loc[removed_indices]
        
        report_content.append(f"共删除 {len(removed_records)} 条记录，详情如下：\n\n")
        
        # 按关键字段分组显示
        key_fields = ['aggregation_index', 'aggregation_code', 'SUBGROUP_CODE', 
                      'STATE_CODE', 'ITEM_DESC', 'GRADE_LEVEL', 'APIB_IND']
        
        grouped = removed_records.groupby(key_fields)
        report_content.append("**删除记录分组**:\n\n")
        
        for i, (name, group) in enumerate(grouped, 1):
            report_content.append(f"**删除组 {i}** (共 {len(group)} 条记录):\n\n")
            
            first_row = group.iloc[0]
            report_content.append("关键字段值:\n")
            for field in key_fields:
                report_content.append(f"- {field}: {first_row[field]}\n")
            
            report_content.append("\n删除的记录:\n")
            for idx, row in group.iterrows():
                report_content.append(f"- 行索引 {idx}: COUNTY_NAME={row['COUNTY_NAME']}, COUNTY_CODE={row['COUNTY_CODE']}\n")
            report_content.append("\n")
        
        report_content.append("\n### 3.4 验证结果\n")
        subset_cleaned = df_cleaned[key_fields].copy()
        duplicate_mask_cleaned = subset_cleaned.duplicated(keep=False)
        remaining_duplicates = duplicate_mask_cleaned.sum()
        
        report_content.append(f"- **删除后剩余重复记录**: {remaining_duplicates} 行\n")
        
        if remaining_duplicates == 0:
            report_content.append("- **结论**: ✓ 所有重复记录已成功删除，数据质量良好\n")
        else:
            report_content.append(f"- **结论**: ⚠️ 仍有 {remaining_duplicates} 行重复记录，可能需要进一步处理\n")
    else:
        report_content.append("\n### 3.1 处理摘要\n")
        report_content.append("**结论**: 未发现需要删除的重复记录，数据质量良好，无需处理。\n")
    
    report_content.append("\n---\n")
    report_content.append("\n## 四、数据质量评估\n")
    
    total_duplicates = complete_duplicates.sum()
    report_content.append(f"\n### 4.1 总体重复情况\n")
    report_content.append(f"- **完全重复行数**: {total_duplicates:,} 行\n")
    report_content.append(f"- **完全重复率**: {total_duplicates/len(df_original)*100:.4f}%\n")
    
    if len(removed_indices) > 0:
        report_content.append(f"- **已删除错误重复记录**: {len(removed_indices)} 行\n")
        report_content.append(f"- **删除后数据行数**: {len(df_cleaned):,} 行\n")
    
    if total_duplicates == 0 and len(removed_indices) == 0:
        report_content.append("\n**评估结论**: ✓ 数据质量优秀，未发现重复记录。\n")
    elif total_duplicates == 0 and len(removed_indices) > 0:
        report_content.append("\n**评估结论**: ✓ 数据质量良好，已成功清理所有错误重复记录。\n")
    elif total_duplicates < len(df_original) * 0.01:
        report_content.append("\n**评估结论**: ✓ 数据质量良好，重复率低于1%，属于正常范围。\n")
    else:
        report_content.append("\n**评估结论**: ⚠️ 数据质量一般，建议进行进一步清理。\n")
    
    report_content.append("\n### 4.2 清理后的数据文件\n")
    if cleaned_file_path:
        report_content.append(f"**清理后的数据文件位置**: `{cleaned_file_path}`\n\n")
        report_content.append("该文件包含以下内容：\n")
        report_content.append(f"- 原始数据行数: {len(df_original):,} 行\n")
        report_content.append(f"- 清理后数据行数: {len(df_cleaned):,} 行\n")
        report_content.append(f"- 删除的记录数: {len(removed_indices)} 行\n")
        report_content.append(f"- 数据列数: {len(df_cleaned.columns)} 列\n")
        report_content.append("\n**使用说明**: 该CSV文件可直接用于后续的数据分析和挖掘工作。\n")
    else:
        report_content.append("**说明**: 未生成清理后的数据文件（未发现需要删除的记录）。\n")
    
    report_content.append("\n### 4.3 建议\n")
    if len(removed_indices) > 0:
        report_content.append("1. ✓ 已成功删除所有错误重复记录\n")
        report_content.append("2. 建议使用清理后的数据文件进行后续分析\n")
        report_content.append("3. 在后续数据处理中继续监控数据质量\n")
    else:
        report_content.append("1. 数据质量良好，无需进行重复值处理\n")
        report_content.append("2. 可以继续进行其他数据预处理步骤（如缺失值处理、异常值检测等）\n")
    
    # 写入文件
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(''.join(report_content))
    
    print(f"\n{'='*80}")
    print(f"完整报告已生成: {report_file}")
    print(f"{'='*80}")
    
    return report_file

def main():
    """主函数"""
    print("="*80)
    print("评估表重复值检测与处理工具（整合版）")
    print("="*80)
    
    # 步骤1: 加载数据
    df_original = load_data(CSV_FILE)
    
    # 步骤2: 检测完全重复
    complete_duplicate_mask, complete_duplicate_examples = detect_complete_duplicates(df_original)
    
    # 步骤3: 检测关键字段重复
    key_field_results = detect_key_field_duplicates(df_original)
    
    # 步骤4: 详细分析重复记录
    duplicate_details = analyze_duplicate_details(df_original, key_field_results)
    
    # 步骤5: 删除错误的重复记录
    df_cleaned, removed_indices = remove_duplicate_records(df_original, key_field_results)
    
    # 步骤6: 保存清理后的数据
    cleaned_file_path = None
    if len(removed_indices) > 0:
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        cleaned_file_path = OUTPUT_DIR / f"AP_IB_Assessment_2024_cleaned_{timestamp}.csv"
        df_cleaned.to_csv(cleaned_file_path, index=False, encoding='utf-8-sig')
        print(f"\n清理后的数据已保存到: {cleaned_file_path}")
    else:
        print("\n未删除任何记录，无需保存清理后的数据")
    
    # 步骤7: 生成整合报告
    report_file = generate_integrated_report(
        df_original, df_cleaned, complete_duplicate_mask, key_field_results,
        duplicate_details, removed_indices, cleaned_file_path
    )
    
    print("\n处理完成！")
    print(f"详细报告已保存至: {report_file}")
    if cleaned_file_path:
        print(f"清理后的数据已保存至: {cleaned_file_path}")

if __name__ == "__main__":
    main()

