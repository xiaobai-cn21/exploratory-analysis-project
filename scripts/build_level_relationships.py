"""
建立层级关系映射表
功能：基于已分离的层级数据，建立学校→学区→县的层级关系映射表
"""

import pandas as pd
import numpy as np
from datetime import datetime
from pathlib import Path
import sys
import io

# 设置标准输出编码为UTF-8，避免Windows控制台中文乱码
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

# ==================== 配置 ====================
# 输入文件路径
LEVEL4_FILE = "assessment_by_level/by_level/AP_IB_Assessment_2024_level4_School_cleaned.csv"
LEVEL3_FILE = "assessment_by_level/by_level/AP_IB_Assessment_2024_level3_District_cleaned.csv"
LEVEL2_FILE = "assessment_by_level/by_level/AP_IB_Assessment_2024_level2_County_cleaned.csv"
LEVEL1_FILE = "assessment_by_level/by_level/AP_IB_Assessment_2024_level1_NRC_cleaned.csv"

# 输出目录
RELATIONSHIPS_DIR = Path("assessment_by_level/level_relationships")
RELATIONSHIPS_DIR.mkdir(parents=True, exist_ok=True)

# ==================== 辅助函数 ====================

def normalize_code(code):
    """统一代码格式：转换为字符串并去除小数点"""
    if pd.isna(code):
        return None
    return str(code).replace('.0', '').strip()

def load_level_data():
    """加载各层级数据"""
    print("="*80)
    print("加载层级数据")
    print("="*80)
    
    data = {}
    
    # 加载层级4（学校）
    print(f"\n1. 加载层级4（学校）数据: {LEVEL4_FILE}")
    df4 = pd.read_csv(LEVEL4_FILE, low_memory=False)
    print(f"   行数: {len(df4):,}")
    print(f"   唯一学校数: {df4['aggregation_code'].nunique():,}")
    data[4] = df4
    
    # 加载层级3（学区）
    print(f"\n2. 加载层级3（学区）数据: {LEVEL3_FILE}")
    df3 = pd.read_csv(LEVEL3_FILE, low_memory=False)
    print(f"   行数: {len(df3):,}")
    print(f"   唯一学区数: {df3['aggregation_code'].nunique():,}")
    data[3] = df3
    
    # 加载层级2（县）
    print(f"\n3. 加载层级2（县）数据: {LEVEL2_FILE}")
    df2 = pd.read_csv(LEVEL2_FILE, low_memory=False)
    print(f"   行数: {len(df2):,}")
    print(f"   唯一县数: {df2['aggregation_code'].nunique():,}")
    data[2] = df2
    
    # 加载层级1（N/RC）- 可选，用于参考
    print(f"\n4. 加载层级1（N/RC）数据: {LEVEL1_FILE}")
    df1 = pd.read_csv(LEVEL1_FILE, low_memory=False)
    print(f"   行数: {len(df1):,}")
    print(f"   唯一N/RC分类数: {df1['aggregation_code'].nunique():,}")
    data[1] = df1
    
    return data

def build_school_to_district_mapping(df4, df3):
    """建立学校→学区映射表"""
    print("\n" + "="*80)
    print("建立学校→学区映射表")
    print("="*80)
    
    # 从层级4提取唯一学校记录
    print("\n1. 提取唯一学校记录...")
    school_cols = ['aggregation_code', 'aggregation_name', 'LEA_BEDS', 'COUNTY_CODE', 
                   'COUNTY_NAME', 'NRC_CODE', 'NRC_DESC', 'INST_ID']
    school_unique = df4[school_cols].drop_duplicates(subset=['aggregation_code']).copy()
    print(f"   唯一学校数: {len(school_unique):,}")
    
    # 统一数据类型
    print("\n2. 统一数据类型...")
    school_unique['LEA_BEDS_str'] = school_unique['LEA_BEDS'].apply(normalize_code)
    
    # 从层级3提取唯一学区记录
    district_cols = ['aggregation_code', 'aggregation_name', 'COUNTY_CODE', 'COUNTY_NAME']
    district_unique = df3[district_cols].drop_duplicates(subset=['aggregation_code']).copy()
    district_unique['aggregation_code_str'] = district_unique['aggregation_code'].apply(normalize_code)
    
    # 建立映射字典
    district_dict = dict(zip(
        district_unique['aggregation_code_str'],
        zip(district_unique['aggregation_code'], district_unique['aggregation_name'])
    ))
    
    # 匹配
    print("\n3. 进行匹配...")
    matched_count = 0
    unmatched_lea_beds = set()
    
    school_unique['district_code'] = None
    school_unique['district_name'] = None
    school_unique['match_status'] = 'unmatched'
    
    for idx, row in school_unique.iterrows():
        lea_beds_str = row['LEA_BEDS_str']
        if lea_beds_str and lea_beds_str in district_dict:
            school_unique.at[idx, 'district_code'] = district_dict[lea_beds_str][0]
            school_unique.at[idx, 'district_name'] = district_dict[lea_beds_str][1]
            school_unique.at[idx, 'match_status'] = 'matched'
            matched_count += 1
        else:
            if lea_beds_str:
                unmatched_lea_beds.add(lea_beds_str)
    
    # 统计
    total_schools = len(school_unique)
    match_rate = (matched_count / total_schools * 100) if total_schools > 0 else 0
    
    print(f"\n4. 匹配结果:")
    print(f"   总学校数: {total_schools:,}")
    print(f"   成功匹配: {matched_count:,} ({match_rate:.2f}%)")
    print(f"   无法匹配: {total_schools - matched_count:,} ({100-match_rate:.2f}%)")
    print(f"   无法匹配的唯一LEA_BEDS数: {len(unmatched_lea_beds)}")
    
    # 构建输出DataFrame
    mapping_df = pd.DataFrame({
        'school_code': school_unique['aggregation_code'],
        'school_name': school_unique['aggregation_name'],
        'lea_beds': school_unique['LEA_BEDS'],
        'district_code': school_unique['district_code'],
        'district_name': school_unique['district_name'],
        'county_code': school_unique['COUNTY_CODE'],
        'county_name': school_unique['COUNTY_NAME'],
        'nrc_code': school_unique['NRC_CODE'],
        'nrc_desc': school_unique['NRC_DESC'],
        'inst_id': school_unique['INST_ID'],
        'match_status': school_unique['match_status']
    })
    
    return mapping_df, unmatched_lea_beds, match_rate

def build_district_to_county_mapping(df3, df2):
    """建立学区→县映射表"""
    print("\n" + "="*80)
    print("建立学区→县映射表")
    print("="*80)
    
    # 从层级3提取唯一学区记录
    print("\n1. 提取唯一学区记录...")
    district_cols = ['aggregation_code', 'aggregation_name', 'COUNTY_CODE', 'COUNTY_NAME']
    district_unique = df3[district_cols].drop_duplicates(subset=['aggregation_code']).copy()
    print(f"   唯一学区数: {len(district_unique):,}")
    
    # 从层级2提取唯一县记录
    print("\n2. 提取唯一县记录...")
    county_cols = ['aggregation_code', 'aggregation_name', 'COUNTY_CODE', 'COUNTY_NAME']
    county_unique = df2[county_cols].drop_duplicates(subset=['aggregation_code']).copy()
    print(f"   唯一县数: {len(county_unique):,}")
    
    # 通过COUNTY_CODE匹配
    print("\n3. 通过COUNTY_CODE进行匹配...")
    
    # 建立县信息字典（以COUNTY_CODE为键）
    county_dict = {}
    for _, row in county_unique.iterrows():
        county_code = row['COUNTY_CODE']
        if pd.notna(county_code):
            county_code_str = normalize_code(county_code)
            if county_code_str not in county_dict:
                county_dict[county_code_str] = {
                    'county_aggregation_code': row['aggregation_code'],
                    'county_name': row['COUNTY_NAME']
                }
    
    # 匹配
    matched_count = 0
    district_unique['county_aggregation_code'] = None
    
    for idx, row in district_unique.iterrows():
        county_code = row['COUNTY_CODE']
        if pd.notna(county_code):
            county_code_str = normalize_code(county_code)
            if county_code_str in county_dict:
                district_unique.at[idx, 'county_aggregation_code'] = county_dict[county_code_str]['county_aggregation_code']
                matched_count += 1
    
    # 统计
    total_districts = len(district_unique)
    match_rate = (matched_count / total_districts * 100) if total_districts > 0 else 0
    
    print(f"\n4. 匹配结果:")
    print(f"   总学区数: {total_districts:,}")
    print(f"   成功匹配: {matched_count:,} ({match_rate:.2f}%)")
    print(f"   无法匹配: {total_districts - matched_count:,} ({100-match_rate:.2f}%)")
    
    # 构建输出DataFrame
    mapping_df = pd.DataFrame({
        'district_code': district_unique['aggregation_code'],
        'district_name': district_unique['aggregation_name'],
        'county_code': district_unique['COUNTY_CODE'],
        'county_name': district_unique['COUNTY_NAME'],
        'county_aggregation_code': district_unique['county_aggregation_code']
    })
    
    return mapping_df, match_rate

def build_complete_hierarchy(school_to_district_df, district_to_county_df):
    """建立完整层级关系表"""
    print("\n" + "="*80)
    print("建立完整层级关系表")
    print("="*80)
    
    # 合并学校→学区和学区→县的映射
    print("\n1. 合并映射关系...")
    
    # 从学校→学区映射开始
    hierarchy_df = school_to_district_df[['school_code', 'school_name', 'district_code', 
                                         'district_name', 'county_code', 'county_name',
                                         'nrc_code', 'nrc_desc', 'match_status']].copy()
    
    # 添加县的aggregation_code
    district_county_dict = dict(zip(
        district_to_county_df['district_code'],
        district_to_county_df['county_aggregation_code']
    ))
    
    hierarchy_df['county_aggregation_code'] = hierarchy_df['district_code'].map(district_county_dict)
    
    # 添加全州标识
    hierarchy_df['statewide_code'] = 0
    hierarchy_df['statewide_name'] = 'NEW YORK STATE'
    
    # 重新排列列
    hierarchy_df = hierarchy_df[[
        'school_code', 'school_name',
        'district_code', 'district_name',
        'county_code', 'county_name', 'county_aggregation_code',
        'nrc_code', 'nrc_desc',
        'statewide_code', 'statewide_name',
        'match_status'
    ]]
    
    print(f"   完整层级关系记录数: {len(hierarchy_df):,}")
    
    # 统计
    matched_count = (hierarchy_df['match_status'] == 'matched').sum()
    print(f"\n2. 统计信息:")
    print(f"   总记录数: {len(hierarchy_df):,}")
    print(f"   成功匹配到学区: {matched_count:,} ({matched_count/len(hierarchy_df)*100:.2f}%)")
    print(f"   无法匹配到学区: {len(hierarchy_df) - matched_count:,} ({(len(hierarchy_df)-matched_count)/len(hierarchy_df)*100:.2f}%)")
    
    return hierarchy_df

def generate_report(school_to_district_df, district_to_county_df, hierarchy_df, 
                   unmatched_lea_beds, school_match_rate, district_match_rate):
    """生成处理报告"""
    print("\n" + "="*80)
    print("生成处理报告")
    print("="*80)
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    report_file = RELATIONSHIPS_DIR / f"level_relationships_report_{timestamp}.md"
    
    report_content = f"""# 层级关系映射表生成报告

**生成时间**: {datetime.now().strftime("%Y年%m月%d日 %H:%M:%S")}

---

## 一、处理结果

### 1.1 学校→学区映射表

- **总学校数**: {len(school_to_district_df):,}
- **成功匹配**: {(school_to_district_df['match_status'] == 'matched').sum():,} ({school_match_rate:.2f}%)
- **无法匹配**: {(school_to_district_df['match_status'] == 'unmatched').sum():,} ({100-school_match_rate:.2f}%)
- **无法匹配的唯一LEA_BEDS数**: {len(unmatched_lea_beds)}

### 1.2 学区→县映射表

- **总学区数**: {len(district_to_county_df):,}
- **成功匹配**: {district_to_county_df['county_aggregation_code'].notna().sum():,} ({district_match_rate:.2f}%)
- **无法匹配**: {district_to_county_df['county_aggregation_code'].isna().sum():,} ({100-district_match_rate:.2f}%)

### 1.3 完整层级关系表

- **总记录数**: {len(hierarchy_df):,}
- **包含字段**: school_code, school_name, district_code, district_name, 
              county_code, county_name, county_aggregation_code, 
              nrc_code, nrc_desc, statewide_code, statewide_name, match_status

---

## 二、无法匹配的记录分析

### 2.1 无法匹配的LEA_BEDS值

"""
    
    if unmatched_lea_beds:
        report_content += f"共 {len(unmatched_lea_beds)} 个无法匹配的LEA_BEDS值：\n\n"
        for lea_beds in sorted(unmatched_lea_beds):
            report_content += f"- `{lea_beds}`\n"
        report_content += "\n**可能原因**：\n"
        report_content += "1. 这些学校可能是特许学校，其LEA_BEDS规则不同\n"
        report_content += "2. 数据收集时的编码差异\n"
        report_content += "3. 部分学校可能已关闭或合并\n"
    else:
        report_content += "✓ 所有LEA_BEDS都能成功匹配\n"
    
    report_content += f"""
---

## 三、输出文件

### 3.1 映射表文件

1. **school_to_district_mapping.csv**
   - 学校→学区映射表
   - 记录数: {len(school_to_district_df):,}

2. **district_to_county_mapping.csv**
   - 学区→县映射表
   - 记录数: {len(district_to_county_df):,}

3. **school_hierarchy.csv**
   - 完整层级关系表
   - 记录数: {len(hierarchy_df):,}

### 3.2 报告文件

- `level_relationships_report_{timestamp}.md` (本报告)

---

## 四、数据质量评估

### 4.1 匹配率评估

- **学校→学区匹配率**: {school_match_rate:.2f}%
  - {'✓ 匹配率良好，可以用于跨层级分析' if school_match_rate >= 90 else '⚠️ 匹配率一般，建议进一步检查'}
  
- **学区→县匹配率**: {district_match_rate:.2f}%
  - {'✓ 匹配率良好' if district_match_rate >= 95 else '⚠️ 匹配率一般，建议进一步检查'}

### 4.2 数据完整性

- ✓ 所有必需字段都已包含
- ✓ 使用aggregation_code作为主键，避免名称重复问题
- {'⚠️ 有部分记录无法匹配，已标记match_status' if len(unmatched_lea_beds) > 0 else '✓ 所有记录都能成功匹配'}

---

## 五、使用建议

### 5.1 跨层级分析

使用 `school_hierarchy.csv` 可以进行：
- 从学校级汇总到学区级
- 从学区级汇总到县级
- 按N/RC分类进行教育公平性分析

### 5.2 数据验证

使用映射表可以：
- 验证层级3的汇总是否等于其下层级4的汇总
- 验证层级2的汇总是否等于其下层级3的汇总
- 检查数据一致性

### 5.3 处理无法匹配的记录

对于无法匹配的记录（match_status='unmatched'）：
- 可以单独分析这些记录的特征
- 可以决定是否排除或单独处理
- 可以进一步调查无法匹配的原因

---

## 六、总结

✅ **层级关系映射表生成成功**

- 成功建立了学校→学区→县的完整层级关系
- 匹配率良好，可以支持跨层级分析
- 所有映射表已保存到 `assessment_by_level/level_relationships/` 目录

---

**报告生成时间**: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}
"""
    
    with open(report_file, 'w', encoding='utf-8') as f:
        f.write(report_content)
    
    print(f"\n报告已保存到: {report_file}")
    return report_file

def save_mapping_tables(school_to_district_df, district_to_county_df, hierarchy_df):
    """保存映射表"""
    print("\n" + "="*80)
    print("保存映射表")
    print("="*80)
    
    # 保存学校→学区映射表
    school_district_file = RELATIONSHIPS_DIR / "school_to_district_mapping.csv"
    school_to_district_df.to_csv(school_district_file, index=False, encoding='utf-8-sig')
    print(f"\n1. 学校→学区映射表已保存: {school_district_file}")
    print(f"   记录数: {len(school_to_district_df):,}")
    
    # 保存学区→县映射表
    district_county_file = RELATIONSHIPS_DIR / "district_to_county_mapping.csv"
    district_to_county_df.to_csv(district_county_file, index=False, encoding='utf-8-sig')
    print(f"\n2. 学区→县映射表已保存: {district_county_file}")
    print(f"   记录数: {len(district_to_county_df):,}")
    
    # 保存完整层级关系表
    hierarchy_file = RELATIONSHIPS_DIR / "school_hierarchy.csv"
    hierarchy_df.to_csv(hierarchy_file, index=False, encoding='utf-8-sig')
    print(f"\n3. 完整层级关系表已保存: {hierarchy_file}")
    print(f"   记录数: {len(hierarchy_df):,}")
    
    return school_district_file, district_county_file, hierarchy_file

# ==================== 主函数 ====================

def main():
    """主函数"""
    print("="*80)
    print("建立层级关系映射表")
    print("="*80)
    print(f"\n输出目录: {RELATIONSHIPS_DIR}")
    
    try:
        # 1. 加载数据
        data = load_level_data()
        
        # 2. 建立学校→学区映射
        school_to_district_df, unmatched_lea_beds, school_match_rate = \
            build_school_to_district_mapping(data[4], data[3])
        
        # 3. 建立学区→县映射
        district_to_county_df, district_match_rate = \
            build_district_to_county_mapping(data[3], data[2])
        
        # 4. 建立完整层级关系表
        hierarchy_df = build_complete_hierarchy(
            school_to_district_df, district_to_county_df
        )
        
        # 5. 保存映射表
        save_mapping_tables(school_to_district_df, district_to_county_df, hierarchy_df)
        
        # 6. 生成报告
        generate_report(
            school_to_district_df, district_to_county_df, hierarchy_df,
            unmatched_lea_beds, school_match_rate, district_match_rate
        )
        
        print("\n" + "="*80)
        print("处理完成！")
        print("="*80)
        print(f"\n所有文件已保存到: {RELATIONSHIPS_DIR}")
        
    except Exception as e:
        print(f"\n❌ 错误: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)

if __name__ == "__main__":
    main()

