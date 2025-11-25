"""验证分离后的数据"""
import pandas as pd
import sys
import io

sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

print("=== 验证分离后的数据 ===\n")

# 文件名映射
level_files = {
    0: "data/by_level/AP_IB_Assessment_2024_level0_Statewide_cleaned.csv",
    1: "data/by_level/AP_IB_Assessment_2024_level1_NRC_cleaned.csv",
    2: "data/by_level/AP_IB_Assessment_2024_level2_County_cleaned.csv",
    3: "data/by_level/AP_IB_Assessment_2024_level3_District_cleaned.csv",
    4: "data/by_level/AP_IB_Assessment_2024_level4_School_cleaned.csv"
}

total_original = 467523
total_separated = 0

for level, filepath in level_files.items():
    df = pd.read_csv(filepath, low_memory=False)
    count = len(df)
    total_separated += count
    print(f"层级 {level}: {count:,} 行")

print(f"\n总行数验证:")
print(f"  原始数据: {total_original:,} 行")
print(f"  分离后总和: {total_separated:,} 行")
print(f"  差异: {total_original - total_separated:,} 行")

if total_original == total_separated:
    print("  ✓ 数据完整性验证通过")
else:
    print("  ⚠️ 数据行数不匹配")

print("\n=== 层级关系映射可行性验证 ===\n")

df4 = pd.read_csv(level_files[4], low_memory=False)
df3 = pd.read_csv(level_files[3], low_memory=False)

print(f"层级4（学校）: {len(df4):,} 行, {df4['aggregation_code'].nunique()} 个唯一学校")
print(f"层级3（学区）: {len(df3):,} 行, {df3['aggregation_code'].nunique()} 个唯一学区")
print(f"\n层级4的LEA_BEDS非空数: {df4['LEA_BEDS'].notna().sum():,}")
print(f"层级4的唯一LEA_BEDS数: {df4['LEA_BEDS'].nunique()}")

# 检查匹配
lea_codes = set(df4['LEA_BEDS'].dropna().astype(str).str.replace('.0', '', regex=False).unique())
district_codes = set(df3['aggregation_code'].astype(str).str.replace('.0', '', regex=False).unique())
matched = lea_codes.intersection(district_codes)

print(f"\n匹配的LEA_BEDS数: {len(matched)} / {len(lea_codes)} ({len(matched)/len(lea_codes)*100:.2f}%)")

if len(matched) / len(lea_codes) > 0.9:
    print("  ✓ 匹配率良好，可以建立层级关系映射表")
else:
    print("  ⚠️ 匹配率较低，需要进一步检查")

