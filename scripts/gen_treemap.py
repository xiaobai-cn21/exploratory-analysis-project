import pandas as pd
import json
from pathlib import Path

data_dir = Path('数据')
course_file = data_dir / 'AP_IB_Course_2024.csv'
assessment_file = data_dir / 'AP_IB_Assessment_2024.csv'

print("正在加载数据...")
course_df = pd.read_csv(course_file, low_memory=False)
assessment_df = pd.read_csv(assessment_file, low_memory=False)
print(f"课程数据: {len(course_df):,} 行")
print(f"评估数据: {len(assessment_df):,} 行")

print("\n处理课程数据...")
state_total_students = course_df[course_df['aggregation_index'] == '0']['student_count'].sum()
nrc_course = course_df[course_df['aggregation_index'] == '1'].groupby(['NRC_CODE', 'NRC_DESC', 'APIB_IND']).agg({
    'student_count': 'sum'
}).reset_index()
county_course = course_df[course_df['aggregation_index'] == '2'].groupby(['COUNTY_CODE', 'COUNTY_NAME', 'NRC_CODE', 'APIB_IND']).agg({
    'student_count': 'sum'
}).reset_index()

print("处理评估数据...")
assessment_df['tested_student_cnt'] = pd.to_numeric(
    assessment_df['tested_student_cnt'].replace('-', '0'), 
    errors='coerce'
).fillna(0)
assessment_df['proficient_student_cnt'] = pd.to_numeric(
    assessment_df['proficient_student_cnt'].replace('-', '0'), 
    errors='coerce'
).fillna(0)

nrc_assessment = assessment_df[assessment_df['aggregation_index'] == '1'].groupby(['NRC_CODE']).agg({
    'tested_student_cnt': 'sum',
    'proficient_student_cnt': 'sum'
}).reset_index()
nrc_assessment['proficiency_rate'] = (
    nrc_assessment['proficient_student_cnt'] / 
    nrc_assessment['tested_student_cnt'].replace(0, 1)
).fillna(0)

county_assessment = assessment_df[assessment_df['aggregation_index'] == '2'].groupby(['COUNTY_CODE']).agg({
    'tested_student_cnt': 'sum',
    'proficient_student_cnt': 'sum'
}).reset_index()
county_assessment['proficiency_rate'] = (
    county_assessment['proficient_student_cnt'] / 
    county_assessment['tested_student_cnt'].replace(0, 1)
).fillna(0)

print("构建TreeMap数据结构...")
treemap_data = []

treemap_data.append({
    'Location': '纽约州',
    'Parent': None,
    'Size': int(state_total_students),
    'Color': 0.5,
    'Type': 'State',
    'Description': '全州总计'
})

nrc_dict = {}
for _, row in nrc_course.groupby(['NRC_CODE', 'NRC_DESC']).agg({
    'student_count': 'sum'
}).reset_index().iterrows():
    nrc_code = str(row['NRC_CODE']).replace('.0', '') if pd.notna(row['NRC_CODE']) else 'Unknown'
    nrc_desc = row['NRC_DESC'] if pd.notna(row['NRC_DESC']) else 'Unknown'
    total_students = int(row['student_count'])
    
    proficiency = 0
    if nrc_code != 'Unknown':
        nrc_assess = nrc_assessment[nrc_assessment['NRC_CODE'].astype(str).str.replace('.0', '') == nrc_code]
        if len(nrc_assess) > 0:
            proficiency = float(nrc_assess.iloc[0]['proficiency_rate'])
    
    nrc_name = f"N/RC {nrc_code}: {nrc_desc}"
    treemap_data.append({
        'Location': nrc_name,
        'Parent': '纽约州',
        'Size': total_students,
        'Color': proficiency,
        'Type': 'NRC',
        'Description': f'{nrc_desc} (学生数: {total_students:,})'
    })
    nrc_dict[nrc_code] = nrc_name

for _, row in county_course.iterrows():
    county_code = str(row['COUNTY_CODE']).replace('.0', '') if pd.notna(row['COUNTY_CODE']) else 'Unknown'
    county_name = row['COUNTY_NAME'] if pd.notna(row['COUNTY_NAME']) else 'Unknown'
    nrc_code = str(row['NRC_CODE']).replace('.0', '') if pd.notna(row['NRC_CODE']) else 'Unknown'
    students = int(row['student_count'])
    
    proficiency = 0
    if county_code != 'Unknown':
        county_assess = county_assessment[county_assessment['COUNTY_CODE'].astype(str).str.replace('.0', '') == county_code]
        if len(county_assess) > 0:
            proficiency = float(county_assess.iloc[0]['proficiency_rate'])
    
    parent = nrc_dict.get(nrc_code, '纽约州')
    county_label = f"{county_name}县"
    treemap_data.append({
        'Location': county_label,
        'Parent': parent,
        'Size': students,
        'Color': proficiency,
        'Type': 'County',
        'Description': f'{county_name}县 (学生数: {students:,}, 达标率: {proficiency*100:.1f}%)'
    })

output_file = Path('cleaned_data/treemap_data.json')
output_file.parent.mkdir(parents=True, exist_ok=True)

with open(output_file, 'w', encoding='utf-8') as f:
    json.dump(treemap_data, f, ensure_ascii=False, indent=2)

print(f"\nTreeMap数据已保存到: {output_file}")
print(f"共 {len(treemap_data)} 个节点")

state_node = [d for d in treemap_data if d['Parent'] is None][0]
nrc_nodes = [d for d in treemap_data if d['Type'] == 'NRC']
county_nodes = [d for d in treemap_data if d['Type'] == 'County']

print(f"\n数据统计:")
print(f"  全州节点: 1")
print(f"  N/RC节点: {len(nrc_nodes)}")
print(f"  县级节点: {len(county_nodes)}")
print(f"  总学生数: {state_node['Size']:,}")


