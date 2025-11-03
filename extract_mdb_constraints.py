"""
提取MDB数据库的完整字段约束信息
不需要安装Microsoft Access，使用pypyodbc库
"""

import sys
import io
# 设置标准输出编码为UTF-8，避免Windows控制台中文乱码
sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')

import pypyodbc
import pandas as pd
from pathlib import Path
import json
from datetime import datetime

# 配置
MDB_FILES = {
    'course': r'E:\STUDY\0-BIT\5-Y3-1\6-DataWarehouse&Mining\project\APIB24\AP_IB_Course_2024.mdb',
    'assessment': r'E:\STUDY\0-BIT\5-Y3-1\6-DataWarehouse&Mining\project\APIB24\AP_IB_Assessment_2024.mdb'
}

# 定义需要完整枚举所有值的字段（基于PDF文档分析）
FIELDS_TO_ENUMERATE_COMPLETELY = [
    'AGGREGATION_INDEX',
    'AGGREGATION_TYPE', 
    'NRC_CODE',
    'NRC_DESC',
    'SUBGROUP_CODE',      # 最重要！人口统计分组
    'SUBGROUP_NAME',      # 最重要！分组名称
    'APIB_IND',
    'NYC_IND',
    'GRADE_LEVEL',
    'SUBJECT_AREA',
    'COUNTY_CODE',
    'COUNTY_NAME'
]

# 定义建议枚举但可能值较多的字段
FIELDS_TO_ENUMERATE_WITH_LIMIT = [
    'COURSE_ID',
    'COURSE_DESC',
    'STATE_CODE',
    'ITEM_DESC'
]

# 定义只需要统计信息的字段（不枚举值）
FIELDS_STATS_ONLY = [
    'AGGREGATION_CODE',
    'AGGREGATION_NAME',
    'LEA_BEDS',
    'LEA_NAME',
    'INST_ID',
    'STUDENT_COUNT',
    'TESTED_STUDENT_CNT',
    'PROFICIENT_STUDENT_CNT',
    'LEVEL1_CNT',
    'LEVEL2_CNT',
    'LEVEL3_CNT',
    'LEVEL4_CNT',
    'LEVEL5_CNT',
    'LEVEL6_CNT',
    'LEVEL7_CNT'
]


def connect_to_mdb(mdb_path):
    """连接到MDB数据库"""
    conn_str = f'Driver={{Microsoft Access Driver (*.mdb, *.accdb)}};DBQ={mdb_path};'
    try:
        conn = pypyodbc.connect(conn_str)
        return conn
    except Exception as e:
        print(f"[错误] 连接失败: {e}")
        print("\n[解决方案]")
        print("1. 安装 pypyodbc: pip install pypyodbc")
        print("2. 如果仍然失败，需要安装 Microsoft Access Database Engine 2016 Redistributable")
        print("   下载地址: https://www.microsoft.com/en-us/download/details.aspx?id=54920")
        return None


def get_table_schema(conn, table_name):
    """获取表结构信息"""
    cursor = conn.cursor()
    
    # 获取字段信息
    # pypyodbc的columns()返回元组，各个位置的含义：
    # 0-table_cat, 1-table_schem, 2-table_name, 3-column_name,
    # 4-data_type, 5-type_name, 6-column_size, 7-buffer_length,
    # 8-decimal_digits, 9-num_prec_radix, 10-nullable, 11-remarks,
    # 12-column_def, 13-sql_data_type, 14-sql_datetime_sub,
    # 15-char_octet_length, 16-ordinal_position, 17-is_nullable
    columns_info = []
    for row in cursor.columns(table=table_name):
        column_info = {
            'column_name': row[3],           # column_name
            'data_type': row[5],             # type_name
            'column_size': row[6],           # column_size
            'nullable': row[10],             # nullable (0=不允许, 1=允许)
            'ordinal_position': row[16],     # ordinal_position
            'default': row[12],              # column_def（默认值，可能为None）
            'remarks': row[11],              # remarks（备注/说明，可能为None）
        }
        columns_info.append(column_info)
    
    return columns_info


def get_table_constraints(conn, table_name):
    """获取主键、外键、索引/唯一性约束信息"""
    cursor = conn.cursor()

    # 主键
    primary_keys = []
    try:
        for row in cursor.primaryKeys(table=table_name):
            # row: table_cat, table_schem, table_name, column_name, key_seq, pk_name
            primary_keys.append({
                'column_name': row[3],
                'key_seq': row[4],
                'pk_name': row[5]
            })
    except Exception as _:
        pass

    # 外键
    foreign_keys = []
    try:
        for row in cursor.foreignKeys(table=table_name):
            # row: pktable_cat, pktable_schem, pktable_name, pkcolumn_name,
            #      fktable_cat, fktable_schem, fktable_name, fkcolumn_name,
            #      key_seq, update_rule, delete_rule, fk_name, pk_name, deferrability
            foreign_keys.append({
                'pk_table': row[2],
                'pk_column': row[3],
                'fk_table': row[6],
                'fk_column': row[7],
                'key_seq': row[8],
                'fk_name': row[11],
                'pk_name': row[12]
            })
    except Exception as _:
        pass

    # 索引（含唯一性）
    indexes = []
    try:
        for row in cursor.statistics(table=table_name, unique=False):
            # row: table_cat, table_schem, table_name, non_unique, index_qualifier,
            #      index_name, type, ordinal_position, column_name, asc_or_desc,
            #      cardinality, pages, filter_condition
            index_name = row[5]
            if index_name is None:
                continue
            indexes.append({
                'index_name': index_name,
                'non_unique': bool(row[3]),
                'type': row[6],
                'ordinal_position': row[7],
                'column_name': row[8],
                'asc_or_desc': row[9]
            })
    except Exception as _:
        pass

    return {
        'primary_keys': primary_keys,
        'foreign_keys': foreign_keys,
        'indexes': indexes
    }


def perform_rule_checks(conn, table_name, is_assessment=False):
    """业务规则校验：
    - 对评估表：
      1) tested_student_cnt == sum(level1..levelN)
      2) AP 达标: proficient == level3+4+5
      3) IB 达标: proficient == level4+5+6+7
    返回不一致记录计数与比例。
    """
    cursor = conn.cursor()

    results = {}
    if not is_assessment:
        return results

    # 将文本'-'与NULL转0，使用VAL函数将文本转数字
    lvl = lambda c: f"IIF([{c}]='-', 0, VAL([{c}]))"

    try:
        # 1) levels求和一致性
        sum_levels = f"{lvl('level1_cnt')}+{lvl('level2_cnt')}+{lvl('level3_cnt')}+{lvl('level4_cnt')}+{lvl('level5_cnt')}+{lvl('level6_cnt')}+{lvl('level7_cnt')}"
        q_total = f"SELECT COUNT(*) FROM [{table_name}]"
        cursor.execute(q_total)
        total_rows = cursor.fetchone()[0]

        q_mismatch_levels = f"""
            SELECT COUNT(*)
            FROM [{table_name}]
            WHERE VAL([{ 'tested_student_cnt' }]) <> ({sum_levels})
        """
        cursor.execute(q_mismatch_levels)
        levels_mismatch = cursor.fetchone()[0]

        results['levels_sum_check'] = {
            'total_rows': total_rows,
            'mismatch_rows': levels_mismatch,
            'mismatch_percentage': round(levels_mismatch / total_rows * 100, 4) if total_rows else 0.0
        }
    except Exception as e:
        results['levels_sum_check_error'] = str(e)

    try:
        # 2) AP 达标（APIB_IND='AP'）：proficient == level3+4+5
        ap_sum = f"{lvl('level3_cnt')}+{lvl('level4_cnt')}+{lvl('level5_cnt')}"
        q_ap_total = f"SELECT COUNT(*) FROM [{table_name}] WHERE [APIB_IND]='AP'"
        cursor.execute(q_ap_total)
        ap_total = cursor.fetchone()[0]
        q_ap_mismatch = f"""
            SELECT COUNT(*)
            FROM [{table_name}]
            WHERE [APIB_IND]='AP'
              AND VAL([{ 'proficient_student_cnt' }]) <> ({ap_sum})
        """
        cursor.execute(q_ap_mismatch)
        ap_mismatch = cursor.fetchone()[0]
        results['ap_proficient_check'] = {
            'total_rows': ap_total,
            'mismatch_rows': ap_mismatch,
            'mismatch_percentage': round(ap_mismatch / ap_total * 100, 4) if ap_total else 0.0
        }
    except Exception as e:
        results['ap_proficient_check_error'] = str(e)

    try:
        # 3) IB 达标（APIB_IND='IB'）：proficient == level4+5+6+7
        ib_sum = f"{lvl('level4_cnt')}+{lvl('level5_cnt')}+{lvl('level6_cnt')}+{lvl('level7_cnt')}"
        q_ib_total = f"SELECT COUNT(*) FROM [{table_name}] WHERE [APIB_IND]='IB'"
        cursor.execute(q_ib_total)
        ib_total = cursor.fetchone()[0]
        q_ib_mismatch = f"""
            SELECT COUNT(*)
            FROM [{table_name}]
            WHERE [APIB_IND]='IB'
              AND VAL([{ 'proficient_student_cnt' }]) <> ({ib_sum})
        """
        cursor.execute(q_ib_mismatch)
        ib_mismatch = cursor.fetchone()[0]
        results['ib_proficient_check'] = {
            'total_rows': ib_total,
            'mismatch_rows': ib_mismatch,
            'mismatch_percentage': round(ib_mismatch / ib_total * 100, 4) if ib_total else 0.0
        }
    except Exception as e:
        results['ib_proficient_check_error'] = str(e)

    return results


def analyze_field_values(conn, table_name, field_name):
    """分析字段的值分布"""
    cursor = conn.cursor()
    
    try:
        # 基本统计
        query_total = f"SELECT COUNT(*) as total FROM [{table_name}]"
        cursor.execute(query_total)
        total_rows = cursor.fetchone()[0]
        
        # 非空值统计
        query_non_null = f"SELECT COUNT([{field_name}]) as non_null FROM [{table_name}]"
        cursor.execute(query_non_null)
        non_null_count = cursor.fetchone()[0]
        
        # 获取所有唯一值及其出现次数（同时得到distinct_count）
        # Access不支持COUNT(DISTINCT)，但GROUP BY可以同时获取唯一值和数量
        query_values = f"""
            SELECT [{field_name}], COUNT(*) as count 
            FROM [{table_name}] 
            GROUP BY [{field_name}]
            ORDER BY COUNT(*) DESC
        """
        cursor.execute(query_values)
        values_distribution = cursor.fetchall()
        
        # 从GROUP BY结果中获取唯一值数量
        distinct_count = len(values_distribution)
        
        return {
            'total_rows': total_rows,
            'non_null_count': non_null_count,
            'null_count': total_rows - non_null_count,
            'distinct_count': distinct_count,
            'values_distribution': [(str(row[0]) if row[0] is not None else 'NULL', row[1]) for row in values_distribution]
        }
    except Exception as e:
        return {'error': str(e)}


def extract_all_constraints(mdb_path, db_name):
    """提取数据库的完整约束信息"""
    print(f"\n{'='*80}")
    print(f"[分析] 正在分析: {db_name}")
    print(f"{'='*80}")
    
    conn = connect_to_mdb(mdb_path)
    if not conn:
        return None
    
    cursor = conn.cursor()
    
    # 获取所有表名
    tables = []
    for table_info in cursor.tables(tableType='TABLE'):
        # pypyodbc返回的是元组，表名在第3个位置（索引2）
        table_name = table_info[2]
        if not table_name.startswith('MSys'):  # 排除系统表
            tables.append(table_name)
    print(f"\n[完成] 找到 {len(tables)} 个表: {tables}")
    
    database_info = {
        'database_name': db_name,
        'mdb_path': mdb_path,
        'tables': {}
    }
    
    for table_name in tables:
        print(f"\n📋 分析表: {table_name}")
        
        # 获取表结构
        schema = get_table_schema(conn, table_name)
        print(f"  ├─ 字段数量: {len(schema)}")
        
        # 获取记录数
        cursor.execute(f"SELECT COUNT(*) FROM [{table_name}]")
        row_count = cursor.fetchone()[0]
        print(f"  ├─ 记录数量: {row_count:,}")
        
        # 采集表级约束
        constraints = get_table_constraints(conn, table_name)

        table_info = {
            'row_count': row_count,
            'column_count': len(schema),
            'schema': schema,
            'field_analysis': {},
            'constraints': constraints
        }
        
        # 分析每个字段
        for col_info in schema:
            field_name = col_info['column_name']
            print(f"  ├─ 分析字段: {field_name}", end='')
            
            field_stats = analyze_field_values(conn, table_name, field_name)
            
            # 判断是否需要完整枚举
            enumerate_all = field_name in FIELDS_TO_ENUMERATE_COMPLETELY
            enumerate_limited = field_name in FIELDS_TO_ENUMERATE_WITH_LIMIT
            
            if 'error' not in field_stats:
                distinct_count = field_stats['distinct_count']
                print(f" - {distinct_count} 个唯一值", end='')
                
                field_analysis = {
                    'data_type': col_info['data_type'],
                    'column_size': col_info['column_size'],
                    'nullable': col_info['nullable'],
                    'total_rows': field_stats['total_rows'],
                    'non_null_count': field_stats['non_null_count'],
                    'null_count': field_stats['null_count'],
                    'null_percentage': round(field_stats['null_count'] / field_stats['total_rows'] * 100, 2),
                    'distinct_count': distinct_count,
                    'distinct_percentage': round(distinct_count / field_stats['non_null_count'] * 100, 2) if field_stats['non_null_count'] > 0 else 0,
                }
                
                # 根据字段类型决定是否枚举值
                if enumerate_all:
                    # 完整枚举所有值
                    field_analysis['all_values'] = field_stats['values_distribution']
                    print(f" [完整枚举]")
                elif enumerate_limited and distinct_count <= 500:
                    # 有限枚举（最多500个）
                    field_analysis['all_values'] = field_stats['values_distribution']
                    print(f" [有限枚举]")
                elif distinct_count <= 20:
                    # 值较少，自动枚举
                    field_analysis['all_values'] = field_stats['values_distribution']
                    print(f" [自动枚举-值少]")
                else:
                    # 只保留前50个最常见的值
                    field_analysis['top_50_values'] = field_stats['values_distribution'][:50]
                    print(f" [仅统计-TOP50]")
                
                table_info['field_analysis'][field_name] = field_analysis
            else:
                print(f" [错误]: {field_stats['error']}")
        
        # 业务规则校验（仅评估表）
        rule_checks = perform_rule_checks(conn, table_name, is_assessment=(db_name == 'assessment'))
        if rule_checks:
            table_info['rule_checks'] = rule_checks

        database_info['tables'][table_name] = table_info
    
    conn.close()
    return database_info


def save_results(database_info, output_dir='analysis_results'):
    """保存分析结果"""
    output_path = Path(output_dir)
    output_path.mkdir(exist_ok=True)
    
    db_name = database_info['database_name']
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # 1. 保存完整的JSON格式
    json_file = output_path / f'{db_name}_完整约束信息_{timestamp}.json'
    with open(json_file, 'w', encoding='utf-8') as f:
        json.dump(database_info, f, ensure_ascii=False, indent=2)
    print(f"\n[完成] JSON文件已保存: {json_file}")
    
    # 2. 保存人类可读的Markdown格式
    md_file = output_path / f'{db_name}_字段约束报告_{timestamp}.md'
    with open(md_file, 'w', encoding='utf-8') as f:
        f.write(f"# {db_name} 数据库字段约束分析报告\n\n")
        f.write(f"**数据库路径**: {database_info['mdb_path']}\n\n")
        
        for table_name, table_info in database_info['tables'].items():
            f.write(f"\n## 表: {table_name}\n\n")
            f.write(f"- **记录数**: {table_info['row_count']:,}\n")
            f.write(f"- **字段数**: {table_info['column_count']}\n\n")

            # 表级约束
            if 'constraints' in table_info:
                cons = table_info['constraints']
                f.write("### 表级约束\n\n")
                # 主键
                if cons.get('primary_keys'):
                    pk_cols = ", ".join([c['column_name'] for c in cons['primary_keys']])
                    f.write(f"- 主键: {pk_cols}\n")
                else:
                    f.write("- 主键: (无/未检测到)\n")
                # 外键
                if cons.get('foreign_keys'):
                    f.write("- 外键:\n")
                    for fk in cons['foreign_keys'][:20]:
                        f.write(f"  - {fk['fk_column']} -> {fk['pk_table']}.{fk['pk_column']} (FK: {fk.get('fk_name')})\n")
                    if len(cons['foreign_keys']) > 20:
                        f.write(f"  - ... 共 {len(cons['foreign_keys'])} 条\n")
                else:
                    f.write("- 外键: (无/未检测到)\n")
                # 索引
                if cons.get('indexes'):
                    uniq = [i for i in cons['indexes'] if not i['non_unique']]
                    nonuniq = [i for i in cons['indexes'] if i['non_unique']]
                    f.write(f"- 唯一索引: {len(uniq)} 个，普通索引: {len(nonuniq)} 个\n\n")

            f.write("### 字段详细信息\n\n")
            
            for field_name, field_analysis in table_info['field_analysis'].items():
                f.write(f"\n#### 字段: `{field_name}`\n\n")
                f.write(f"**基本信息**:\n")
                f.write(f"- 数据类型: `{field_analysis['data_type']}`\n")
                f.write(f"- 字段大小: {field_analysis['column_size']}\n")
                f.write(f"- 允许空值: {'是' if field_analysis['nullable'] else '否'}\n\n")
                # 默认值与备注
                for col in table_info['schema']:
                    if col['column_name'] == field_name:
                        if col.get('default') is not None:
                            f.write(f"- 默认值: {col['default']}\n")
                        if col.get('remarks'):
                            f.write(f"- 备注: {col['remarks']}\n")
                        break
                
                f.write(f"**统计信息**:\n")
                f.write(f"- 总记录数: {field_analysis['total_rows']:,}\n")
                f.write(f"- 非空记录数: {field_analysis['non_null_count']:,}\n")
                f.write(f"- 空值数量: {field_analysis['null_count']:,} ({field_analysis['null_percentage']}%)\n")
                # 唯一值数量的百分比说明：表示值的多样性（多样性 = 唯一值数/非空记录数）
                # 0.01%表示值非常集中（几乎都是重复值），100%表示每个值都不同
                distinct_pct_explanation = f" ({field_analysis['distinct_percentage']}% - 值多样性指标，非覆盖率)"
                f.write(f"- 唯一值数量: {field_analysis['distinct_count']:,}{distinct_pct_explanation}\n\n")
                
                # 枚举值
                if 'all_values' in field_analysis:
                    f.write(f"**所有可能的值** (共 {len(field_analysis['all_values'])} 个):\n\n")
                    f.write("| 值 | 出现次数 | 占总记录百分比 |\n")
                    f.write("|---|---|---|\n")
                    for value, count in field_analysis['all_values']:
                        # 统一使用总记录数作为分母，避免NULL值导致百分比超过100%
                        percentage = count / field_analysis['total_rows'] * 100 if field_analysis['total_rows'] > 0 else 0
                        f.write(f"| {value} | {count:,} | {percentage:.2f}% |\n")
                elif 'top_50_values' in field_analysis:
                    f.write(f"**前50个最常见的值**:\n\n")
                    f.write("| 值 | 出现次数 | 占总记录百分比 |\n")
                    f.write("|---|---|---|\n")
                    for value, count in field_analysis['top_50_values']:
                        # 统一使用总记录数作为分母
                        percentage = count / field_analysis['total_rows'] * 100 if field_analysis['total_rows'] > 0 else 0
                        f.write(f"| {value} | {count:,} | {percentage:.2f}% |\n")
                
                f.write("\n---\n")

            # 规则校验
            if 'rule_checks' in table_info and table_info['rule_checks']:
                f.write("\n### 规则校验\n\n")
                rc = table_info['rule_checks']
                if rc.get('levels_sum_check'):
                    s = rc['levels_sum_check']
                    f.write(f"- Levels求和一致性: 异常 {s['mismatch_rows']:,} / {s['total_rows']:,} ({s['mismatch_percentage']}%)\n")
                if rc.get('ap_proficient_check'):
                    s = rc['ap_proficient_check']
                    f.write(f"- AP达标一致性: 异常 {s['mismatch_rows']:,} / {s['total_rows']:,} ({s['mismatch_percentage']}%)\n")
                if rc.get('ib_proficient_check'):
                    s = rc['ib_proficient_check']
                    f.write(f"- IB达标一致性: 异常 {s['mismatch_rows']:,} / {s['total_rows']:,} ({s['mismatch_percentage']}%)\n")
    
    print(f"[完成] Markdown报告已保存: {md_file}")
    
    # 3. 单独保存关键字段的完整枚举值（CSV格式）
    for table_name, table_info in database_info['tables'].items():
        for field_name in FIELDS_TO_ENUMERATE_COMPLETELY:
            if field_name in table_info['field_analysis']:
                field_data = table_info['field_analysis'][field_name]
                if 'all_values' in field_data:
                    csv_file = output_path / f'{db_name}_{table_name}_{field_name}_完整枚举值.csv'
                    df = pd.DataFrame(field_data['all_values'], columns=['值', '出现次数'])
                    # 使用总记录数作为分母计算百分比，避免NULL值导致问题
                    total_rows = field_data['total_rows']
                    df['占总记录百分比'] = (df['出现次数'] / total_rows * 100).round(2)
                    df.to_csv(csv_file, index=False, encoding='utf-8-sig')
                    print(f"[完成] 字段枚举CSV已保存: {csv_file}")


def main():
    """主函数"""
    print("="*80)
    print("MDB数据库字段约束提取工具")
    print("="*80)
    print("\n[配置信息]")
    print(f"   - 需要完整枚举的字段: {len(FIELDS_TO_ENUMERATE_COMPLETELY)} 个")
    print(f"   - 关键字段: {', '.join(FIELDS_TO_ENUMERATE_COMPLETELY[:6])}...")
    
    all_results = {}
    
    for db_name, mdb_path in MDB_FILES.items():
        if not Path(mdb_path).exists():
            print(f"\n[错误] 文件不存在: {mdb_path}")
            continue
        
        database_info = extract_all_constraints(mdb_path, db_name)
        if database_info:
            all_results[db_name] = database_info
            save_results(database_info)
    
    print("\n" + "="*80)
    print("[完成] 所有分析完成！")
    print("="*80)
    print("\n[输出] 文件位于: analysis_results/ 目录")
    print("\n[文件类型]")
    print("   1. JSON格式 - 完整的结构化数据")
    print("   2. Markdown格式 - 人类可读的分析报告")
    print("   3. CSV格式 - 关键字段的完整枚举值")
    print("\n[重点] 特别关注 SUBGROUP_CODE 和 SUBGROUP_NAME 的完整枚举值！")


if __name__ == '__main__':
    main()

