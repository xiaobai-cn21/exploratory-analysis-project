import csv

# 读取第二个表（AP_IB_Assessment_2024.csv）
file_path = "AP_IB_Assessment_2024.csv"

print(f"正在读取文件: {file_path}")

# 定义需要检查的7个列的列名
level_columns = ['level1_cnt', 'level2_cnt', 'level3_cnt', 'level4_cnt', 
                 'level5_cnt', 'level6_cnt', 'level7_cnt']

# 读取CSV文件
rows = []
header = None
level_indices = []

with open(file_path, 'r', encoding='utf-8') as f:
    reader = csv.reader(f)
    header = next(reader)  # 读取表头
    
    # 找到7个level列的索引位置
    for col_name in level_columns:
        if col_name in header:
            level_indices.append(header.index(col_name))
        else:
            print(f"警告: 列 '{col_name}' 不存在")
    
    if len(level_indices) != 7:
        print(f"错误: 只找到 {len(level_indices)} 个level列，需要7个")
        print(f"可用的列: {header}")
    else:
        # 读取所有行
        original_count = 0
        for row in reader:
            original_count += 1
            # 检查这7个列中是否有任何一列是"-"
            has_dash = any(row[idx] == '-' for idx in level_indices if idx < len(row))
            # 只保留这7个列都不为"-"的行
            if not has_dash:
                rows.append(row)
        
        print(f"原始数据行数: {original_count}")
        print(f"删除后的数据行数: {len(rows)}")
        print(f"删除了 {original_count - len(rows)} 行数据")
        
        # 保存清理后的数据（保存为新文件，避免覆盖原文件）
        output_path = "AP_IB_Assessment_2024_cleaned.csv"
        with open(output_path, 'w', encoding='utf-8', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(header)  # 写入表头
            writer.writerows(rows)   # 写入数据行
        
        print(f"清理后的数据已保存到: {output_path}")

