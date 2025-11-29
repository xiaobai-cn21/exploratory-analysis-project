# 数据生成脚本

此文件夹包含用于生成可视化数据的 Python 脚本。

## 脚本说明

### 1. `build_county_coverage.py`
生成县级覆盖度指标数据，用于地理热力图可视化。

**输出文件**: `static/js/county_coverage.json`

**运行方式**:
```bash
python scripts/build_county_coverage.py
```

### 2. `build_resource_outcome_scatter.py`
生成资源与成果对比数据，用于散点图可视化。

**输出文件**: `static/js/resource_outcome_scatter.json`

**运行方式**:
```bash
python scripts/build_resource_outcome_scatter.py
```

### 3. `build_treemap_data.py`
生成多层级树形图数据，展示全州 → N/RC → 县的层级结构。

**输出文件**: `static/js/treemap_data.json`

**运行方式**:
```bash
python scripts/build_treemap_data.py
```

## 运行所有脚本

要生成所有可视化数据，可以依次运行上述三个脚本，或使用以下命令（在项目根目录）：

```bash
python scripts/build_county_coverage.py
python scripts/build_resource_outcome_scatter.py
python scripts/build_treemap_data.py
```

## 依赖要求

确保已安装以下 Python 包：
- pandas

安装依赖：
```bash
pip install pandas
```

## 注意事项

- 所有脚本会自动从 `static/data/csv/` 读取输入数据
- 所有脚本会将输出保存到 `static/js/` 目录
- 脚本会自动创建输出目录（如果不存在）

