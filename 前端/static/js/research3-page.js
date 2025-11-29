/**
 * 问题三：成绩预测与影响因素分析 - 前端图表渲染
 * 
 * 参考 research-equity.js 的结构和样式
 */

import { createMidnightLayout } from "./theme.js";

const DATA_URL = "/static/js/q3/chart_specs.json";

const palette = {
    ap: "#58a6ff",
    ib: "#ffa657",
    accent: "#42dd8a",
    warning: "#f08c67",
    neutral: "#8da0c5"
};

const statFields = {
    validRecords: document.querySelector('[data-field="valid-records"]'),
    apAvgProficiency: document.querySelector('[data-field="ap-avg-proficiency"]'),
    ibAvgProficiency: document.querySelector('[data-field="ib-avg-proficiency"]'),
    suppressionRate: document.querySelector('[data-field="suppression-rate"]')
};

const formatNumber = (value) => (typeof value === "number" ? value.toLocaleString("en-US") : "—");
const formatPercent = (value, digits = 1) =>
    typeof value === "number" ? `${(value * 100).toFixed(digits)}%` : "—";

const handlePlotlyMissing = (containerId) => {
    const container = document.getElementById(containerId);
    if (container) {
        container.innerHTML = "<p class=\"plot-note\">Plotly 未加载，暂无法渲染。</p>";
    }
};

const setStatValue = (node, value) => {
    if (!node) return;
    node.textContent = value;
};

// 渲染概览统计
const renderStats = (data) => {
    // 从数据中提取统计信息
    if (data.target_variable_summary && data.target_variable_summary.length > 0) {
        const ap_data = data.target_variable_summary.find(d => d.APIB_IND === "AP");
        const ib_data = data.target_variable_summary.find(d => d.APIB_IND === "IB");
        
        if (ap_data) {
            setStatValue(statFields.apAvgProficiency, formatPercent(ap_data.mean));
        }
        if (ib_data) {
            setStatValue(statFields.ibAvgProficiency, formatPercent(ib_data.mean));
        }
        
        // 计算总记录数
        const total_count = data.target_variable_summary.reduce((sum, d) => sum + (d.count || 0), 0);
        setStatValue(statFields.validRecords, formatNumber(total_count));
    }
    
    // 从抑制率数据计算总体抑制率
    if (data.suppression_sorted && data.suppression_sorted.length > 0) {
        const total_suppressed = data.suppression_sorted.reduce((sum, d) => sum + (d.suppression_rate || 0) * (d.total_records || 0), 0);
        const total_records = data.suppression_sorted.reduce((sum, d) => sum + (d.total_records || 0), 0);
        const avg_suppression = total_records > 0 ? total_suppressed / total_records : 0;
        setStatValue(statFields.suppressionRate, formatPercent(avg_suppression));
    }
};

// 图表1：达标率分布（密度图）
const renderProficiencyDistribution = (data) => {
    if (!window.Plotly) {
        console.error("Plotly未加载");
        handlePlotlyMissing("chart-proficiency-distribution");
        return;
    }

    const dist_data = data.proficiency_distribution || [];
    if (dist_data.length === 0) {
        console.warn("图表1：数据为空", data.proficiency_distribution);
        handlePlotlyMissing("chart-proficiency-distribution");
        return;
    }

    const traces = dist_data.map(item => {
        const rates = item.proficiency_rates || [];
        return {
            x: rates.map(r => r * 100), // 转换为百分比
            type: "histogram",
            histnorm: "probability density",
            name: item.APIB_IND,
            opacity: 0.6,
            marker: {
                color: item.APIB_IND === "AP" ? palette.ap : palette.ib
            },
            hovertemplate: `${item.APIB_IND}<br>达标率：%{x:.1f}%<br>概率密度：%{y:.3f}<br><extra></extra>`,
            nbinsx: 50 // 增加bins数量使分布更平滑
        };
    });

    const layout = createMidnightLayout({
        title: "达标率分布（密度图）",
        xaxis: { 
            title: "达标率 (%)",
            gridcolor: "rgba(255, 255, 255, 0.1)"
        },
        yaxis: { 
            title: "概率密度（Probability Density）",
            gridcolor: "rgba(255, 255, 255, 0.1)"
        },
        barmode: "overlay",
        legend: { orientation: "h", y: -0.25 },
        annotations: [{
            text: "密度图展示了达标率的概率分布。Y轴表示概率密度，即在该达标率值附近的相对频率。",
            xref: "paper",
            yref: "paper",
            x: 0.5,
            y: -0.35,
            showarrow: false,
            font: { size: 11, color: "rgba(255, 255, 255, 0.7)" }
        }]
    });

    Plotly.newPlot("chart-proficiency-distribution", traces, layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 图表2：抑制率排序（水平条形图）
const renderSuppressionSorted = (data) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-suppression-sorted");
        return;
    }

    const suppression_data = data.suppression_sorted || [];
    if (suppression_data.length === 0) {
        handlePlotlyMissing("chart-suppression-sorted");
        return;
    }

    // 按抑制率排序（从高到低），取Top 30
    const sorted = [...suppression_data]
        .sort((a, b) => (b.suppression_rate || 0) - (a.suppression_rate || 0))
        .slice(0, 30);

    // 获取唯一的子组名称和AP/IB标识
    const uniqueSubgroups = [...new Set(sorted.map(d => d.subgroup_name))];
    const apibTypes = ["AP", "IB"];

    // 构建矩阵数据
    const z = [];
    const text = [];
    const hovertext = [];

    uniqueSubgroups.forEach(subgroup => {
        const row = [];
        const textRow = [];
        const hoverRow = [];
        apibTypes.forEach(apib => {
            const item = sorted.find(d => d.subgroup_name === subgroup && d.apib_ind === apib);
            const rate = item ? (item.suppression_rate || 0) * 100 : 0;
            const total = item ? (item.total_records || 0) : 0;
            row.push(rate);
            textRow.push(rate > 0 ? `${rate.toFixed(1)}%` : "");
            hoverRow.push(`${subgroup}<br>${apib}<br>抑制率：${rate.toFixed(1)}%<br>总记录数：${total}`);
        });
        z.push(row);
        text.push(textRow);
        hovertext.push(hoverRow);
    });

    const trace = {
        type: "heatmap",
        x: apibTypes,
        y: uniqueSubgroups,
        z: z,
        text: text,
        texttemplate: "%{text}",
        textfont: { size: 10, color: "#ffffff" },
        hovertemplate: "%{customdata}<extra></extra>",
        customdata: hovertext,
        colorscale: [
            [0, "#42dd8a"],    // 绿色（低抑制率）
            [0.2, "#ffd700"],  // 黄色
            [0.5, "#ffa500"],  // 橙色
            [0.8, "#ff6b6b"],  // 红色（高抑制率）
            [1, "#cc0000"]     // 深红色
        ],
        showscale: true,
        colorbar: {
            title: "抑制率 (%)",
            titleside: "right"
        }
    };

    const layout = createMidnightLayout({
        title: "数据抑制率热力图（Top 30子组 × AP/IB）",
        xaxis: { 
            title: "AP/IB类型",
            tickmode: "array",
            tickvals: apibTypes,
            ticktext: apibTypes
        },
        yaxis: { 
            title: "子组", 
            autorange: "reversed",
            tickmode: "array",
            tickvals: uniqueSubgroups,
            ticktext: uniqueSubgroups
        },
        margin: { t: 60, r: 100, b: 150, l: 200 }
    });

    Plotly.newPlot("chart-suppression-sorted", [trace], layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 图表3：人口统计分组对比（带类别选择器）
let currentDemographicCategory = "经济状况";
let demographicDataCache = null;

const renderDemographicComparison = (data, selectedCategory = null) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-demographic-comparison");
        return;
    }

    const demo_data = data.demographic_comparison || [];
    if (demo_data.length === 0) {
        handlePlotlyMissing("chart-demographic-comparison");
        return;
    }

    // 缓存数据
    demographicDataCache = demo_data;

    // 使用选择的类别或默认类别
    const category = selectedCategory || currentDemographicCategory;

    // 过滤掉All Students，按选择的类别过滤
    const filtered = demo_data.filter(
        d => d.SUBGROUP_NAME !== "All Students" && d.category === category
    );
    
    const traces = [];
    
    // 为AP和IB分别创建条形，只显示差值（基准在中间）
    ["AP", "IB"].forEach(apib => {
        const subset = filtered
            .filter(d => d.APIB_IND === apib)
            .sort((a, b) => (b.gap_vs_all || 0) - (a.gap_vs_all || 0)); // 按差值排序
        
        if (subset.length > 0) {
            // 从数据中获取All Students的达标率（每个子组都有all_proficiency_rate字段）
            const sample = subset[0];
            let base_rate = 0;
            if (sample.all_proficiency_rate !== undefined && sample.all_proficiency_rate !== null && !isNaN(sample.all_proficiency_rate)) {
                base_rate = (sample.all_proficiency_rate || 0) * 100;
            } else if (sample.proficiency_rate !== undefined && sample.gap_vs_all !== undefined) {
                // 如果没有all_proficiency_rate，从gap_vs_all反推
                base_rate = ((sample.proficiency_rate || 0) - (sample.gap_vs_all || 0)) * 100;
            }
            
            // 只显示差值条形（正值在一边，负值在另一边）
            const gaps = subset.map(d => {
                const gap = d.gap_vs_all;
                if (gap === undefined || gap === null || isNaN(gap)) {
                    // 如果没有gap_vs_all，从proficiency_rate和all_proficiency_rate计算
                    if (d.proficiency_rate !== undefined && d.all_proficiency_rate !== undefined) {
                        return ((d.proficiency_rate || 0) - (d.all_proficiency_rate || 0)) * 100;
                    }
                    return 0;
                }
                return gap * 100;
            });
            const proficiency_rates = subset.map(d => (d.proficiency_rate || 0) * 100);
            
            // 分离正差值和负差值，但使用相同的图例名称
            const positive_indices = [];
            const negative_indices = [];
            
            gaps.forEach((gap, i) => {
                if (gap >= 0) {
                    positive_indices.push(i);
                } else {
                    negative_indices.push(i);
                }
            });
            
            const base_color = apib === "AP" ? palette.ap : palette.ib;
            
            // 正差值trace（绿色）
            if (positive_indices.length > 0) {
                const pos_subset = positive_indices.map(i => subset[i]);
                const pos_gaps = positive_indices.map(i => gaps[i]);
                const pos_rates = positive_indices.map(i => proficiency_rates[i]);
                
                traces.push({
                    type: "bar",
                    x: pos_subset.map(d => d.SUBGROUP_NAME),
                    y: pos_gaps,
                    name: `${apib}`,
                    marker: { 
                        color: "#42dd8a",  // 绿色表示正值
                        opacity: 0.8
                    },
                    legendgroup: apib,
                    showlegend: positive_indices.length > 0,  // 只在有正值时显示图例
                    hovertemplate: `${apib}<br>%{x}<br>达标率：%{customdata[0]:.1f}%<br>与All Students差值：%{y:.1f}pp<br>基准：%{customdata[1]:.1f}%<extra></extra>`,
                    customdata: pos_rates.map(rate => [rate, base_rate])
                });
            }
            
            // 负差值trace（红色，不显示图例，与正差值共享）
            if (negative_indices.length > 0) {
                const neg_subset = negative_indices.map(i => subset[i]);
                const neg_gaps = negative_indices.map(i => gaps[i]);
                const neg_rates = negative_indices.map(i => proficiency_rates[i]);
                
                traces.push({
                    type: "bar",
                    x: neg_subset.map(d => d.SUBGROUP_NAME),
                    y: neg_gaps,
                    name: `${apib} (负)`,
                    marker: { 
                        color: "#ff6b6b",  // 红色表示负值
                        opacity: 0.8
                    },
                    legendgroup: apib,
                    showlegend: false,  // 不显示图例（与正差值共享）
                    hovertemplate: `${apib}<br>%{x}<br>达标率：%{customdata[0]:.1f}%<br>与All Students差值：%{y:.1f}pp<br>基准：%{customdata[1]:.1f}%<extra></extra>`,
                    customdata: neg_rates.map(rate => [rate, base_rate])
                });
            }
            
            // 添加一个隐藏的trace用于图例显示正确的AP/IB颜色
            // 只在第一次遇到该apib时添加
            const has_legend = traces.some(t => t.legendgroup === apib && t.showlegend);
            if (!has_legend && (positive_indices.length === 0 || negative_indices.length === 0)) {
                // 如果只有正值或只有负值，添加一个图例项
                traces.push({
                    type: "scatter",
                    mode: "markers",
                    x: [null],
                    y: [null],
                    name: `${apib}`,
                    marker: {
                        color: base_color,  // AP蓝色，IB橙色
                        size: 10,
                        opacity: 0.8
                    },
                    showlegend: true,
                    hoverinfo: "skip",
                    legendgroup: apib
                });
            }
        }
    });

    // 获取AP和IB的All Students达标率用于显示
    const ap_subset = filtered.filter(d => d.APIB_IND === "AP");
    const ib_subset = filtered.filter(d => d.APIB_IND === "IB");
    let ap_base_rate = 0, ib_base_rate = 0;
    
    if (ap_subset.length > 0 && ap_subset[0].all_proficiency_rate !== undefined && ap_subset[0].all_proficiency_rate !== null) {
        ap_base_rate = (ap_subset[0].all_proficiency_rate || 0) * 100;
    }
    if (ib_subset.length > 0 && ib_subset[0].all_proficiency_rate !== undefined && ib_subset[0].all_proficiency_rate !== null) {
        ib_base_rate = (ib_subset[0].all_proficiency_rate || 0) * 100;
    }
    
    // 添加All Students基准线标注（在Y=0处，即中间），显示达标率
    const annotations = [];
    let annotationText = "All Students基准线 (0pp)<br>";
    if (ap_base_rate > 0) {
        annotationText += `AP: ${ap_base_rate.toFixed(1)}%`;
    }
    if (ib_base_rate > 0) {
        if (ap_base_rate > 0) annotationText += " | ";
        annotationText += `IB: ${ib_base_rate.toFixed(1)}%`;
    }
    annotations.push({
        xref: "paper",
        yref: "y",
        x: 0.5,
        y: 0,
        text: annotationText,
        showarrow: true,
        arrowhead: 2,
        arrowcolor: "rgba(255,255,255,0.6)",
        ax: 0,
        ay: -30,
        font: { color: "rgba(255,255,255,0.8)", size: 11 },
        bgcolor: "rgba(0, 0, 0, 0.5)",
        bordercolor: "rgba(255, 255, 255, 0.3)",
        borderwidth: 1
    });

    // 计算Y轴范围（基于差值的最大值）
    const all_gaps = filtered.map(d => (d.gap_vs_all || 0) * 100);
    const max_gap = Math.max(...all_gaps.map(Math.abs), 10);
    const y_range = [-max_gap * 1.1, max_gap * 1.1];

    // 获取所有唯一的子组名称（用于x轴标签）
    const unique_subgroups = [...new Set(filtered.map(d => d.SUBGROUP_NAME))];
    
    // 处理子组名称：最多只显示两行
    const formatSubgroupNameForAxis = (name) => {
        // 如果名称包含空格，尝试分成两行
        if (name.includes(" ")) {
            const words = name.split(" ");
            if (words.length === 2) {
                // 两个单词，直接分成两行
                return words.join("<br>");
            } else if (words.length > 2) {
                // 多个单词，找到中间位置分成两行（最多两行）
                const mid = Math.ceil(words.length / 2);
                const firstLine = words.slice(0, mid).join(" ");
                const secondLine = words.slice(mid).join(" ");
                return firstLine + "<br>" + secondLine;
            }
        }
        // 如果单个单词超过15个字符，在中间换行（最多两行）
        if (name.length > 15) {
            const mid = Math.floor(name.length / 2);
            return name.substring(0, mid) + "<br>" + name.substring(mid);
        }
        return name;
    };

    const layout = createMidnightLayout({
        title: `人口统计分组达标率对比 - ${category}`,
        xaxis: { 
            title: "子组",
            tickangle: 0,  // 水平放置
            tickmode: "array",
            tickvals: unique_subgroups,
            ticktext: unique_subgroups.map(formatSubgroupNameForAxis),
            type: "category",  // 使用分类轴确保正确显示
            tickfont: { size: 10 },  // 减小字体，确保两行能显示
            automargin: true  // 自动调整边距
        },
        yaxis: { 
            title: "与All Students差值 (pp)",
            range: y_range,
            zeroline: true,
            zerolinecolor: "rgba(255, 255, 255, 0.8)",  // 增强基准线可见性
            zerolinewidth: 3,  // 增加基准线宽度
            gridcolor: "rgba(255, 255, 255, 0.2)",  // 增强网格线可见性
            gridwidth: 1.5,  // 增加网格线宽度
            showgrid: true
        },
        barmode: "group",
        annotations: [
            ...annotations,
            {
                xref: "paper",
                yref: "paper",
                x: 0.02,
                y: -0.35,
                text: "颜色说明：<span style='color:#42dd8a'>绿色</span> = 高于基准 | <span style='color:#ff6b6b'>红色</span> = 低于基准",
                showarrow: false,
                font: { color: "rgba(255,255,255,0.7)", size: 10 },
                align: "left"
            }
        ],
        margin: { t: 60, r: 20, b: 200, l: 80 },  // 增加底部边距以容纳图例和说明（两行标签）
        legend: { 
            orientation: "h", 
            y: -0.45,  // 往下移，避免与颜色说明重叠
            tracegroupgap: 10,
            itemsizing: "constant",
            font: { size: 11 }
        },
        shapes: [{
            type: "line",
            xref: "paper",
            yref: "y",
            x0: 0,
            x1: 1,
            y0: 0,
            y1: 0,
            line: {
                color: "rgba(255, 255, 255, 0.8)",  // 增强基准线可见性
                width: 3,  // 增加基准线宽度
                dash: "dash"
            }
        }]
    });

    Plotly.newPlot("chart-demographic-comparison", traces, layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 为图表3添加类别选择器事件监听
const setupDemographicCategorySelector = (data) => {
    const selector = document.getElementById("demographic-category-select");
    if (selector) {
        selector.addEventListener("change", (e) => {
            currentDemographicCategory = e.target.value;
            if (demographicDataCache) {
                renderDemographicComparison({ demographic_comparison: demographicDataCache }, currentDemographicCategory);
            }
        });
    }
};

// 图表4：学生特征重要性排序（按类别）
let currentImportanceCategory = "经济状况";
let importanceDataCache = null;

const renderDemographicImportance = (data, selectedCategory = null) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-demographic-importance");
        return;
    }

    const demo_data = data.demographic_comparison || [];
    if (demo_data.length === 0) {
        handlePlotlyMissing("chart-demographic-importance");
        return;
    }

    // 缓存数据
    importanceDataCache = demo_data;

    // 过滤掉All Students
    const filtered = demo_data.filter(
        d => d.SUBGROUP_NAME !== "All Students"
    );

    if (filtered.length === 0) {
        const container = document.getElementById("chart-demographic-importance");
        if (container) {
            container.innerHTML = "<p class=\"plot-note\">暂无数据。</p>";
        }
        return;
    }

    // 计算每个子组的重要性：基于与All Students的差距和样本量
    const importance_data = filtered.map(d => {
        const gap = Math.abs(d.gap_vs_all || 0);
        const sample_size = d.tested_cnt_valid || 0;
        // 重要性 = 绝对差距 * sqrt(样本量)
        const importance = gap * Math.sqrt(sample_size);
        return {
            ...d,
            importance: importance
        };
    });

    // 按类别分组，计算每个类别的聚合重要性
    const categoryImportance = {};
    
    ["AP", "IB"].forEach(apib => {
        const apib_data = importance_data.filter(d => d.APIB_IND === apib);
        
        // 按类别分组
        const categoryGroups = {};
        apib_data.forEach(d => {
            const cat = d.category || "其他";
            if (!categoryGroups[cat]) {
                categoryGroups[cat] = [];
            }
            categoryGroups[cat].push(d);
        });
        
        // 计算每个类别的聚合重要性
        // 使用最大重要性（最能代表该类别的影响强度）
        Object.keys(categoryGroups).forEach(cat => {
            const subgroups = categoryGroups[cat];
            const maxImportance = Math.max(...subgroups.map(d => d.importance || 0));
            const maxGap = Math.max(...subgroups.map(d => Math.abs(d.gap_vs_all || 0)));
            const avgGap = subgroups.reduce((sum, d) => sum + Math.abs(d.gap_vs_all || 0), 0) / subgroups.length;
            
            const categoryKey = `${cat}_${apib}`;
            categoryImportance[categoryKey] = {
                category: cat,
                apib: apib,
                importance: maxImportance,
                maxGap: maxGap,
                avgGap: avgGap,
                subgroupCount: subgroups.length
            };
        });
    });

    // 转换为数组并按类别分组
    const categoryList = Object.values(categoryImportance);
    
    // 按类别分组，计算每个类别在AP和IB中的重要性
    const categorySummary = {};
    categoryList.forEach(item => {
        const cat = item.category;
        if (!categorySummary[cat]) {
            categorySummary[cat] = {
                category: cat,
                apImportance: 0,
                ibImportance: 0,
                maxImportance: 0,
                apMaxGap: 0,
                ibMaxGap: 0
            };
        }
        if (item.apib === "AP") {
            categorySummary[cat].apImportance = item.importance;
            categorySummary[cat].apMaxGap = item.maxGap;
        } else {
            categorySummary[cat].ibImportance = item.importance;
            categorySummary[cat].ibMaxGap = item.maxGap;
        }
        categorySummary[cat].maxImportance = Math.max(
            categorySummary[cat].maxImportance,
            item.importance
        );
    });

    // 不按重要性排序，而是打乱顺序让大小值穿插，使图案效果更明显
    const allCategories = Object.values(categorySummary);
    
    // 打乱顺序：将大值和小值穿插排列
    // 方法：按重要性排序后，交替取最大值和最小值
    const sortedByImportance = [...allCategories].sort((a, b) => (b.maxImportance || 0) - (a.maxImportance || 0));
    const shuffledCategories = [];
    let left = 0;
    let right = sortedByImportance.length - 1;
    while (left <= right) {
        if (left === right) {
            shuffledCategories.push(sortedByImportance[left]);
            break;
        }
        shuffledCategories.push(sortedByImportance[left]);
        shuffledCategories.push(sortedByImportance[right]);
        left++;
        right--;
    }
    
    const categories = shuffledCategories.map(cat => cat.category);
    
    // 使用平方根缩放来压缩大值，使差距更明显
    const maxImportance = Math.max(
        ...shuffledCategories.map(cat => Math.max(cat.apImportance || 0, cat.ibImportance || 0)),
        1
    );
    
    // 使用平方根缩放：sqrt(value) / sqrt(max) * 100
    // 这样可以让大值之间的差距更明显
    const scaleValue = (val) => {
        if (val <= 0) return 0;
        return (Math.sqrt(val) / Math.sqrt(maxImportance)) * 100;
    };

    const traces = [
        {
            type: "scatterpolar",
            r: shuffledCategories.map(cat => scaleValue(cat.apImportance || 0)),
            theta: categories,
            fill: "toself",
            name: "AP",
            line: { color: palette.ap, width: 2 },
            fillcolor: palette.ap.replace(")", ", 0.3)").replace("rgb", "rgba"),
            hovertemplate: `AP<br>%{theta}<br>重要性（缩放后）：%{r:.1f}%<br>原始值：%{customdata:.2f}<extra></extra>`,
            customdata: shuffledCategories.map(cat => cat.apImportance || 0)
        },
        {
            type: "scatterpolar",
            r: shuffledCategories.map(cat => scaleValue(cat.ibImportance || 0)),
            theta: categories,
            fill: "toself",
            name: "IB",
            line: { color: palette.ib, width: 2 },
            fillcolor: palette.ib.replace(")", ", 0.3)").replace("rgb", "rgba"),
            hovertemplate: `IB<br>%{theta}<br>重要性（缩放后）：%{r:.1f}%<br>原始值：%{customdata:.2f}<extra></extra>`,
            customdata: shuffledCategories.map(cat => cat.ibImportance || 0)
        }
    ];

    const layout = createMidnightLayout({
        title: `学生特征类别重要性对比（雷达图，平方根缩放）`,
        polar: {
            radialaxis: {
                title: "重要性（平方根缩放，0-100）",
                range: [0, 100],
                gridcolor: "rgba(255, 255, 255, 0.2)",
                linecolor: "rgba(255, 255, 255, 0.5)",
                tickmode: "linear",
                tick0: 0,
                dtick: 20,
                tickfont: { color: "#1a1a1a", size: 11 },  // 深色字体，在白色背景上可见
                titlefont: { color: "#1a1a1a", size: 12 }   // 深色标题
            },
            angularaxis: {
                gridcolor: "rgba(255, 255, 255, 0.2)",
                linecolor: "rgba(255, 255, 255, 0.5)",
                tickfont: { color: "#f5f7fb", size: 11 }    // 白色字体，用于子组名称
            }
        },
        margin: { t: 60, r: 20, b: 50, l: 20 },
        legend: { orientation: "h", y: -0.15 }
    });

    Plotly.newPlot("chart-demographic-importance", traces, layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 图表4不再需要类别选择器，因为现在显示所有类别的对比
// const setupImportanceCategorySelector 已移除

// 图表5：N/RC与达标率关系（含县级分布）- 合并图表6和7
const renderNrcProficiencyRelationship = (data) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-nrc-proficiency-relationship");
        return;
    }

    const nrc_county_data = data.nrc_proficiency_relationship || data.nrc_county_relationship || [];
    if (nrc_county_data.length === 0) {
        handlePlotlyMissing("chart-nrc-proficiency-relationship");
        return;
    }

    // 分离县级数据和N/RC聚合数据
    const county_data = nrc_county_data.filter(d => d.county_name && !d.is_nrc_aggregate);
    const nrc_aggregate = nrc_county_data.filter(d => d.is_nrc_aggregate);

    const traces = [];
    
    // 县级散点（按AP/IB分组）
    ["AP", "IB"].forEach(apib => {
        const subset = county_data.filter(d => d.apib_ind === apib && d.nrc_code !== null && d.proficiency_rate !== null);
        if (subset.length > 0) {
            traces.push({
                type: "scatter",
                mode: "markers",
                x: subset.map(d => d.nrc_code),
                y: subset.map(d => (d.proficiency_rate || 0) * 100),
                name: `${apib} (县)`,
                marker: {
                    color: apib === "AP" ? palette.ap : palette.ib,
                    size: 12, // 增大点的大小
                    opacity: 0.7
                },
                text: subset.map(d => d.county_name),
                hovertemplate: "%{text}<br>N/RC：%{x}<br>达标率：%{y:.1f}%<extra></extra>"
            });
        }
    });

    // N/RC聚合趋势线
    ["AP", "IB"].forEach(apib => {
        const subset = nrc_aggregate
            .filter(d => d.apib_ind === apib && d.nrc_code !== null && d.proficiency_rate !== null)
            .sort((a, b) => (a.nrc_code || 0) - (b.nrc_code || 0));
        
        if (subset.length > 0) {
            traces.push({
                type: "scatter",
                mode: "lines+markers",
                x: subset.map(d => d.nrc_code),
                y: subset.map(d => (d.proficiency_rate || 0) * 100),
                name: `${apib} (趋势)`,
                marker: {
                    color: apib === "AP" ? palette.ap : palette.ib,
                    size: 10
                },
                line: {
                    color: apib === "AP" ? palette.ap : palette.ib,
                    width: 2,
                    dash: "dash"
                },
                hovertemplate: `${apib} (N/RC聚合)<br>N/RC：%{x}<br>平均达标率：%{y:.1f}%<extra></extra>`
            });
        }
    });

    const layout = createMidnightLayout({
        title: "N/RC与达标率关系（含县级分布）",
        xaxis: { 
            title: "N/RC代码（1=NYC, 2=大城市, 3=城市郊区高需求, 4=农村高需求, 5=平均需求, 6=低需求, 7=特许学校）",
            tickmode: "linear",
            tick0: 1,
            dtick: 1,
            gridcolor: "rgba(255, 255, 255, 0.15)", // 增加网格线透明度
            showgrid: true
        },
        yaxis: { 
            title: "达标率 (%)",
            gridcolor: "rgba(255, 255, 255, 0.15)" // 增加网格线透明度
        },
        margin: { t: 60, r: 20, b: 100, l: 60 }
    });

    Plotly.newPlot("chart-nrc-proficiency-relationship", traces, layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 图表6：NYC vs 非NYC对比（按N/RC细化）
const renderNycComparison = (data) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-nyc-comparison");
        return;
    }

    const nyc_nrc_data = data.nyc_comparison || data.nyc_nrc_summary || [];
    if (nyc_nrc_data.length === 0) {
        handlePlotlyMissing("chart-nyc-comparison");
        return;
    }

    // 按N/RC分组，每个N/RC内对比NYC和非NYC
    const nrc_codes = [...new Set(nyc_nrc_data.map(d => d.nrc_code || d.NRC_CODE).filter(c => c !== null && c !== undefined))].sort((a, b) => a - b);
    
    const traces = [];
    
    ["AP", "IB"].forEach(apib => {
        // NYC数据
        const nyc_subset = nrc_codes.map(nrc => {
            const item = nyc_nrc_data.find(d => 
                (d.nrc_code === nrc || d.NRC_CODE === nrc) && 
                (d.nyc_ind === 1 || d.NYC_IND === "1" || d.NYC_IND === 1) &&
                (d.apib_ind === apib || d.APIB_IND === apib)
            );
            return item ? (item.proficiency_rate || 0) * 100 : null;
        });
        
        // 非NYC数据
        const non_nyc_subset = nrc_codes.map(nrc => {
            const item = nyc_nrc_data.find(d => 
                (d.nrc_code === nrc || d.NRC_CODE === nrc) && 
                (d.nyc_ind === 0 || d.NYC_IND === "0" || d.NYC_IND === 0) &&
                (d.apib_ind === apib || d.APIB_IND === apib)
            );
            return item ? (item.proficiency_rate || 0) * 100 : null;
        });

        traces.push({
            type: "bar",
            x: nrc_codes.map(nrc => `N/RC ${nrc}`),
            y: nyc_subset,
            name: `${apib} - NYC`,
            marker: { 
                color: apib === "AP" ? palette.ap : palette.ib,
                opacity: 0.8
            },
            hovertemplate: `${apib} - NYC<br>%{x}<br>达标率：%{y:.1f}%<extra></extra>`
        });

        traces.push({
            type: "bar",
            x: nrc_codes.map(nrc => `N/RC ${nrc}`),
            y: non_nyc_subset,
            name: `${apib} - 非NYC`,
            marker: { 
                color: apib === "AP" ? palette.ap : palette.ib,
                opacity: 0.5,
                pattern: { shape: "/" }
            },
            hovertemplate: `${apib} - 非NYC<br>%{x}<br>达标率：%{y:.1f}%<extra></extra>`
        });
    });

    const layout = createMidnightLayout({
        title: "NYC vs 非NYC对比（按N/RC细化）",
        xaxis: { title: "N/RC分类" },
        yaxis: { title: "达标率 (%)" },
        barmode: "group",
        legend: { orientation: "h", y: -0.3 },
        margin: { t: 60, r: 20, b: 100, l: 60 }
    });

    Plotly.newPlot("chart-nyc-comparison", traces, layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 图表7：资源-结果象限图
const renderResourceQuadrant = (data) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-resource-quadrant");
        return;
    }

    const resource_data = data.resource_quadrant || [];
    if (resource_data.length === 0) {
        handlePlotlyMissing("chart-resource-quadrant");
        return;
    }

    // 过滤有效数据（学校级数据）
    const valid_data = resource_data.filter(
        d => d.nrc_code !== null && d.proficiency_rate !== null && d.proficiency_rate <= 1.0
    );

    if (valid_data.length === 0) {
        handlePlotlyMissing("chart-resource-quadrant");
        return;
    }

    // 计算中位数作为象限分割线
    const nrc_values = valid_data.map(d => d.nrc_code).filter(v => v !== null && v !== undefined);
    const proficiency_values = valid_data.map(d => d.proficiency_rate).filter(v => v !== null && v !== undefined);
    const nrc_mid = nrc_values.length > 0 ? nrc_values.sort((a, b) => a - b)[Math.floor(nrc_values.length / 2)] : 4;
    const prof_mid = proficiency_values.length > 0 ? proficiency_values.sort((a, b) => a - b)[Math.floor(proficiency_values.length / 2)] : 0.5;

    // 由于资源数据可能不包含APIB_IND，我们按N/RC分组显示
    const traces = [];
    
    // 按N/RC分组，每个N/RC显示所有学校
    const nrc_groups = {};
    valid_data.forEach(d => {
        const nrc = d.nrc_code;
        if (!nrc_groups[nrc]) {
            nrc_groups[nrc] = [];
        }
        nrc_groups[nrc].push(d);
    });

    // 为每个N/RC创建一个trace（限制显示数量）
    // 使用统一颜色，不区分N/RC
    Object.keys(nrc_groups).slice(0, 7).forEach(nrc => {
        const subset = nrc_groups[nrc].slice(0, 50); // 每个N/RC最多显示50个点
        traces.push({
            type: "scatter",
            mode: "markers",
            x: subset.map(d => d.nrc_code),
            y: subset.map(d => (d.proficiency_rate || 0) * 100),
            name: `N/RC ${nrc}`,
            text: subset.map(d => d.aggregation_name || d.county_name || ""),
            marker: {
                size: 8,  // 稍微增大点的大小，使其更明显
                opacity: 0.7,  // 增加不透明度，使点更清晰
                color: "rgba(88, 166, 255, 0.7)",  // 统一颜色，不区分N/RC
                line: {
                    width: 1,
                    color: "rgba(255, 255, 255, 0.5)"  // 添加白色边框，使点更明显
                }
            },
            hovertemplate: "%{text}<br>N/RC：%{x}<br>达标率：%{y:.1f}%<extra></extra>",
            showlegend: false  // 不显示图例，因为所有点颜色相同
        });
    });

    const layout = createMidnightLayout({
        title: "资源-结果象限图",
        xaxis: {
            title: "N/RC代码（资源能力）",
            zeroline: true,
            zerolinecolor: "rgba(255,255,255,0.3)",
            gridcolor: "rgba(255, 255, 255, 0.15)",  // 增加网格线透明度，使点更清楚
            gridwidth: 1.5,
            showgrid: true
        },
        yaxis: {
            title: "达标率 (%)",
            zeroline: true,
            zerolinecolor: "rgba(255,255,255,0.3)",
            gridcolor: "rgba(255, 255, 255, 0.15)",  // 增加网格线透明度，使点更清楚
            gridwidth: 1.5,
            showgrid: true
        },
        shapes: [
            {
                type: "line",
                x0: nrc_mid,
                x1: nrc_mid,
                y0: 0,
                y1: 100,
                line: { color: "rgba(255,255,255,0.3)", width: 1, dash: "dash" }
            },
            {
                type: "line",
                x0: 0,
                x1: 7,
                y0: prof_mid * 100,
                y1: prof_mid * 100,
                line: { color: "rgba(255,255,255,0.3)", width: 1, dash: "dash" }
            }
        ],
        legend: { orientation: "h", y: -0.25 }
    });

    Plotly.newPlot("chart-resource-quadrant", traces, layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 图表8：学科达标率排序（折线图）
const renderSubjectComparison = (data) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-subject-comparison");
        return;
    }

    const subject_data = data.subject_comparison || [];
    if (subject_data.length === 0) {
        handlePlotlyMissing("chart-subject-comparison");
        return;
    }

    // 获取所有学科
    const allSubjects = [...new Set(subject_data.map(d => d.SUBJECT_AREA || d.subject_area))];
    
    // 按AP达标率排序学科
    const apData = subject_data
        .filter(d => d.APIB_IND === "AP" && d.proficiency_rate !== null)
        .sort((a, b) => (b.proficiency_rate || 0) - (a.proficiency_rate || 0));
    
    const sortedSubjects = apData.map(d => d.SUBJECT_AREA || d.subject_area);

    // 构建折线图数据
    const apRates = sortedSubjects.map(subject => {
        const item = subject_data.find(d => 
            (d.SUBJECT_AREA || d.subject_area) === subject && d.APIB_IND === "AP"
        );
        return item ? (item.proficiency_rate || 0) * 100 : null;
    });

    const ibRates = sortedSubjects.map(subject => {
        const item = subject_data.find(d => 
            (d.SUBJECT_AREA || d.subject_area) === subject && d.APIB_IND === "IB"
        );
        return item ? (item.proficiency_rate || 0) * 100 : null;
    });

    const traces = [
        {
            type: "scatter",
            mode: "lines+markers",
            x: sortedSubjects,
            y: apRates,
            name: "AP",
            line: { color: palette.ap, width: 3 },
            marker: { color: palette.ap, size: 8 },
            hovertemplate: "AP<br>%{x}<br>达标率：%{y:.1f}%<extra></extra>"
        },
        {
            type: "scatter",
            mode: "lines+markers",
            x: sortedSubjects,
            y: ibRates,
            name: "IB",
            line: { color: palette.ib, width: 3 },
            marker: { color: palette.ib, size: 8 },
            hovertemplate: "IB<br>%{x}<br>达标率：%{y:.1f}%<extra></extra>"
        }
    ];

    const layout = createMidnightLayout({
        title: "学科达标率对比（AP vs IB，按AP达标率排序）",
        xaxis: { 
            title: "学科领域",
            tickangle: -45
        },
        yaxis: { title: "达标率 (%)" },
        margin: { t: 60, r: 20, b: 150, l: 60 },
        legend: { orientation: "h", y: -0.25 }
    });

    Plotly.newPlot("chart-subject-comparison", traces, layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 图表9：特征-目标相关性（按特征分类）
const renderFeatureCorrelation = (data) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-feature-correlation");
        return;
    }

    const corr_data = data.feature_correlation || [];
    if (corr_data.length === 0) {
        handlePlotlyMissing("chart-feature-correlation");
        return;
    }

    // 按特征分类分组
    const categorizeFeature = (feature) => {
        const f = feature.toLowerCase();
        if (f.includes("subgroup") || f.includes("经济") || f.includes("种族") || f.includes("性别") || f.includes("残疾") || f.includes("语言")) {
            return "学生特征";
        } else if (f.includes("county") || f.includes("nyc")) {
            return "地区特征";
        } else if (f.includes("nrc") || f.includes("district")) {
            return "资源特征";
        } else if (f.includes("subject") || f.includes("apib") || f.includes("grade")) {
            return "学科特征";
        }
        return "其他";
    };

    // 按特征分类分组
    const grouped = {};
    corr_data.forEach(d => {
        const group = categorizeFeature(d.feature || "");
        if (!grouped[group]) {
            grouped[group] = [];
        }
        grouped[group].push(d);
    });

    // 为每个特征组创建trace
    const traces = [];
    const colors = {
        "学生特征": palette.ap,
        "地区特征": palette.ib,
        "资源特征": palette.accent,
        "学科特征": palette.warning,
        "其他": palette.neutral
    };

    Object.entries(grouped).forEach(([group, items]) => {
        // 取Top 20并按绝对值排序
        const sorted = items
            .sort((a, b) => (b.abs_correlation || 0) - (a.abs_correlation || 0))
            .slice(0, 20);
        
        if (sorted.length > 0) {
            traces.push({
                type: "bar",
                orientation: "h",
                x: sorted.map(d => d.correlation || 0),
                y: sorted.map(d => `${group} - ${d.feature || ""}`),
                name: group,
                marker: { 
                    color: colors[group] || palette.neutral,
                    opacity: 0.7
                },
                hovertemplate: "%{y}<br>相关系数：%{x:.3f}<br>样本量：%{customdata}<extra></extra>",
                customdata: sorted.map(d => d.sample_size || 0)
            });
        }
    });

    const layout = createMidnightLayout({
        title: "特征-目标相关性（Top 20，按特征分类）",
        xaxis: { title: "相关系数" },
        yaxis: { 
            title: "特征（按分类）",
            autorange: "reversed"
        },
        barmode: "overlay",
        margin: { t: 60, r: 20, b: 50, l: 250 }
    });

    Plotly.newPlot("chart-feature-correlation", traces, layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 图表10：特征重要性排序（按业务含义分组：学生/地区/资源/学科）
const renderFeatureImportance = (data) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-feature-importance");
        return;
    }

    const importance_data = data.feature_importance || [];
    if (importance_data.length === 0) {
        handlePlotlyMissing("chart-feature-importance");
        return;
    }

    // 按重要性排序（全局排序）
    const sorted = [...importance_data]
        .sort((a, b) => (b.importance || 0) - (a.importance || 0));

    // 特征组颜色映射
    const getFeatureGroupColor = (group) => {
        if (group === "学生特征") return palette.ap;      // 蓝色
        if (group === "地区特征") return palette.ib;      // 橙色
        if (group === "资源特征") return palette.accent;  // 绿色
        if (group === "学科特征") return palette.warning; // 红色
        return palette.neutral;
    };

    // 创建图例（按特征组分组）
    const legendGroups = {};
    sorted.forEach(d => {
        const group = d.feature_group || "其他";
        if (!legendGroups[group]) {
            legendGroups[group] = getFeatureGroupColor(group);
        }
    });

    // 构建树状图数据（按特征组分组）
    const groupData = {};
    sorted.forEach(d => {
        const group = d.feature_group || "其他";
        if (!groupData[group]) {
            groupData[group] = [];
        }
        groupData[group].push({
            label: d.display_name || d.feature || "",
            value: d.importance || 0,
            color: getFeatureGroupColor(group),
            group: group,
            method: d.method || "",
            sample_size: d.sample_size || 0
        });
    });

    // 创建树状图数据格式
    const treemapLabels = [];
    const treemapValues = [];
    const treemapParents = [];
    const treemapColors = [];
    const treemapText = [];
    const treemapCustomdata = [];

    // 添加特征组作为父节点
    Object.keys(groupData).forEach(group => {
        const groupItems = groupData[group];
        const groupTotal = groupItems.reduce((sum, item) => sum + item.value, 0);
        
        // 添加特征组节点
        treemapLabels.push(group);
        treemapValues.push(groupTotal);
        treemapParents.push("");
        treemapColors.push(getFeatureGroupColor(group));
        treemapText.push(`${group}<br>总计：${groupTotal.toFixed(2)}`);
        treemapCustomdata.push([group, "", 0]);

        // 添加特征节点
        groupItems.forEach(item => {
            treemapLabels.push(item.label);
            treemapValues.push(item.value);
            treemapParents.push(group);
            treemapColors.push(item.color);
            treemapText.push(`${item.label}<br>${item.value.toFixed(2)}`);
            treemapCustomdata.push([item.group, item.method, item.sample_size]);
        });
    });

    const trace = {
        type: "treemap",
        labels: treemapLabels,
        values: treemapValues,
        parents: treemapParents,
        marker: {
            colors: treemapColors,
            line: { color: "#1a1a1a", width: 2 }
        },
        text: treemapText,
        textinfo: "label+value",
        texttemplate: "<b>%{label}</b><br>%{value:.2f}",
        hovertemplate: "<b>%{label}</b><br>特征组：%{customdata[0]}<br>重要性：%{value:.3f}<br>方法：%{customdata[1]}<br>样本量：%{customdata[2]}<extra></extra>",
        customdata: treemapCustomdata
    };

    const layout = createMidnightLayout({
        title: "特征重要性树状图（按业务含义分组）",
        margin: { t: 60, r: 20, b: 50, l: 20 }
    });

    Plotly.newPlot("chart-feature-importance", [trace], layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 图表11：特征组重要性对比（按业务含义分组：学生/地区/资源/学科）
const renderFeatureGroupImportance = (data) => {
    if (!window.Plotly) {
        handlePlotlyMissing("chart-feature-group-importance");
        return;
    }

    const group_data = data.feature_group_importance || [];
    if (group_data.length === 0) {
        handlePlotlyMissing("chart-feature-group-importance");
        return;
    }

    // 特征组颜色映射
    const getFeatureGroupColor = (group) => {
        if (group === "学生特征") return palette.ap;      // 蓝色
        if (group === "地区特征") return palette.ib;      // 橙色
        if (group === "资源特征") return palette.accent;  // 绿色
        if (group === "学科特征") return palette.warning; // 红色
        return palette.neutral;
    };

    // 创建环形图数据
    // 计算总重要性，用于计算百分比
    const totalImportance = group_data.reduce((sum, d) => sum + (Number(d.avg_importance) || 0), 0);
    
    // 为每个扇区创建包含最大重要性的hover文本
    const hoverTexts = group_data.map((d) => {
        const maxImp = Number(d.max_importance) || 0;
        const featureCount = Number(d.feature_count) || 0;
        const avgImp = Number(d.avg_importance) || 0;
        const percent = totalImportance > 0 ? ((avgImp / totalImportance) * 100).toFixed(1) : "0.0";
        return `<b>${d.feature_group || ""}</b><br>平均重要性：${avgImp.toFixed(3)}<br>占比：${percent}%<br>特征数：${featureCount}<br>最大重要性：<b>${maxImp.toFixed(2)}</b>`;
    });
    
    const trace = {
        type: "pie",
        labels: group_data.map(d => d.feature_group || ""),
        values: group_data.map(d => d.avg_importance || 0),
        hole: 0.4,  // 创建环形图
        marker: {
            colors: group_data.map(d => getFeatureGroupColor(d.feature_group || "")),
            line: { color: "#1a1a1a", width: 2 }
        },
        textinfo: "label+percent",
        textposition: "outside",
        // 使用hovertext数组，每个元素对应一个扇区
        hovertext: hoverTexts,
        hoverinfo: "text",
        customdata: group_data.map((d) => {
            // 确保数据是数字类型
            const maxImp = Number(d.max_importance) || 0;
            const featureCount = Number(d.feature_count) || 0;
            // 返回数组：[特征数, 最大重要性]
            return [featureCount, maxImp];
        })
    };

    const layout = createMidnightLayout({
        title: "特征组重要性对比（环形图）",
        margin: { t: 60, r: 20, b: 50, l: 20 },
        showlegend: true,
        legend: {
            orientation: "h",
            y: -0.2
        },
        annotations: []  // 删除总重要性说明
    });

    Plotly.newPlot("chart-feature-group-importance", [trace], layout, {
        displayModeBar: false,
        responsive: true
    });
};

// 主函数：加载数据并渲染所有图表
const init = async () => {
    try {
        const response = await fetch(DATA_URL);
        if (!response.ok) {
            throw new Error(`加载数据失败 (${response.status})`);
        }
        // 处理NaN值（参考research-equity.js的方法）
        const raw = await response.text();
        const sanitized = raw.replace(/\bNaN\b/g, "null");
        let chartSpecs;
        try {
            chartSpecs = JSON.parse(sanitized);
        } catch (parseError) {
            console.error("JSON解析失败:", parseError);
            console.error("JSON字符串前500字符:", sanitized.substring(0, 500));
            throw new Error(`JSON解析失败: ${parseError.message}`);
        }
        
        console.log("数据加载成功，键名:", Object.keys(chartSpecs));

        // 从chart_specs.json中提取数据（结构是嵌套的）
        const data = {
            proficiency_distribution: chartSpecs.proficiency_distribution?.data || [],
            suppression_sorted: chartSpecs.suppression_sorted?.data || [],
            demographic_comparison: chartSpecs.demographic_comparison?.data || [],
            demographic_importance: chartSpecs.demographic_importance?.data || chartSpecs.demographic_correlation?.data || [],
            nrc_proficiency_relationship: chartSpecs.nrc_proficiency_relationship?.data || chartSpecs.nrc_county_relationship?.data || [],
            nyc_comparison: chartSpecs.nyc_comparison?.data || chartSpecs.nyc_nrc_summary?.data || [],
            resource_quadrant: chartSpecs.resource_quadrant?.data || [],
            subject_comparison: chartSpecs.subject_comparison?.data || [],
            feature_correlation: chartSpecs.feature_correlation?.data || [],
            feature_importance: chartSpecs.feature_importance?.data || [],
            feature_group_importance: chartSpecs.feature_group_importance?.data || [],
            target_variable_summary: chartSpecs.target_variable_summary?.data || []
        };

        // 渲染概览统计
        renderStats(data);

        // 渲染所有图表（11个图表）
        renderProficiencyDistribution(data);  // 图表1
        renderSuppressionSorted(data);  // 图表2
        renderDemographicComparison(data);  // 图表3
        setupDemographicCategorySelector(data);  // 图表3选择器
        renderDemographicImportance(data);  // 图表4（类别对比，不需要选择器）
        renderNrcProficiencyRelationship(data);  // 图表5（合并后的）
        renderNycComparison(data);  // 图表6
        renderResourceQuadrant(data);  // 图表7
        renderSubjectComparison(data);  // 图表8
        renderFeatureCorrelation(data);  // 图表9
        renderFeatureImportance(data);  // 图表10
        renderFeatureGroupImportance(data);  // 图表11

        console.log("所有图表渲染完成");
    } catch (error) {
        console.error("初始化失败:", error);
        console.error("错误堆栈:", error.stack);
        document.querySelectorAll(".plotly-canvas").forEach(container => {
            container.innerHTML = `<p class="plot-note">数据加载失败：${error.message}</p>`;
        });
    }
};

// 页面加载完成后初始化
if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
} else {
    init();
}

