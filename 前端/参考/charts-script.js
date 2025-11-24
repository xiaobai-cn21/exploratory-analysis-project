// ============================================
// 图表库页面脚本
// ============================================

// 代码显示/隐藏切换
function toggleCode(codeId) {
    const codeBlock = document.getElementById(codeId);
    const button = event.target;
    
    if (codeBlock.classList.contains('active')) {
        codeBlock.classList.remove('active');
        button.textContent = '显示代码 ▼';
        button.classList.remove('active');
    } else {
        codeBlock.classList.add('active');
        button.textContent = '隐藏代码 ▲';
        button.classList.add('active');
    }
}

// 初始化所有图表
function initAllCharts() {
    initBarChart();
    initStackedBar();
    initLineChart();
    initPieChart();
    initScatterPlot();
    initHeatmap();
    initBoxPlot();
    initGroupedBar();
}

// 1. 柱状图
function initBarChart() {
    var data = [{
        x: ['AP Calculus AB', 'AP English Language', 'AP Biology', 'AP US History', 'AP Chemistry'],
        y: [12500, 11200, 9800, 8500, 7200],
        type: 'bar',
        marker: {
            color: '#003DA5',
            line: {
                color: '#1E3A8A',
                width: 1
            }
        },
        text: [12500, 11200, 9800, 8500, 7200],
        textposition: 'outside',
        textfont: {
            size: 12,
            color: '#1F2937'
        }
    }];

    var layout = {
        title: {
            text: 'Top 5 AP课程注册人数',
            font: {
                size: 20,
                color: '#003DA5',
                family: 'Noto Sans SC, sans-serif'
            }
        },
        xaxis: {
            title: '课程名称',
            titlefont: {
                size: 14,
                color: '#1F2937'
            },
            tickfont: {
                size: 12,
                color: '#6B7280'
            }
        },
        yaxis: {
            title: '注册人数',
            titlefont: {
                size: 14,
                color: '#1F2937'
            },
            tickfont: {
                size: 12,
                color: '#6B7280'
            }
        },
        plot_bgcolor: '#FFFFFF',
        paper_bgcolor: '#FFFFFF',
        font: {
            family: 'Noto Sans SC, sans-serif'
        },
        margin: {
            l: 60,
            r: 30,
            t: 80,
            b: 60
        }
    };

    Plotly.newPlot('bar-chart-1', data, layout, {responsive: true});
}

// 2. 堆叠柱状图
function initStackedBar() {
    var trace1 = {
        x: ['数学', '科学', '语言', '历史', '艺术'],
        y: [45, 38, 32, 28, 22],
        name: '经济困难学生',
        type: 'bar',
        marker: {
            color: '#EF4444'
        }
    };

    var trace2 = {
        x: ['数学', '科学', '语言', '历史', '艺术'],
        y: [55, 62, 68, 72, 78],
        name: '非经济困难学生',
        type: 'bar',
        marker: {
            color: '#10B981'
        }
    };

    var data = [trace1, trace2];

    var layout = {
        title: {
            text: '不同学科领域的参与率对比',
            font: {
                size: 20,
                color: '#003DA5',
                family: 'Noto Sans SC, sans-serif'
            }
        },
        barmode: 'stack',
        xaxis: {
            title: '学科领域',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        yaxis: {
            title: '参与率 (%)',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        plot_bgcolor: '#FFFFFF',
        paper_bgcolor: '#FFFFFF',
        font: {
            family: 'Noto Sans SC, sans-serif'
        },
        legend: {
            x: 0.7,
            y: 1,
            bgcolor: 'rgba(255,255,255,0.8)'
        },
        margin: {
            l: 60,
            r: 30,
            t: 80,
            b: 60
        }
    };

    Plotly.newPlot('stacked-bar-1', data, layout, {responsive: true});
}

// 3. 折线图
function initLineChart() {
    var trace1 = {
        x: ['9年级', '10年级', '11年级', '12年级'],
        y: [15, 35, 58, 72],
        mode: 'lines+markers',
        name: 'AP课程参与率',
        type: 'scatter',
        line: {
            color: '#003DA5',
            width: 3
        },
        marker: {
            size: 10,
            color: '#003DA5'
        }
    };

    var trace2 = {
        x: ['9年级', '10年级', '11年级', '12年级'],
        y: [8, 22, 45, 68],
        mode: 'lines+markers',
        name: 'IB课程参与率',
        type: 'scatter',
        line: {
            color: '#3B82F6',
            width: 3
        },
        marker: {
            size: 10,
            color: '#3B82F6'
        }
    };

    var data = [trace1, trace2];

    var layout = {
        title: {
            text: '不同年级的AP/IB课程参与率趋势',
            font: {
                size: 20,
                color: '#003DA5',
                family: 'Noto Sans SC, sans-serif'
            }
        },
        xaxis: {
            title: '年级',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        yaxis: {
            title: '参与率 (%)',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        plot_bgcolor: '#FFFFFF',
        paper_bgcolor: '#FFFFFF',
        font: {
            family: 'Noto Sans SC, sans-serif'
        },
        legend: {
            x: 0.05,
            y: 0.95
        },
        margin: {
            l: 60,
            r: 30,
            t: 80,
            b: 60
        }
    };

    Plotly.newPlot('line-chart-1', data, layout, {responsive: true});
}

// 4. 饼图
function initPieChart() {
    var data = [{
        values: [35, 28, 18, 12, 7],
        labels: ['白人', '西班牙裔', '非裔', '亚裔', '其他'],
        type: 'pie',
        marker: {
            colors: ['#003DA5', '#3B82F6', '#1E3A8A', '#60A5FA', '#93C5FD'],
            line: {
                color: '#FFFFFF',
                width: 2
            }
        },
        textfont: {
            size: 14,
            family: 'Noto Sans SC, sans-serif',
            color: '#1F2937'
        },
        textposition: 'outside',
        textinfo: 'label+percent',
        hovertemplate: '<b>%{label}</b><br>' +
                       '占比: %{percent}<br>' +
                       '人数: %{value}<extra></extra>'
    }];

    var layout = {
        title: {
            text: 'AP/IB学生种族分布',
            font: {
                size: 20,
                color: '#003DA5',
                family: 'Noto Sans SC, sans-serif'
            }
        },
        plot_bgcolor: '#FFFFFF',
        paper_bgcolor: '#FFFFFF',
        font: {
            family: 'Noto Sans SC, sans-serif'
        },
        margin: {
            l: 30,
            r: 30,
            t: 80,
            b: 30
        },
        showlegend: true,
        legend: {
            x: 1.1,
            y: 0.5,
            font: {
                size: 12
            }
        }
    };

    Plotly.newPlot('pie-chart-1', data, layout, {responsive: true});
}

// 5. 散点图
function initScatterPlot() {
    var trace1 = {
        x: [45, 52, 58, 62, 68, 72, 75, 78, 82, 85],
        y: [35, 42, 48, 52, 58, 62, 65, 68, 72, 75],
        mode: 'markers',
        name: '高N/RC学区',
        type: 'scatter',
        marker: {
            size: 12,
            color: '#EF4444',
            opacity: 0.7,
            line: {
                color: '#DC2626',
                width: 1
            }
        }
    };

    var trace2 = {
        x: [55, 62, 68, 72, 78, 82, 85, 88, 92, 95],
        y: [48, 55, 62, 68, 72, 75, 78, 82, 85, 88],
        mode: 'markers',
        name: '低N/RC学区',
        type: 'scatter',
        marker: {
            size: 12,
            color: '#10B981',
            opacity: 0.7,
            line: {
                color: '#059669',
                width: 1
            }
        }
    };

    var data = [trace1, trace2];

    var layout = {
        title: {
            text: '参与率与达标率相关性分析',
            font: {
                size: 20,
                color: '#003DA5',
                family: 'Noto Sans SC, sans-serif'
            }
        },
        xaxis: {
            title: '参与率 (%)',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        yaxis: {
            title: '达标率 (%)',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        plot_bgcolor: '#FFFFFF',
        paper_bgcolor: '#FFFFFF',
        font: {
            family: 'Noto Sans SC, sans-serif'
        },
        legend: {
            x: 0.05,
            y: 0.95
        },
        margin: {
            l: 60,
            r: 30,
            t: 80,
            b: 60
        }
    };

    Plotly.newPlot('scatter-plot-1', data, layout, {responsive: true});
}

// 6. 热力图
function initHeatmap() {
    var data = [{
        z: [[15, 25, 35, 45],
             [20, 30, 40, 50],
             [12, 22, 32, 42],
             [18, 28, 38, 48],
             [10, 20, 30, 40]],
        x: ['9年级', '10年级', '11年级', '12年级'],
        y: ['数学', '科学', '语言', '历史', '艺术'],
        type: 'heatmap',
        colorscale: [
            [0, '#E0F2FE'],
            [0.25, '#93C5FD'],
            [0.5, '#3B82F6'],
            [0.75, '#1E3A8A'],
            [1, '#003DA5']
        ],
        colorbar: {
            title: '参与率 (%)',
            titlefont: {
                size: 12,
                color: '#1F2937'
            },
            tickfont: {
                size: 11,
                color: '#6B7280'
            }
        },
        hovertemplate: '学科: %{y}<br>' +
                       '年级: %{x}<br>' +
                       '参与率: %{z}%<extra></extra>'
    }];

    var layout = {
        title: {
            text: '不同学科和年级的参与率热力图',
            font: {
                size: 20,
                color: '#003DA5',
                family: 'Noto Sans SC, sans-serif'
            }
        },
        xaxis: {
            title: '年级',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        yaxis: {
            title: '学科领域',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        plot_bgcolor: '#FFFFFF',
        paper_bgcolor: '#FFFFFF',
        font: {
            family: 'Noto Sans SC, sans-serif'
        },
        margin: {
            l: 100,
            r: 30,
            t: 80,
            b: 60
        }
    };

    Plotly.newPlot('heatmap-1', data, layout, {responsive: true});
}

// 7. 箱线图
function initBoxPlot() {
    var trace1 = {
        y: [2.5, 3.0, 3.2, 3.5, 3.8, 4.0, 4.2, 4.5, 4.8, 5.0],
        name: '经济困难学生',
        type: 'box',
        marker: {
            color: '#EF4444'
        },
        boxmean: 'sd'
    };

    var trace2 = {
        y: [3.2, 3.5, 3.8, 4.0, 4.2, 4.5, 4.7, 4.9, 5.0, 5.0],
        name: '非经济困难学生',
        type: 'box',
        marker: {
            color: '#10B981'
        },
        boxmean: 'sd'
    };

    var data = [trace1, trace2];

    var layout = {
        title: {
            text: '不同经济背景学生的AP成绩分布',
            font: {
                size: 20,
                color: '#003DA5',
                family: 'Noto Sans SC, sans-serif'
            }
        },
        yaxis: {
            title: 'AP成绩 (1-5分)',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        plot_bgcolor: '#FFFFFF',
        paper_bgcolor: '#FFFFFF',
        font: {
            family: 'Noto Sans SC, sans-serif'
        },
        legend: {
            x: 0.7,
            y: 0.95
        },
        margin: {
            l: 60,
            r: 30,
            t: 80,
            b: 60
        }
    };

    Plotly.newPlot('box-plot-1', data, layout, {responsive: true});
}

// 8. 分组柱状图
function initGroupedBar() {
    var trace1 = {
        x: ['数学', '科学', '语言', '历史'],
        y: [45, 38, 35, 32],
        name: '白人',
        type: 'bar',
        marker: {
            color: '#003DA5'
        }
    };

    var trace2 = {
        x: ['数学', '科学', '语言', '历史'],
        y: [32, 28, 42, 25],
        name: '西班牙裔',
        type: 'bar',
        marker: {
            color: '#3B82F6'
        }
    };

    var trace3 = {
        x: ['数学', '科学', '语言', '历史'],
        y: [28, 25, 38, 22],
        name: '非裔',
        type: 'bar',
        marker: {
            color: '#1E3A8A'
        }
    };

    var trace4 = {
        x: ['数学', '科学', '语言', '历史'],
        y: [58, 52, 48, 45],
        name: '亚裔',
        type: 'bar',
        marker: {
            color: '#60A5FA'
        }
    };

    var data = [trace1, trace2, trace3, trace4];

    var layout = {
        title: {
            text: '不同种族在各学科领域的参与率对比',
            font: {
                size: 20,
                color: '#003DA5',
                family: 'Noto Sans SC, sans-serif'
            }
        },
        barmode: 'group',
        xaxis: {
            title: '学科领域',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        yaxis: {
            title: '参与率 (%)',
            titlefont: {
                size: 14,
                color: '#1F2937'
            }
        },
        plot_bgcolor: '#FFFFFF',
        paper_bgcolor: '#FFFFFF',
        font: {
            family: 'Noto Sans SC, sans-serif'
        },
        legend: {
            x: 0.7,
            y: 1,
            bgcolor: 'rgba(255,255,255,0.8)'
        },
        margin: {
            l: 60,
            r: 30,
            t: 80,
            b: 60
        }
    };

    Plotly.newPlot('grouped-bar-1', data, layout, {responsive: true});
}

// 页面加载完成后初始化所有图表
document.addEventListener('DOMContentLoaded', () => {
    if (typeof Plotly !== 'undefined') {
        initAllCharts();
    } else {
        console.error('Plotly library not loaded');
    }
});

// 窗口大小改变时重新调整图表
window.addEventListener('resize', () => {
    if (typeof Plotly !== 'undefined') {
        Plotly.Plots.resize('bar-chart-1');
        Plotly.Plots.resize('stacked-bar-1');
        Plotly.Plots.resize('line-chart-1');
        Plotly.Plots.resize('pie-chart-1');
        Plotly.Plots.resize('scatter-plot-1');
        Plotly.Plots.resize('heatmap-1');
        Plotly.Plots.resize('box-plot-1');
        Plotly.Plots.resize('grouped-bar-1');
    }
});
