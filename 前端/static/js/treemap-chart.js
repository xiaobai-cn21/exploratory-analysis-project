/**
 * TreeMap多层级地理分布可视化
 * 展示：全州 → N/RC → 县的层级结构
 */
const TREEMAP_DATA_URL = '/static/js/treemap_data.json';

let treemapChart;

/**
 * 加载TreeMap数据
 */
const loadTreemapData = async () => {
  try {
    const response = await fetch(TREEMAP_DATA_URL);
    if (!response.ok) {
      throw new Error(`Failed to load ${TREEMAP_DATA_URL} (${response.status})`);
    }
    return await response.json();
  } catch (error) {
    console.error('Error loading treemap data:', error);
    throw error;
  }
};

/**
 * 转换数据为Google Charts格式
 */
const convertToGoogleChartsFormat = (data) => {
  // Google Charts TreeMap格式: [Location, Parent, Size, Color]
  const chartData = [
    ['Location', 'Parent', 'Size (学生数)', 'Color (达标率)']
  ];
  
  // 添加所有节点
  data.forEach(node => {
    chartData.push([
      node.Location,
      node.Parent || null,
      node.Size,
      node.Color
    ]);
  });
  
  return chartData;
};

/**
 * 绘制TreeMap
 */
const drawTreemap = async () => {
  const container = document.getElementById('treemap-chart');
  if (!container) {
    console.warn('TreeMap container not found');
    return;
  }

  if (typeof google === 'undefined' || !google.charts) {
    container.innerHTML = '<p class="chart-error">未加载 Google Charts，无法绘制TreeMap。</p>';
    return;
  }

  try {
    // 加载数据
    const rawData = await loadTreemapData();
    
    // 转换为Google Charts格式
    const chartData = google.visualization.arrayToDataTable(
      convertToGoogleChartsFormat(rawData)
    );

    // 检测暗色主题
    const isDarkTheme = document.body.classList.contains('ny-site') || 
                        document.documentElement.classList.contains('dark') ||
                        window.matchMedia('(prefers-color-scheme: dark)').matches;

    // 配置选项
    const options = {
      minColor: '#1e3a5f',      // 低达标率 - 深蓝（暗色主题）
      midColor: '#3b82f6',      // 中等达标率 - 亮蓝
      maxColor: '#60a5fa',      // 高达标率 - 浅蓝
      headerHeight: 15,
      fontColor: isDarkTheme ? '#e2e8f0' : '#1e293b',  // 暗色主题用浅色文字
      showScale: true,
      fontSize: 12,
      fontFamily: 'Open Sans, sans-serif',
      useWeightedAverageForAggregation: true,
      backgroundColor: isDarkTheme ? '#0f172a' : '#ffffff',  // 暗色背景
      tooltip: {
        trigger: 'item',
        textStyle: {
          color: isDarkTheme ? '#e2e8f0' : '#1e293b'
        },
        formatter: (params) => {
          // 自定义tooltip显示
          const node = rawData.find(n => n.Location === params.name);
          if (node) {
            return `
              <div style="padding: 8px; color: ${isDarkTheme ? '#e2e8f0' : '#1e293b'};">
                <strong>${node.Location}</strong><br/>
                ${node.Description || ''}<br/>
                达标率: ${(node.Color * 100).toFixed(1)}%
              </div>
            `;
          }
          return params.name;
        }
      }
    };

    // 创建并绘制TreeMap
    treemapChart = new google.visualization.TreeMap(container);
    treemapChart.draw(chartData, options);

    // 响应式调整
    window.addEventListener('resize', () => {
      if (treemapChart) {
        treemapChart.draw(chartData, options);
      }
    });

  } catch (error) {
    console.error('Error drawing treemap:', error);
    container.innerHTML = '<p class="chart-error">无法加载TreeMap数据，请稍后重试。</p>';
  }
};

/**
 * 初始化TreeMap
 */
const initTreemap = () => {
  // 确保Google Charts已加载
  if (typeof google !== 'undefined' && google.charts) {
    google.charts.load('current', { packages: ['treemap'] });
    google.charts.setOnLoadCallback(drawTreemap);
  } else {
    // 如果Google Charts未加载，等待DOMContentLoaded
    if (document.readyState === 'loading') {
      document.addEventListener('DOMContentLoaded', () => {
        if (typeof google !== 'undefined' && google.charts) {
          google.charts.load('current', { packages: ['treemap'] });
          google.charts.setOnLoadCallback(drawTreemap);
        }
      });
    } else {
      // DOM已加载，但Google Charts可能还未加载
      // 等待一段时间后重试
      setTimeout(() => {
        if (typeof google !== 'undefined' && google.charts) {
          google.charts.load('current', { packages: ['treemap'] });
          google.charts.setOnLoadCallback(drawTreemap);
        }
      }, 500);
    }
  }
};

// 自动初始化
if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', initTreemap);
} else {
  initTreemap();
}




