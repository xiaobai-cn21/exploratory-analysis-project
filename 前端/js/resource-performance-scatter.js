const RESOURCE_SCATTER_DATA_URL = '../../cleaned_data/resource_outcome_scatter.json';

let resourceScatterDataTable;
let resourceMaterialOptions;
let resourceClassicOptions;
let resourceCurrentMode = 'material';
let resourceScatterChart;

const loadScatterRecords = async () => {
  const response = await fetch(RESOURCE_SCATTER_DATA_URL, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`加载数据失败 (${response.status})`);
  }
  const payload = await response.json();
  if (!Array.isArray(payload) || payload.length === 0) {
    throw new Error('resource_outcome_scatter.json 内容为空');
  }
  return payload;
};

const buildScatterDataTable = (records) => {
  const data = new google.visualization.DataTable();
  data.addColumn('number', '资源排名');
  data.addColumn('number', '资源覆盖指数');
  data.addColumn({ type: 'string', role: 'tooltip', p: { html: true } });
  data.addColumn('number', '达标率');
  data.addColumn({ type: 'string', role: 'tooltip', p: { html: true } });

  records.forEach((record) => {
    data.addRow([
      record.rank,
      record.resource_index,
      buildScatterTooltip(record, '资源覆盖指数', record.resource_index, 'resource'),
      record.proficiency_rate,
      buildScatterTooltip(record, '达标率', record.proficiency_rate, 'outcome'),
    ]);
  });

  return data;
};

const buildScatterTooltip = (record, metricLabel, value, key) => {
  const formatter = new Intl.NumberFormat('en-US', { maximumFractionDigits: 1 });
  const suffix = key === 'resource' ? '%' : '%';
  return `
    <div class="regional-tooltip">
      <div class="regional-tooltip__title">${record.county_name}</div>
      <div class="regional-tooltip__value">${metricLabel}：<strong>${formatter.format(
        value,
      )}${suffix}</strong></div>
      <div>测试人数：${record.tested.toLocaleString('en-US')}</div>
      <div>课程数：${record.course_count}</div>
    </div>
  `;
};

const buildMaterialOptions = () => ({
  backgroundColor: 'transparent',
  chart: {
    title: '县域资源 vs 成绩（Top 35）',
    subtitle: 'X 轴为资源指数排名，双 Y 轴展示供给与达标',
  },
  height: 420,
  series: {
    0: { axis: 'resourceAxis', color: '#60a5fa' },
    1: { axis: 'outcomeAxis', color: '#fbbf24' },
  },
  axes: {
    x: {
      0: { label: '资源排名（1 = 资源最充足）' },
    },
    y: {
      resourceAxis: { label: '资源覆盖指数（%）' },
      outcomeAxis: { label: '考试达标率（%）' },
    },
  },
});

const buildClassicOptions = () => ({
  height: 420,
  backgroundColor: 'transparent',
  legend: { position: 'top', textStyle: { color: '#cbd5f5' } },
  hAxis: {
    title: '资源排名（1 = 资源最充足）',
    textStyle: { color: '#94a3b8' },
    titleTextStyle: { color: '#f5f7fb', bold: true },
  },
  vAxes: {
    0: {
      title: '资源覆盖指数（%）',
      textStyle: { color: '#94a3b8' },
      titleTextStyle: { color: '#f5f7fb', bold: true },
      viewWindow: { min: 0, max: 110 },
    },
    1: {
      title: '考试达标率（%）',
      textStyle: { color: '#94a3b8' },
      titleTextStyle: { color: '#f5f7fb', bold: true },
      viewWindow: { min: 40, max: 90 },
    },
  },
  series: {
    0: { targetAxisIndex: 0, color: '#60a5fa' },
    1: { targetAxisIndex: 1, color: '#f97316' },
  },
  tooltip: { isHtml: true },
  chartArea: { left: 70, right: 70, top: 70, bottom: 60 },
  pointSize: 6,
});

const renderScatterError = (message) => {
  const container = document.getElementById('resource-performance-chart');
  if (!container) {
    return;
  }
  container.innerHTML = `<p class="chart-error">${message}</p>`;
};

const attachResizeHandler = () => {
  window.addEventListener('resize', () => {
    if (!resourceScatterChart || !resourceScatterDataTable) {
      return;
    }
    if (resourceCurrentMode === 'material') {
      resourceScatterChart.draw(
        resourceScatterDataTable,
        google.charts.Scatter.convertOptions(resourceMaterialOptions),
      );
    } else {
      resourceScatterChart.draw(resourceScatterDataTable, resourceClassicOptions);
    }
  });
};

const initializeScatterChart = async () => {
  const container = document.getElementById('resource-performance-chart');
  const toggleButton = document.getElementById('resource-chart-toggle');
  if (!container || typeof google === 'undefined' || !google.visualization) {
    return;
  }

  try {
    const records = await loadScatterRecords();
    resourceScatterDataTable = buildScatterDataTable(records);
    resourceMaterialOptions = buildMaterialOptions();
    resourceClassicOptions = buildClassicOptions();

    const drawMaterialChart = () => {
      resourceCurrentMode = 'material';
      resourceScatterChart = new google.charts.Scatter(container);
      resourceScatterChart.draw(
        resourceScatterDataTable,
        google.charts.Scatter.convertOptions(resourceMaterialOptions),
      );
      if (toggleButton) {
        toggleButton.textContent = '切换为经典样式';
      }
    };

    const drawClassicChart = () => {
      resourceCurrentMode = 'classic';
      resourceScatterChart = new google.visualization.ScatterChart(container);
      resourceScatterChart.draw(resourceScatterDataTable, resourceClassicOptions);
      if (toggleButton) {
        toggleButton.textContent = '切换为 Material 样式';
      }
    };

    if (toggleButton) {
      toggleButton.addEventListener('click', () => {
        if (resourceCurrentMode === 'material') {
          drawClassicChart();
        } else {
          drawMaterialChart();
        }
      });
    }

    drawMaterialChart();
    attachResizeHandler();
  } catch (error) {
    console.error(error);
    renderScatterError('无法加载散点图数据，请稍后重试。');
  }
};

google.charts.load('current', { packages: ['corechart', 'scatter'] });
google.charts.setOnLoadCallback(initializeScatterChart);


