const NYC_DONUT_DATA_URL = '/static/js/county_coverage.json';
const NYC_COUNTIES = new Set(['BRONX', 'KINGS', 'NEW YORK', 'QUEENS', 'RICHMOND']);

const NYC_METRIC_CONFIGS = {
  tested: {
    label: '考试参与规模',
    accessor: (record) => record.tested ?? 0,
    detail: (share) => `NYC 贡献了全州 ${share} 的考试参与人数，体现都市区学生体量优势。`,
  },
  course_count: {
    label: '课程供给数',
    accessor: (record) => record.course_count ?? 0,
    detail: (share) =>
      `课程供给份额为 ${share}，表明 NYC 课程覆盖更密集，但也意味着非 NYC 的拓科潜力。`,
  },
  coverage_mass: {
    label: '覆盖权重',
    accessor: (record) =>
      typeof record.coverage_score === 'number' && typeof record.tested === 'number'
        ? record.coverage_score * record.tested
        : 0,
    detail: (share) =>
      `覆盖权重（覆盖度 × 规模）中 NYC 占 ${share}，若希望资源均衡，可优先扶持非 NYC 的高潜县。`,
  },
};

let nycDonutData;
let nycDonutChart;
let activeMetricKey = 'tested';

const fetchNycDonutSource = async () => {
  const response = await fetch(NYC_DONUT_DATA_URL, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`县域覆盖数据加载失败（${response.status}）`);
  }
  const payload = await response.json();
  if (!Array.isArray(payload) || payload.length === 0) {
    throw new Error('county_coverage.json 无数据');
  }
  return payload;
};

const summarizeByMetric = (records, metricKey) => {
  const config = NYC_METRIC_CONFIGS[metricKey];
  return records.reduce(
    (acc, record) => {
      const value = config.accessor(record);
      if (Number.isFinite(value) && value > 0) {
        if (NYC_COUNTIES.has(record.county_name)) {
          acc.nyc += value;
        } else {
          acc.nonNyc += value;
        }
      }
      return acc;
    },
    { nyc: 0, nonNyc: 0 },
  );
};

const buildNycDonutDataTable = (summary) => {
  const data = new google.visualization.DataTable();
  data.addColumn('string', '区域');
  data.addColumn('number', '数值');
  data.addRows([
    ['NYC', summary.nyc],
    ['非 NYC', summary.nonNyc],
  ]);
  return data;
};

const buildNycDonutOptions = () => ({
  pieHole: 0.62,
  pieSliceText: 'percentage',
  backgroundColor: 'transparent',
  legend: { position: 'right', textStyle: { color: '#64748b', fontSize: 13 } },
  pieSliceTextStyle: { color: '#0f172a', fontSize: 13, bold: true },
  slices: {
    0: { color: '#f1c0b9' },
    1: { color: '#8fb7cf' },
  },
  chartArea: { width: '80%', height: '80%' },
  tooltip: { text: 'percentage' },
});

const formatSharePercent = (value) => `${(value * 100).toFixed(1)}%`;

const updateNycInsightPanel = (summary, metricKey) => {
  const total = summary.nyc + summary.nonNyc;
  const share = total === 0 ? 0 : summary.nyc / total;
  const shareText = formatSharePercent(share);
  const metricLabelNode = document.querySelector('[data-nyc-metric-label]');
  const shareNode = document.querySelector('[data-nyc-share]');
  const detailNode = document.querySelector('[data-nyc-detail]');

  if (metricLabelNode) {
    metricLabelNode.textContent = NYC_METRIC_CONFIGS[metricKey].label;
  }
  if (shareNode) {
    shareNode.textContent = shareText;
  }
  if (detailNode) {
    detailNode.textContent = NYC_METRIC_CONFIGS[metricKey].detail(shareText);
  }
};

const renderNycDonutError = (message) => {
  const container = document.getElementById('nyc-donut-chart');
  if (container) {
    container.innerHTML = `<p class="chart-error">${message}</p>`;
  }
};

const drawNycDonutChart = (summary) => {
  const container = document.getElementById('nyc-donut-chart');
  if (!container) {
    return;
  }
  if (!nycDonutChart) {
    nycDonutChart = new google.visualization.PieChart(container);
  }
  const data = buildNycDonutDataTable(summary);
  nycDonutChart.draw(data, buildNycDonutOptions());
};

const setActiveMetricButton = (metricKey) => {
  const buttons = document.querySelectorAll('[data-nyc-metric] .pill');
  buttons.forEach((button) => {
    const isActive = button.dataset.metric === metricKey;
    button.classList.toggle('is-active', isActive);
    button.setAttribute('aria-selected', String(isActive));
  });
};

const handleMetricChange = (records, metricKey) => {
  activeMetricKey = metricKey;
  setActiveMetricButton(metricKey);
  const summary = summarizeByMetric(records, metricKey);
  drawNycDonutChart(summary);
  updateNycInsightPanel(summary, metricKey);
};

const bindMetricPills = (records) => {
  const container = document.querySelector('[data-nyc-metric]');
  if (!container) {
    return;
  }
  container.addEventListener('click', (event) => {
    const target = event.target.closest('button[data-metric]');
    if (!target) {
      return;
    }
    const metricKey = target.dataset.metric;
    if (metricKey && NYC_METRIC_CONFIGS[metricKey] && metricKey !== activeMetricKey) {
      handleMetricChange(records, metricKey);
    }
  });
};

const initializeNycDonut = async () => {
  const chartContainer = document.getElementById('nyc-donut-chart');
  if (!chartContainer || typeof google === 'undefined') {
    return;
  }

  try {
    const records = await fetchNycDonutSource();
    nycDonutData = records;
    bindMetricPills(records);
    handleMetricChange(records, activeMetricKey);
    window.addEventListener('resize', () => {
      if (nycDonutData) {
        handleMetricChange(nycDonutData, activeMetricKey);
      }
    });
  } catch (error) {
    console.error(error);
    renderNycDonutError('圆环图加载失败，请稍后重试。');
  }
};

google.charts.load('current', { packages: ['corechart'] });
google.charts.setOnLoadCallback(initializeNycDonut);




