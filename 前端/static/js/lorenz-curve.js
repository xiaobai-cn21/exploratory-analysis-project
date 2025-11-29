const LORENZ_DATA_URL = '/static/js/county_coverage.json';

let lorenzDataTable;
let lorenzChartOptions;
let lorenzChart;

const loadLorenzRecords = async () => {
  const response = await fetch(LORENZ_DATA_URL, { cache: 'no-store' });
  if (!response.ok) {
    throw new Error(`无法加载 Lorenz 数据（${response.status}）`);
  }
  const payload = await response.json();
  if (!Array.isArray(payload) || payload.length === 0) {
    throw new Error('county_coverage.json 内容为空');
  }
  return payload;
};

const computeLorenzPoints = (records) => {
  const sortable = records
    .filter(
      (item) =>
        typeof item.tested === 'number' &&
        typeof item.coverage_score === 'number' &&
        item.tested > 0,
    )
    .map((item) => ({
      countyName: item.county_name ?? item.county_code,
      tested: item.tested,
      resourceMass: item.coverage_score * item.tested,
    }))
    .sort((a, b) => a.resourceMass - b.resourceMass);

  const totalTested = sortable.reduce((sum, item) => sum + item.tested, 0);
  const totalResource = sortable.reduce((sum, item) => sum + item.resourceMass, 0);

  if (totalTested === 0 || totalResource === 0) {
    throw new Error('Lorenz 计算所需的指标为 0');
  }

  const points = [
    {
      populationShare: 0,
      resourceShare: 0,
      label: '起点',
    },
  ];

  let cumulativeTested = 0;
  let cumulativeResource = 0;

  sortable.forEach((item) => {
    cumulativeTested += item.tested;
    cumulativeResource += item.resourceMass;
    points.push({
      populationShare: cumulativeTested / totalTested,
      resourceShare: cumulativeResource / totalResource,
      label: item.countyName,
      latestShare: item.resourceMass / totalResource,
    });
  });

  return points;
};

const buildLorenzDataTable = (points) => {
  const formatter = new Intl.NumberFormat('en-US', {
    style: 'percent',
    maximumFractionDigits: 1,
  });

  const data = new google.visualization.DataTable();
  data.addColumn('number', '累计学生占比');
  data.addColumn('number', '资源累计占比');
  data.addColumn({ type: 'string', role: 'tooltip', p: { html: true } });
  data.addColumn('number', '完全均衡');

  points.forEach((point, index) => {
    const tooltip =
      index === 0
        ? `
      <div class="regional-tooltip">
        <div class="regional-tooltip__title">起点</div>
        <div class="regional-tooltip__value">0% 学生，0% 资源</div>
      </div>
    `
        : `
      <div class="regional-tooltip">
        <div class="regional-tooltip__title">${point.label}</div>
        <div class="regional-tooltip__value">累计学生：<strong>${formatter.format(
          point.populationShare,
        )}</strong></div>
        <div>累计资源：${formatter.format(point.resourceShare)}</div>
      </div>
    `;

    data.addRow([
      point.populationShare,
      point.resourceShare,
      tooltip,
      point.populationShare,
    ]);
  });

  return data;
};

const buildLorenzOptions = () => ({
  backgroundColor: 'transparent',
  chart: {
    title: '县域 AP/IB 资源 Lorenz 曲线',
    subtitle: 'X 轴为学生累计占比，Y 轴为资源累计占比',
  },
  height: 420,
  legend: { position: 'bottom', textStyle: { color: '#cbd5f5' } },
  series: {
    0: { color: '#7aa5c9', lineWidth: 3, pointsVisible: true },
    1: { color: '#cfd8dc', lineDashStyle: [6, 6], lineWidth: 2, pointsVisible: false },
  },
  hAxis: {
    minValue: 0,
    maxValue: 1,
    format: 'percent',
    textStyle: { color: '#94a3b8' },
    title: '学生累计占比',
    titleTextStyle: { color: '#f5f7fb', bold: true },
    gridlines: { color: 'rgba(148,163,184,0.2)' },
  },
  vAxis: {
    minValue: 0,
    maxValue: 1,
    format: 'percent',
    textStyle: { color: '#94a3b8' },
    title: '资源累计占比',
    titleTextStyle: { color: '#f5f7fb', bold: true },
    gridlines: { color: 'rgba(148,163,184,0.2)' },
  },
  tooltip: { isHtml: true },
  chartArea: { top: 50, bottom: 80, left: 70, right: 30 },
});

const calculateGini = (points) => {
  let area = 0;
  for (let i = 1; i < points.length; i += 1) {
    const x1 = points[i - 1].populationShare;
    const x2 = points[i].populationShare;
    const y1 = points[i - 1].resourceShare;
    const y2 = points[i].resourceShare;
    area += ((y1 + y2) / 2) * (x2 - x1);
  }
  return Math.max(0, Math.min(1, 1 - 2 * area));
};

const updateGiniIndicator = (giniValue) => {
  const target = document.querySelector('[data-lorenz-gini]');
  if (!target) {
    return;
  }
  target.textContent = `${(giniValue * 100).toFixed(1)}%`;
};

const renderLorenzError = (message) => {
  const container = document.getElementById('lorenz-curve-chart');
  if (container) {
    container.innerHTML = `<p class="chart-error">${message}</p>`;
  }
};

const drawLorenzChart = () => {
  const container = document.getElementById('lorenz-curve-chart');
  if (!container) {
    return;
  }
  lorenzChart = new google.charts.Line(container);
  lorenzChart.draw(
    lorenzDataTable,
    google.charts.Line.convertOptions(lorenzChartOptions),
  );
};

const initializeLorenzChart = async () => {
  const container = document.getElementById('lorenz-curve-chart');
  if (!container || typeof google === 'undefined') {
    return;
  }

  try {
    const records = await loadLorenzRecords();
    const points = computeLorenzPoints(records);
    const gini = calculateGini(points);
    lorenzDataTable = buildLorenzDataTable(points);
    lorenzChartOptions = buildLorenzOptions();

    updateGiniIndicator(gini);
    drawLorenzChart();

    window.addEventListener('resize', () => {
      if (lorenzChart && lorenzDataTable) {
        drawLorenzChart();
      }
    });
  } catch (error) {
    console.error(error);
    renderLorenzError('Lorenz 曲线加载失败，请稍后刷新页面。');
  }
};

google.charts.load('current', { packages: ['line'] });
google.charts.setOnLoadCallback(initializeLorenzChart);




