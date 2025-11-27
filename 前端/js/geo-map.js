const DATA_URL = '../../cleaned_data/county_coverage.json';
const MAP_URL = '../../cleaned_data/ny-counties.geojson';

let mapChart;

const renderMapError = (message) => {
  const container = document.getElementById('ny-coverage-map');
  if (container) {
    container.innerHTML = `<p class="map-error">${message}</p>`;
  }
};

const normalizeCountyName = (name = '') => name
  .toUpperCase()
  .replace(/\s+COUNTY$/, '')
  .replace(/\s+/g, ' ')
  .trim();

const fetchJson = async (url) => {
  const response = await fetch(url);
  if (!response.ok) {
    throw new Error(`Failed to load ${url} (${response.status})`);
  }
  return response.json();
};

const buildSeriesData = (records, geoJson) => {
  const byCode = new Map(records.map((row) => [row.county_code.padStart(3, '0'), row]));
  const byName = new Map(records.map((row) => [normalizeCountyName(row.county_name), row]));

  return geoJson.features.map((feature) => {
    const geoid = feature.properties?.geoid || '';
    const countyCode = geoid.slice(-3);
    const fallbackName = normalizeCountyName(feature.properties?.name || '');
    const record = byCode.get(countyCode) || byName.get(fallbackName);

    if (!record) {
      return {
        name: feature.properties?.name || 'Unknown',
        value: null,
      };
    }

    return {
      name: feature.properties?.name || record.county_name,
      value: record.coverage_score,
      countyName: record.county_name,
      tested: record.tested,
      courseCount: record.course_count,
    };
  });
};

const buildMapOptions = (seriesData) => ({
  tooltip: {
    trigger: 'item',
    backgroundColor: '#0f172a',
    borderColor: '#334155',
    textStyle: { color: '#e2e8f0' },
    formatter: (params) => {
      const data = params.data;
      if (!data || data.value === null || data.value === undefined) {
        return `${params.name}<br/>暂无数据`;
      }
      return `
        <div class="geo-tooltip">
          <div><strong>${data.countyName || params.name}</strong></div>
          <div>覆盖度：${(data.value * 100).toFixed(1)}%</div>
          <div>测试人数：${data.tested.toLocaleString('en-US')}</div>
          <div>AP/IB课程数：${data.courseCount}</div>
        </div>
      `;
    },
  },
  visualMap: {
    min: 0,
    max: 1,
    text: ['高覆盖', '低覆盖'],
    realtime: false,
    calculable: true,
    left: 'right',
    bottom: 20,
    textStyle: {
      color: '#e2e8f0',
      fontSize: 12,
      fontWeight: 500,
    },
    inRange: {
      color: ['#d4ecff', '#0057b7'],
    },
    formatter: (value) => `${Math.round(value * 100)}%`,
  },
  series: [
    {
      type: 'map',
      map: 'ny-counties',
      roam: true,
      itemStyle: {
        borderColor: '#ffffff',
        borderWidth: 0.5,
      },
      emphasis: {
        label: { show: false },
        itemStyle: {
          borderColor: '#0f172a',
          borderWidth: 1.2,
        },
      },
      data: seriesData,
    },
  ],
});

const drawCoverageMap = async () => {
  const container = document.getElementById('ny-coverage-map');
  if (!container) {
    return;
  }

  if (typeof echarts === 'undefined') {
    renderMapError('未加载 ECharts，无法绘制地图。');
    return;
  }

  try {
    const [records, geoJson] = await Promise.all([
      fetchJson(DATA_URL),
      fetchJson(MAP_URL),
    ]);

    echarts.registerMap('ny-counties', geoJson);
    const seriesData = buildSeriesData(records, geoJson);

    mapChart = echarts.init(container);
    mapChart.setOption(buildMapOptions(seriesData));

    window.addEventListener('resize', () => {
      if (mapChart) {
        mapChart.resize();
      }
    });
  } catch (error) {
    console.error(error);
    renderMapError('无法加载地理图，请稍后重试。');
  }
};

if (document.readyState === 'loading') {
  document.addEventListener('DOMContentLoaded', drawCoverageMap);
} else {
  drawCoverageMap();
}


