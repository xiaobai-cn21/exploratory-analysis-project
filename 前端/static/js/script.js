const chartData = [
  { year: '1975', attacks: 1200, killed: 430, wounded: 800 },
  { year: '1985', attacks: 2400, killed: 690, wounded: 1300 },
  { year: '1995', attacks: 3600, killed: 1100, wounded: 2100 },
  { year: '2005', attacks: 5200, killed: 1800, wounded: 3200 },
  { year: '2015', attacks: 8400, killed: 2600, wounded: 4100 }
];

const mainCategory = document.getElementById('mainCategory');
const subCategory = document.getElementById('subCategory');
const metricSelect = document.getElementById('metric');
const chartLabel = document.getElementById('chartLabel');
const metricLabel = document.getElementById('metricLabel');
const histogramChart = document.getElementById('histogramChart');

const metricFormatter = value => Intl.NumberFormat('en').format(value);

function buildChart(metric = 'attacks') {
  if (!histogramChart) return;
  histogramChart.innerHTML = '';

  const maxValue = Math.max(...chartData.map(item => item[metric]));

  chartData.forEach(item => {
    const bar = document.createElement('div');
    bar.classList.add('bar');
    bar.dataset.year = item.year;
    bar.style.height = `${(item[metric] / maxValue) * 100}%`;

    const value = document.createElement('span');
    value.textContent = metricFormatter(item[metric]);
    bar.appendChild(value);

    bar.title = `${item.year}: ${metricFormatter(item[metric])}`;
    histogramChart.appendChild(bar);
  });
}

function updateLabels() {
  if (!chartLabel || !metricLabel) return;
  chartLabel.textContent = `犯罪轨迹 — ${mainCategory.value}：${subCategory.value}`;
  metricLabel.textContent = metricSelect.options[metricSelect.selectedIndex].text;
}

if (mainCategory && subCategory && metricSelect) {
  mainCategory.addEventListener('change', updateLabels);
  subCategory.addEventListener('change', updateLabels);
  metricSelect.addEventListener('change', () => {
    updateLabels();
    buildChart(metricSelect.value);
  });

  updateLabels();
  buildChart(metricSelect.value);
}

(function initGeoChoropleth() {
  if (typeof d3 === 'undefined' || typeof topojson === 'undefined') return;

  const mapContainer = document.getElementById('nyMap');
  const svgElement = document.getElementById('nyMapSvg');
  const tooltipEl = document.getElementById('nyMapTooltip');
  if (!mapContainer || !svgElement || !tooltipEl) return;

  const dataUrl = mapContainer.dataset.dataUrl;
  if (!dataUrl) return;

  const topojsonUrl = 'https://cdn.jsdelivr.net/npm/us-atlas@3/counties-10m.json';
  const STATE_FIPS = '36';

  const svg = d3.select(svgElement);
  const tooltip = d3.select(tooltipEl);
  const stateSummaryCard = d3.select('#state-summary');
  const countyInfoCard = d3.select('#county-info');

  const programSelect = document.getElementById('programFilter');
  const subjectSelect = document.getElementById('subjectFilter');
  const gradeSelect = document.getElementById('gradeFilter');
  const subgroupSelect = document.getElementById('subgroupFilter');

  const numberFormat = d3.format(',');

  let nyCounties = [];
  let g;
  let projection;
  let pathGenerator;
  let lockedCountyId = null;

  let rawRecords = [];
  let aggregatesByCounty = new Map();
  let currentProgram = 'All';
  let currentSubject = 'All';
  let currentGrade = 'All';
  let currentSubgroup = 'All';
  const countyNameByFips = new Map();

  function getSize() {
    const rect = mapContainer.getBoundingClientRect();
    return { width: Math.max(320, Math.floor(rect.width)), height: Math.max(360, Math.floor(rect.height)) };
  }

  function parseNumeric(value) {
    if (value === undefined || value === null) return 0;
    if (value === '-' || value === '') return 0;
    return +value;
  }

  function parseRow(row) {
    const numericFields = [
      'tested_student_cnt',
      'proficient_student_cnt',
      'level1_cnt',
      'level2_cnt',
      'level3_cnt',
      'level4_cnt',
      'level5_cnt',
      'level6_cnt',
      'level7_cnt'
    ];
    numericFields.forEach(field => {
      row[field] = parseNumeric(row[field]);
    });
    const countyCodeRaw = row.COUNTY_CODE ? String(Math.trunc(Number(row.COUNTY_CODE))) : '';
    const countyCode = countyCodeRaw ? countyCodeRaw.padStart(3, '0') : null;
    return {
      ...row,
      countyFips: countyCode ? `${STATE_FIPS}${countyCode}` : null
    };
  }

  Promise.all([fetch(topojsonUrl).then(r => r.json()), d3.csv(dataUrl, parseRow)])
    .then(([us, rows]) => {
      rawRecords = rows.filter(r => r.countyFips);

      const counties = topojson.feature(us, us.objects.counties).features;
      nyCounties = counties.filter(f => String(f.id).startsWith(STATE_FIPS));
      nyCounties.forEach(f => {
        countyNameByFips.set(String(f.id), f.properties.name || `County ${f.id}`);
      });

      initializeMap();
      initializeFilters();
      recomputeAggregates();
    })
    .catch(err => {
      console.error(err);
      tooltip.text('数据加载失败').attr('hidden', null);
    });

  function initializeMap() {
    const nyFeature = { type: 'FeatureCollection', features: nyCounties };
    const { width, height } = getSize();
    projection = d3.geoMercator().fitSize([width, height], nyFeature);
    pathGenerator = d3.geoPath().projection(projection);

    svg.attr('viewBox', `0 0 ${width} ${height}`).attr('preserveAspectRatio', 'xMidYMid meet');

    g = svg.append('g');

    g.selectAll('path')
      .data(nyCounties)
      .enter()
      .append('path')
      .attr('class', 'county')
      .attr('d', pathGenerator)
      .attr('fill', 'rgba(255,255,255,0.08)')
      .attr('stroke', 'rgba(255,255,255,0.2)')
      .attr('stroke-width', 0.8)
      .attr('data-fips', d => String(d.id))
      .on('mouseenter', handleMouseEnter)
      .on('mouseleave', handleMouseLeave)
      .on('click', handleCountyClick);

    window.addEventListener('resize', () => {
      const size = getSize();
      const proj = d3.geoMercator().fitSize([size.width, size.height], nyFeature);
      projection = proj;
      pathGenerator = d3.geoPath().projection(proj);
      svg.attr('viewBox', `0 0 ${size.width} ${size.height}`);
      g.selectAll('path').attr('d', pathGenerator);
    });
  }

  function initializeFilters() {
    const programs = Array.from(new Set(rawRecords.map(r => r.APIB_IND).filter(Boolean))).sort();
    populateSelect(programSelect, ['All', ...programs]);

    const subjects = Array.from(new Set(rawRecords.map(r => r.SUBJECT_AREA).filter(Boolean))).sort();
    populateSelect(subjectSelect, ['All', ...subjects]);

    const grades = Array.from(new Set(rawRecords.map(r => r.GRADE_LEVEL).filter(Boolean))).sort((a, b) =>
      a.localeCompare(b, undefined, { numeric: true })
    );
    populateSelect(gradeSelect, ['All', ...grades]);

    const subgroups = Array.from(new Set(rawRecords.map(r => r.SUBGROUP_NAME).filter(Boolean))).sort();
    populateSelect(subgroupSelect, ['All', ...subgroups]);

    programSelect.addEventListener('change', event => {
      currentProgram = event.target.value;
      recomputeAggregates();
    });

    subjectSelect.addEventListener('change', event => {
      currentSubject = event.target.value;
      recomputeAggregates();
    });

    gradeSelect.addEventListener('change', event => {
      currentGrade = event.target.value;
      recomputeAggregates();
    });

    subgroupSelect.addEventListener('change', event => {
      currentSubgroup = event.target.value;
      recomputeAggregates();
    });
  }

  function populateSelect(selectEl, options) {
    if (!selectEl) return;
    selectEl.innerHTML = '';
    options.forEach(option => {
      const opt = document.createElement('option');
      opt.value = option;
      opt.textContent = option === 'All' ? '全部' : option;
      selectEl.appendChild(opt);
    });
  }

  function recomputeAggregates() {
    aggregatesByCounty = new Map();

    rawRecords.forEach(record => {
      if (currentProgram !== 'All' && record.APIB_IND !== currentProgram) return;
      if (currentSubject !== 'All' && record.SUBJECT_AREA !== currentSubject) return;
      if (currentGrade !== 'All' && record.GRADE_LEVEL !== currentGrade) return;
      if (currentSubgroup !== 'All' && record.SUBGROUP_NAME !== currentSubgroup) return;
      const countyId = record.countyFips;
      if (!countyId) return;
      if (!aggregatesByCounty.has(countyId)) {
        aggregatesByCounty.set(countyId, {
          tested: 0,
          proficient: 0,
          rows: 0,
          subjectTotals: new Map()
        });
      }
      const stats = aggregatesByCounty.get(countyId);
      stats.tested += record.tested_student_cnt;
      stats.proficient += record.proficient_student_cnt;
      stats.rows += 1;
      const subjKey = record.SUBJECT_AREA || 'Other';
      stats.subjectTotals.set(subjKey, (stats.subjectTotals.get(subjKey) || 0) + record.tested_student_cnt);
    });

    colorizeCounties();
    updateStatewideSummary();

    if (lockedCountyId) {
      if (aggregatesByCounty.has(lockedCountyId)) {
        renderCountyInfo(lockedCountyId);
      } else {
        g.selectAll('path')
          .filter(d => String(d.id) === lockedCountyId)
          .attr('fill', 'rgba(255,255,255,0.08)');
        lockedCountyId = null;
        renderCountyInfo(null);
      }
    } else {
      renderCountyInfo(null);
    }
  }

  function colorizeCounties() {
    const maxTested = Math.max(0, ...Array.from(aggregatesByCounty.values()).map(stats => stats.tested));
    const colorScale = d3
      .scaleSequential()
      .domain([0, maxTested || 1])
      .interpolator(d3.interpolateBlues);

    g.selectAll('path').attr('fill', d => {
      const stats = aggregatesByCounty.get(String(d.id));
      if (!stats || stats.tested === 0) {
        return 'rgba(255,255,255,0.08)';
      }
      return colorScale(stats.tested);
    });
  }

  function updateStatewideSummary() {
    const totals = Array.from(aggregatesByCounty.values()).reduce(
      (acc, stats) => {
        acc.tested += stats.tested;
        acc.proficient += stats.proficient;
        return acc;
      },
      { tested: 0, proficient: 0 }
    );

    const proficiencyRate = totals.tested > 0 ? ((totals.proficient / totals.tested) * 100).toFixed(1) : '—';
    const reportingCounties = aggregatesByCounty.size;

    stateSummaryCard.html(`
      <h5>全州概览</h5>
      <div class="geo-stats-grid">
        <div><span>测试人数</span><strong>${totals.tested ? numberFormat(Math.round(totals.tested)) : '—'}</strong></div>
        <div><span>合格人数</span><strong>${totals.proficient ? numberFormat(Math.round(totals.proficient)) : '—'}</strong></div>
        <div><span>合格率</span><strong>${proficiencyRate === '—' ? '—' : `${proficiencyRate}%`}</strong></div>
        <div><span>有数据县</span><strong>${reportingCounties}</strong></div>
      </div>
      <p class="geo-note">筛选条件同时作用于热力图与详情卡片。</p>
    `);
  }

  function handleMouseEnter(event, d) {
    const countyId = String(d.id);
    if (lockedCountyId !== countyId) {
      d3.select(event.currentTarget).attr('stroke', '#fff').attr('stroke-width', 1.2);
    }
    showTooltip(event, countyId);
  }

  function handleMouseLeave(event, d) {
    const countyId = String(d.id);
    if (lockedCountyId !== countyId) {
      d3.select(event.currentTarget).attr('stroke', 'rgba(255,255,255,0.2)').attr('stroke-width', 0.8);
    }
    if (!lockedCountyId) {
      tooltip.attr('hidden', '');
    }
  }

  function handleCountyClick(event, d) {
    const countyId = String(d.id);
    if (lockedCountyId === countyId) {
      lockedCountyId = null;
      d3.select(event.currentTarget)
        .attr('stroke', 'rgba(255,255,255,0.2)')
        .attr('stroke-width', 0.8);
      tooltip.attr('hidden', '');
      renderCountyInfo(null);
    } else {
      lockedCountyId = countyId;
      g.selectAll('path')
        .attr('stroke', 'rgba(255,255,255,0.2)')
        .attr('stroke-width', 0.8);
      d3.select(event.currentTarget).attr('stroke', '#fff').attr('stroke-width', 1.6);
      showTooltip(event, countyId);
      renderCountyInfo(countyId);
    }
  }

  function showTooltip(event, countyId) {
    const stats = aggregatesByCounty.get(countyId);
    const countyName = countyNameByFips.get(countyId) || 'County';
    let html = `<strong>${countyName}</strong><br/>`;
    if (stats && stats.tested > 0) {
      const rate = stats.tested ? ((stats.proficient / stats.tested) * 100).toFixed(1) : '0.0';
      html += `测试人数：${numberFormat(Math.round(stats.tested))}<br/>合格人数：${numberFormat(
        Math.round(stats.proficient)
      )}<br/>合格率：${rate}%`;
    } else {
      html += '当前筛选下暂无数据';
    }
    const [mx, my] = d3.pointer(event);
    tooltip
      .attr('hidden', null)
      .style('left', `${mx + 12}px`)
      .style('top', `${my + 12}px`)
      .html(html);
  }

  function renderCountyInfo(countyId) {
    if (!countyId) {
      countyInfoCard.attr('class', 'geo-card geo-card--empty').html('<p>点击任意县以固定其详细信息。</p>');
      return;
    }
    const stats = aggregatesByCounty.get(countyId);
    const countyName = countyNameByFips.get(countyId) || 'County';
    if (!stats || stats.tested === 0) {
      countyInfoCard
        .attr('class', 'geo-card')
        .html(`<h5>${countyName}</h5><p>当前筛选条件下暂无数据。</p>`);
      return;
    }
    const proficiency = stats.tested ? ((stats.proficient / stats.tested) * 100).toFixed(1) : '0.0';
    const topSubjects = Array.from(stats.subjectTotals.entries())
      .filter(([, tested]) => tested > 0)
      .sort((a, b) => b[1] - a[1])
      .slice(0, 3);
    const subjectsHtml = topSubjects.length
      ? `<div class="geo-top-subjects"><strong>热门科目（按测试人数）</strong><ol>${topSubjects
          .map(([name, value]) => `<li>${name}：${numberFormat(Math.round(value))}</li>`)
          .join('')}</ol></div>`
      : '';
    countyInfoCard
      .attr('class', 'geo-card')
      .html(`
        <h5>${countyName}</h5>
        <div class="geo-stats-grid">
          <div><span>测试人数</span><strong>${numberFormat(Math.round(stats.tested))}</strong></div>
          <div><span>合格人数</span><strong>${numberFormat(Math.round(stats.proficient))}</strong></div>
          <div><span>合格率</span><strong>${proficiency}%</strong></div>
          <div><span>记录数</span><strong>${stats.rows}</strong></div>
        </div>
        ${subjectsHtml}
      `);
  }
})();

(function initShowcaseNyMap() {
  if (typeof d3 === 'undefined' || typeof topojson === 'undefined') return;

  const wrapper = document.getElementById('nycChoropleth');
  const mapContainer = document.getElementById('nycMap');
  const svgNode = document.getElementById('nycMapSvg');
  const tooltipNode = document.getElementById('nycMapTooltip');
  const zoomInButton = document.getElementById('nycZoomIn');
  const zoomOutButton = document.getElementById('nycZoomOut');
  const programFilter = document.getElementById('nycProgramFilter');
  const subjectFilter = document.getElementById('nycSubjectFilter');
  const subgroupFilter = document.getElementById('nycSubgroupFilter');
  const gradeFilter = document.getElementById('nycGradeFilter');

  if (!wrapper || !mapContainer || !svgNode || !tooltipNode) return;

  // 使用本地 TopoJSON 文件
  const topojsonUrl = '/static/data/json/new_york_counties.json';
  const dataUrl = '/static/data/csv/AP_IB_Assessment_2024_level2_County_cleaned.csv';
  const STATE_FIPS = '36';

  const svg = d3.select(svgNode);
  const tooltip = d3.select(tooltipNode);
  const numberFormat = d3.format(',');

  let nyCounties = [];
  let projection;
  let pathGenerator;
  let g;
  let colorScale;
  let lockedCounty = null;
  const valueByCounty = new Map();
  const countyStats = new Map();
  let currentTransform = d3.zoomIdentity;
  let zoomControlsAttached = false;
  let rawStudentRows = [];
  let currentProgram = 'All';
  let currentSubject = 'All';
  let currentSubgroup = 'All';
  let currentGrade = 'All';

  const zoomBehavior = d3
    .zoom()
    .scaleExtent([1, 6])
    .on('zoom', event => {
      currentTransform = event.transform;
      if (g) {
        g.attr('transform', currentTransform);
      }
    });

  function getSize() {
    const rect = mapContainer.getBoundingClientRect();
    return {
      width: Math.max(360, Math.floor(rect.width)),
      height: Math.max(360, Math.floor(rect.height))
    };
  }

  function parseStudentRow(row) {
    const toNumber = value => {
      if (value === undefined || value === null || value === '' || value === '-' || value === '--') {
        return 0;
      }
      const num = Number(value);
      return isNaN(num) ? 0 : num;
    };
    
    // 处理 COUNTY_CODE，支持整数和浮点数格式
    let countyCodeRaw = '';
    if (row.COUNTY_CODE !== undefined && row.COUNTY_CODE !== null && row.COUNTY_CODE !== '') {
      const codeNum = Number(row.COUNTY_CODE);
      if (!isNaN(codeNum)) {
        countyCodeRaw = String(Math.trunc(codeNum));
      }
    }
    const countyFips = countyCodeRaw ? `${STATE_FIPS}${countyCodeRaw.padStart(3, '0')}` : null;
    
    return {
      ...row,
      countyFips,
      tested_student_cnt: toNumber(row.tested_student_cnt),
      proficient_student_cnt: toNumber(row.proficient_student_cnt)
    };
  }

  function aggregateCountyStats(rows) {
    countyStats.clear();
    valueByCounty.clear();

    rows.forEach(row => {
      if (!row.countyFips) return;
      const stats = countyStats.get(row.countyFips) || {
        tested: 0,
        proficient: 0
      };
      stats.tested += row.tested_student_cnt || 0;
      stats.proficient += row.proficient_student_cnt || 0;
      countyStats.set(row.countyFips, stats);
    });

    countyStats.forEach((stats, countyId) => {
      const rate = stats.tested > 0 ? stats.proficient / stats.tested : 0;
      valueByCounty.set(countyId, rate);
    });
  }

  function updateColorScale() {
    colorScale = () => '#4e8fd5';
  }

  function updatePaths() {
    if (!nyCounties.length) return;
    const featureCollection = { type: 'FeatureCollection', features: nyCounties };
    const { width, height } = getSize();
    projection = d3.geoMercator().fitSize([width, height], featureCollection);
    pathGenerator = d3.geoPath().projection(projection);

    svg.attr('viewBox', `0 0 ${width} ${height}`).attr('preserveAspectRatio', 'xMidYMid meet');

    if (!g) {
      g = svg.append('g');
    }

    zoomBehavior.translateExtent([
      [0, 0],
      [width, height]
    ]);
    zoomBehavior.extent([
      [0, 0],
      [width, height]
    ]);

    g.selectAll('path')
      .data(nyCounties, d => d.id || d.properties?.GEOID)
      .join('path')
      .attr('class', 'county')
      .attr('d', pathGenerator)
      .attr('data-fips', d => String(d.id || d.properties?.GEOID || ''))
      .attr('fill', d => getCountyColor(String(d.id || d.properties?.GEOID || '')))
      .attr('stroke', '#fff')
      .attr('stroke-width', 0.6)
      .on('mousemove', handleMouseMove)
      .on('mouseleave', handleMouseLeave)
      .on('click', handleCountyClick);

    g.attr('transform', currentTransform);
    svg.call(zoomBehavior).on('dblclick.zoom', null);
    attachZoomControls();
  }

  function getCountyColor(countyId) {
    if (colorScale) {
      return colorScale();
    }
    return '#4e8fd5';
  }

  function handleMouseMove(event, feature) {
    const countyId = String(feature.id || feature.properties?.GEOID || '');
    if (!lockedCounty || lockedCounty === countyId) {
      showTooltip(event, feature);
    }
    if (lockedCounty !== countyId) {
      d3.select(event.currentTarget).attr('fill', '#1f4fd8');
    }
  }

  function handleMouseLeave(event) {
    if (!lockedCounty) {
      tooltip.attr('hidden', '');
    }
    const countyId = event.currentTarget.dataset.fips;
    if (lockedCounty === countyId) return;
    d3.select(event.currentTarget).attr('fill', getCountyColor(countyId));
  }

  function handleCountyClick(event, feature) {
    const countyId = String(feature.id || feature.properties?.GEOID || '');
    if (lockedCounty === countyId) {
      lockedCounty = null;
      tooltip.attr('hidden', '');
      d3.select(event.currentTarget).attr('fill', getCountyColor(countyId));
    } else {
      lockedCounty = countyId;
      showTooltip(event, feature);
      d3.select(event.currentTarget).attr('fill', '#0c2ca4');
    }
  }

  function zoomStep(factor) {
    svg.transition().duration(250).call(zoomBehavior.scaleBy, factor);
  }

  function attachZoomControls() {
    if (zoomControlsAttached) return;
    if (!zoomInButton || !zoomOutButton) return;
    zoomInButton.addEventListener('click', () => zoomStep(1.2));
    zoomOutButton.addEventListener('click', () => zoomStep(1 / 1.2));
    zoomControlsAttached = true;
  }

  function showTooltip(event, feature) {
    const countyId = String(feature.id || feature.properties?.GEOID || '');
    const stats = countyStats.get(countyId);
    const name = feature.properties?.NAME || feature.properties?.name || `County ${countyId}`;
    const [x, y] = d3.pointer(event, mapContainer);
    let html = `<strong>${name}</strong>`;
    if (stats && stats.tested > 0) {
      const rate = stats.proficient / stats.tested;
      html += `<br/>通过人数：${numberFormat(stats.proficient)} / ${numberFormat(stats.tested)}`;
      html += `<br/>通过率：${(rate * 100).toFixed(1)}%`;
    } else {
      html += '<br/>当前没有考试数据';
    }
    tooltip.attr('hidden', null).style('left', `${x + 12}px`).style('top', `${y + 12}px`).html(html);
  }

  function applyFilters() {
    if (!rawStudentRows.length) return;
    const filtered = rawStudentRows.filter(row => {
      if (!row.countyFips) return false;
      if (currentProgram !== 'All' && row.APIB_IND !== currentProgram) return false;
      if (currentSubject !== 'All' && row.SUBJECT_AREA !== currentSubject) return false;
      if (currentSubgroup !== 'All' && row.SUBGROUP_NAME !== currentSubgroup) return false;
      if (currentGrade !== 'All' && row.GRADE_LEVEL !== currentGrade) return false;
      return true;
    });
    aggregateCountyStats(filtered);
    updateColorScale();
    updatePaths();
  }

  function populateSelect(selectEl, options, formatter = value => value) {
    if (!selectEl) return;
    selectEl.innerHTML = '';
    options.forEach(option => {
      const opt = document.createElement('option');
      opt.value = option;
      opt.textContent = formatter(option);
      selectEl.appendChild(opt);
    });
  }

  function initializeFilters(rows) {
    const programs = Array.from(new Set(rows.map(row => row.APIB_IND).filter(Boolean))).sort();
    const subjects = Array.from(new Set(rows.map(row => row.SUBJECT_AREA).filter(Boolean))).sort();
    const subgroups = Array.from(
      new Set(rows.map(row => row.SUBGROUP_NAME || 'All Students').filter(Boolean))
    ).sort();
    const grades = Array.from(new Set(rows.map(row => row.GRADE_LEVEL).filter(Boolean))).sort(
      (a, b) => a.localeCompare(b, undefined, { numeric: true })
    );

    populateSelect(
      programFilter,
      ['All', ...programs],
      value => (value === 'All' ? '全部项目' : value)
    );
    populateSelect(
      subjectFilter,
      ['All', ...subjects],
      value => (value === 'All' ? '全部科目' : value)
    );
    populateSelect(
      subgroupFilter,
      ['All', ...subgroups],
      value => (value === 'All' ? '全部学生' : value)
    );
    populateSelect(
      gradeFilter,
      ['All', ...grades],
      value => (value === 'All' ? '全部年级' : value)
    );
    if (programFilter) {
      programFilter.addEventListener('change', event => {
        currentProgram = event.target.value;
        applyFilters();
      });
    }
    if (subjectFilter) {
      subjectFilter.addEventListener('change', event => {
        currentSubject = event.target.value;
        applyFilters();
      });
    }
    if (subgroupFilter) {
      subgroupFilter.addEventListener('change', event => {
        currentSubgroup = event.target.value;
        applyFilters();
      });
    }
    if (gradeFilter) {
      gradeFilter.addEventListener('change', event => {
        currentGrade = event.target.value;
        applyFilters();
      });
    }
  }

  Promise.all([fetch(topojsonUrl).then(r => r.json()), d3.csv(dataUrl, parseStudentRow)])
    .then(([topology, rows]) => {
      // 使用本地 TopoJSON 文件的对象名
      const countiesObject = topology.objects.cb_2015_new_york_county_20m;
      if (!countiesObject) {
        throw new Error('TopoJSON 文件中找不到县数据对象');
      }
      const counties = topojson.feature(topology, countiesObject).features;
      nyCounties = counties.filter(f => {
        const fips = String(f.properties.GEOID || f.id || '');
        return fips.startsWith(STATE_FIPS);
      });
      
      // 确保每个县都有正确的 ID
      nyCounties.forEach(f => {
        if (!f.id && f.properties.GEOID) {
          f.id = f.properties.GEOID;
        }
      });
      
      if (nyCounties.length === 0) {
        throw new Error('未找到纽约县的几何数据');
      }
      
      if (!rows || rows.length === 0) {
        throw new Error('CSV 文件为空或无法解析');
      }
      
      rawStudentRows = rows;
      console.log(`成功加载 ${rows.length} 条 CSV 记录和 ${nyCounties.length} 个县`);
      initializeFilters(rows);
      applyFilters();
      window.addEventListener('resize', updatePaths);

      if (window.ResizeObserver) {
        const mapResizeObserver = new ResizeObserver(entries => {
          if (!entries.length) return;
          updatePaths();
        });
        mapResizeObserver.observe(mapContainer);
      }
    })
    .catch(error => {
      console.error('数据加载错误:', error);
      const errorMsg = error.message || '地图数据加载失败';
      tooltip
        .attr('hidden', null)
        .text(errorMsg)
        .style('left', '12px')
        .style('top', '12px');
      console.error('详细错误信息:', error);
    });
})();