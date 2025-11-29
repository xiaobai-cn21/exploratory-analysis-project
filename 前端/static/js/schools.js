const DATA_URL = '/static/data/csv/AP_IB_Assessment_2024_level4_School_cleaned.csv';

const numberFormat = new Intl.NumberFormat('en-US');

const filters = {
  year: document.getElementById('schoolYearSelect'),
  county: document.getElementById('countySelect'),
  school: document.getElementById('schoolSelect'),
  program: document.getElementById('programSelect'),
  subject: document.getElementById('subjectSelect'),
  course: document.getElementById('courseSelect'),
  grade: document.getElementById('gradeSelect')
};

const tableBody = document.getElementById('schoolTableBody');
const tableStatus = document.getElementById('tableStatus');
const statTotalRows = document.getElementById('statTotalRows');
const statTested = document.getElementById('statTested');
const statProficient = document.getElementById('statProficient');
const statRate = document.getElementById('statRate');

const applyButton = document.getElementById('applyFilters');
const resetButton = document.getElementById('resetFilters');

let rawRows = [];
let currentRows = [];

function toNumber(value) {
  if (value === undefined || value === null) return 0;
  if (value === '' || value === '-' || value === '--') return 0;
  return Number(value);
}

function parseRow(row) {
  return {
    reportYear: row.REPORT_SCHOOL_YEAR || '',
    aggregationName: row.aggregation_name || row.AGGREGATION_NAME || '',
    schoolId: row.INST_ID,
    schoolName: row.aggregation_name || row.AGGREGATION_NAME || '',
    county: row.COUNTY_NAME || '',
    subgroup: row.SUBGROUP_NAME || 'All Students',
    program: row.APIB_IND || '',
    subject: row.SUBJECT_AREA || '',
    stateCode: row.STATE_CODE || '',
    course: row.ITEM_DESC || '',
    grade: row.GRADE_LEVEL || '',
    tested: toNumber(row.tested_student_cnt),
    proficient: toNumber(row.proficient_student_cnt),
    level1: toNumber(row.level1_cnt),
    level2: toNumber(row.level2_cnt),
    level3: toNumber(row.level3_cnt),
    level4: toNumber(row.level4_cnt),
    level5: toNumber(row.level5_cnt),
    level6: toNumber(row.level6_cnt),
    level7: toNumber(row.level7_cnt)
  };
}

function uniqueValues(data, accessor) {
  return Array.from(new Set(data.map(accessor).filter(Boolean))).sort((a, b) =>
    a.localeCompare(b, undefined, { numeric: true })
  );
}

function populateSelect(selectEl, options, formatter = value => value) {
  if (!selectEl) return;
  selectEl.innerHTML = '';
  const allOption = document.createElement('option');
  allOption.value = 'All';
  allOption.textContent = '全部';
  selectEl.appendChild(allOption);
  options.forEach(option => {
    const opt = document.createElement('option');
    opt.value = option;
    opt.textContent = formatter(option);
    selectEl.appendChild(opt);
  });
}

function initializeFilters(data) {
  populateSelect(filters.year, uniqueValues(data, d => d.reportYear));
  populateSelect(filters.county, uniqueValues(data, d => d.county));
  populateSelect(filters.school, uniqueValues(data, d => d.schoolName));
  populateSelect(filters.program, uniqueValues(data, d => d.program));
  populateSelect(filters.subject, uniqueValues(data, d => d.subject));
  populateSelect(filters.course, uniqueValues(data, d => d.course));
  populateSelect(filters.grade, uniqueValues(data, d => d.grade));
}

function getCheckedValues(selector) {
  return Array.from(document.querySelectorAll(selector))
    .filter(input => input.checked)
    .map(input => input.value);
}

function recordMatchesDemographics(row, selected) {
  if (!selected.length) return true;
  return selected.includes(row.subgroup);
}

function applyFiltersAndRender() {
  if (!rawRows.length) return;

  const selectedGender = getCheckedValues('.demographic.gender');
  const selectedEthnicity = getCheckedValues('.demographic.ethnicity');
  const selectedOther = getCheckedValues('.demographic.other');
  const selectedSubgroups = [...new Set([...selectedGender, ...selectedEthnicity, ...selectedOther])];

  currentRows = rawRows.filter(row => {
    if (filters.year.value !== 'All' && row.reportYear !== filters.year.value) return false;
    if (filters.county.value !== 'All' && row.county !== filters.county.value) return false;
    if (filters.school.value !== 'All' && row.schoolName !== filters.school.value) return false;
    if (filters.program.value !== 'All' && row.program !== filters.program.value) return false;
    if (filters.subject.value !== 'All' && row.subject !== filters.subject.value) return false;
    if (filters.course.value !== 'All' && row.course !== filters.course.value) return false;
    if (filters.grade.value !== 'All' && row.grade !== filters.grade.value) return false;
    if (!recordMatchesDemographics(row, selectedSubgroups)) return false;
    return true;
  });

  updateSummary();
  renderTable();
}

function updateSummary() {
  const totalRows = currentRows.length;
  const totalTested = currentRows.reduce((sum, row) => sum + row.tested, 0);
  const totalProficient = currentRows.reduce((sum, row) => sum + row.proficient, 0);
  const rate = totalTested > 0 ? (totalProficient / totalTested) * 100 : 0;

  statTotalRows.textContent = numberFormat.format(totalRows);
  statTested.textContent = numberFormat.format(totalTested);
  statProficient.textContent = numberFormat.format(totalProficient);
  statRate.textContent = totalTested ? `${rate.toFixed(1)}%` : '—';
  tableStatus.textContent = totalRows ? `显示 ${totalRows} 条记录` : '暂无符合条件的记录';
}

function renderTable() {
  if (!tableBody) return;
  tableBody.innerHTML = '';
  if (!currentRows.length) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = 17;
    td.textContent = '当前筛选下暂无数据。';
    tr.appendChild(td);
    tableBody.appendChild(tr);
    return;
  }

  const MAX_ROWS = 1500;
  const sliced = currentRows.slice(0, MAX_ROWS);

  sliced.forEach(row => {
    const tr = document.createElement('tr');
    tr.innerHTML = `
      <td>${row.schoolName}</td>
      <td>${row.county}</td>
      <td>${row.subgroup}</td>
      <td>${row.program}</td>
      <td>${row.subject}</td>
      <td>${row.course}</td>
      <td>${row.grade}</td>
      <td>${numberFormat.format(row.tested)}</td>
      <td>${numberFormat.format(row.proficient)}</td>
      <td>${row.tested ? ((row.proficient / row.tested) * 100).toFixed(1) + '%' : '—'}</td>
      <td>${row.level1}</td>
      <td>${row.level2}</td>
      <td>${row.level3}</td>
      <td>${row.level4}</td>
      <td>${row.level5}</td>
      <td>${row.level6}</td>
      <td>${row.level7}</td>
    `;
    tableBody.appendChild(tr);
  });

  if (currentRows.length > MAX_ROWS) {
    const tr = document.createElement('tr');
    const td = document.createElement('td');
    td.colSpan = 17;
    td.textContent = `仅显示前 ${MAX_ROWS} 条记录，请使用更多筛选条件以缩小范围。`;
    tr.appendChild(td);
    tableBody.appendChild(tr);
  }
}

function resetAll() {
  Object.values(filters).forEach(select => {
    if (select) select.value = 'All';
  });
  document.querySelectorAll('.demographic').forEach(checkbox => {
    checkbox.checked = false;
  });
  applyFiltersAndRender();
}

function init() {
  if (!tableBody) return;
  tableStatus.textContent = '加载 CSV...';

  d3.csv(DATA_URL, parseRow)
    .then(rows => {
      rawRows = rows;
      initializeFilters(rawRows);
      applyFiltersAndRender();
    })
    .catch(error => {
      console.error(error);
      tableStatus.textContent = '数据加载失败';
      if (tableBody) {
        tableBody.innerHTML = '<tr><td colspan="17">无法加载学校级别 CSV。</td></tr>';
      }
    });

  if (applyButton) {
    applyButton.addEventListener('click', applyFiltersAndRender);
  }
  if (resetButton) {
    resetButton.addEventListener('click', resetAll);
  }
}

init();

