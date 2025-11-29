const regionalComparisonMetrics = {
  nrc: {
    high: { coverage: 82.61, participation: 42.72, proficiency: 56.56 },
    low: { coverage: 92.17, participation: 56.67, proficiency: 74.46 },
  },
  nyc: {
    nyc: { coverage: 56.52, participation: 36.99, proficiency: 57.5 },
    nonNyc: { coverage: 96.52, participation: 63.01, proficiency: 71.87 },
  },
  urbanRural: {
    urban: { coverage: 92.17, participation: 65.65, proficiency: 61.32 },
    rural: { coverage: 84.35, participation: 34.35, proficiency: 76.55 },
  },
};

const regionalSeries = [
  { key: "highNrc", label: "高 N/RC" },
  { key: "lowNrc", label: "低 N/RC" },
  { key: "nyc", label: "NYC 地区" },
  { key: "nonNyc", label: "非 NYC 地区" },
  { key: "urban", label: "城市 / 郊区" },
  { key: "rural", label: "农村区域" },
];

let regionalChart;
let regionalChartData;
let regionalChartOptions;
let resizeTimer;

google.charts.load("current", { packages: ["corechart"] });
google.charts.setOnLoadCallback(initializeRegionalChart);

function initializeRegionalChart() {
  const container = document.getElementById("regional-comparison-chart");
  if (!container || typeof google === "undefined" || !google.visualization) {
    return;
  }

  regionalChartData = buildRegionalDataTable();
  regionalChartOptions = buildRegionalChartOptions();

  regionalChart = new google.visualization.BarChart(container);
  regionalChart.draw(regionalChartData, regionalChartOptions);

  window.addEventListener("resize", () => {
    clearTimeout(resizeTimer);
    resizeTimer = setTimeout(() => {
      regionalChart.draw(regionalChartData, regionalChartOptions);
    }, 180);
  });
}

function buildRegionalDataTable() {
  const data = new google.visualization.DataTable();
  data.addColumn("string", "对比维度");

  regionalSeries.forEach((series) => {
    data.addColumn("number", series.label);
    data.addColumn({ type: "string", role: "tooltip", p: { html: true } });
  });

  const rows = [
    createRow("覆盖率 · N/RC", {
      highNrc: regionalComparisonMetrics.nrc.high.coverage,
      lowNrc: regionalComparisonMetrics.nrc.low.coverage,
    }),
    createRow("参与率 · N/RC", {
      highNrc: regionalComparisonMetrics.nrc.high.participation,
      lowNrc: regionalComparisonMetrics.nrc.low.participation,
    }),
    createRow("达标率 · N/RC", {
      highNrc: regionalComparisonMetrics.nrc.high.proficiency,
      lowNrc: regionalComparisonMetrics.nrc.low.proficiency,
    }),
    createRow("覆盖率 · NYC", {
      nyc: regionalComparisonMetrics.nyc.nyc.coverage,
      nonNyc: regionalComparisonMetrics.nyc.nonNyc.coverage,
    }),
    createRow("参与率 · NYC", {
      nyc: regionalComparisonMetrics.nyc.nyc.participation,
      nonNyc: regionalComparisonMetrics.nyc.nonNyc.participation,
    }),
    createRow("达标率 · NYC", {
      nyc: regionalComparisonMetrics.nyc.nyc.proficiency,
      nonNyc: regionalComparisonMetrics.nyc.nonNyc.proficiency,
    }),
    createRow("覆盖率 · 城乡", {
      urban: regionalComparisonMetrics.urbanRural.urban.coverage,
      rural: regionalComparisonMetrics.urbanRural.rural.coverage,
    }),
    createRow("参与率 · 城乡", {
      urban: regionalComparisonMetrics.urbanRural.urban.participation,
      rural: regionalComparisonMetrics.urbanRural.rural.participation,
    }),
    createRow("达标率 · 城乡", {
      urban: regionalComparisonMetrics.urbanRural.urban.proficiency,
      rural: regionalComparisonMetrics.urbanRural.rural.proficiency,
    }),
  ];

  data.addRows(rows);
  return data;
}

function createRow(label, valueMap) {
  const row = [label];
  regionalSeries.forEach((series) => {
    const value =
      typeof valueMap[series.key] === "number" ? valueMap[series.key] : null;
    if (value === null) {
      row.push(null, null);
      return;
    }
    row.push(value, buildTooltip(label, series.label, value));
  });
  return row;
}

function buildTooltip(metricLabel, seriesLabel, value) {
  return `
    <div class="regional-tooltip">
      <div class="regional-tooltip__title">${metricLabel}</div>
      <div class="regional-tooltip__value">${seriesLabel}：<strong>${value.toFixed(
        1
      )}%</strong></div>
    </div>
  `;
}

function buildRegionalChartOptions() {
  return {
    height: 440,
    backgroundColor: "transparent",
    isStacked: true,
    bar: { groupWidth: "70%" },
    legend: {
      position: "top",
      alignment: "center",
      textStyle: { color: "#cbd5f5", fontSize: 12 },
    },
    hAxis: {
      title: "指标值 (%)",
      minValue: 0,
      textStyle: { color: "#cbd5f5" },
      titleTextStyle: { color: "#f5f7fb", bold: true },
      gridlines: { color: "rgba(226, 232, 240, 0.25)" },
    },
    vAxis: {
      textStyle: { color: "#cbd5f5" },
    },
    colors: [
      "#6d8fb3",
      "#b7c7d9",
      "#6a9f9b",
      "#bfd5cd",
      "#9488b6",
      "#d5cee6",
    ],
    tooltip: { isHtml: true, ignoreBounds: true },
    chartArea: { width: "75%", top: 60, bottom: 40 },
    annotations: {
      textStyle: { fontSize: 12, color: "#f5f7fb" },
    },
  };
}





