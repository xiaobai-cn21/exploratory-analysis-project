const DATA_URL = "/static/data/fairness/chart_specs.json";

const palette = {
  ap: "#0d2e5f",
  ib: "#7b1111",
  accent: "#1f7a8c",
  warning: "#c26542",
  neutral: "#5c6270"
};

const statMap = {
  "ap-tested": document.querySelector('[data-equity-field="ap-tested"]'),
  "ap-valid": document.querySelector('[data-equity-field="ap-valid"]'),
  "ap-proficiency": document.querySelector('[data-equity-field="ap-proficiency"]'),
  "ap-gap": document.querySelector('[data-equity-field="ap-gap"]'),
  "ap-highscore": document.querySelector('[data-equity-field="ap-highscore"]'),
  "ib-tested": document.querySelector('[data-equity-field="ib-tested"]'),
  "ib-valid": document.querySelector('[data-equity-field="ib-valid"]'),
  "ib-proficiency": document.querySelector('[data-equity-field="ib-proficiency"]'),
  "ib-gap": document.querySelector('[data-equity-field="ib-gap"]'),
  "ib-highscore": document.querySelector('[data-equity-field="ib-highscore"]')
};

const fmtNumber = value => (typeof value === "number" ? value.toLocaleString("en-US") : "—");
const fmtPercent = (value, digits = 1) =>
  typeof value === "number" ? `${(value * 100).toFixed(digits)}%` : "—";

const siteLayout = (extra = {}) => ({
  paper_bgcolor: "#ffffff",
  plot_bgcolor: "#ffffff",
  font: { color: "#0c0c0c" },
  margin: { t: 40, r: 20, b: 40, l: 60 },
  ...extra
});

const ensurePlotly = (containerId) => {
  if (window.Plotly) return true;
  const container = document.getElementById(containerId);
  if (container) {
    container.innerHTML = "<p class=\"plot-note\">Plotly 未加载，暂无法渲染。</p>";
  }
  return false;
};

const setStat = (key, value) => {
  const node = statMap[key];
  if (node) node.textContent = value;
};

const renderStats = (rows = []) => {
  const mapped = rows.reduce((acc, row) => {
    acc[row.APIB_IND] = row;
    return acc;
  }, {});

  const ap = mapped.AP;
  const ib = mapped.IB;

  if (ap) {
    setStat("ap-tested", fmtNumber(ap.tested_total));
    setStat("ap-valid", `有效覆盖 ${fmtPercent(ap.valid_coverage)}`);
    setStat("ap-proficiency", fmtPercent(ap.proficiency_rate));
    setStat("ap-gap", `抑制率 ${fmtPercent(ap.suppressed_share)}`);
    setStat("ap-highscore", fmtPercent(ap.high_score_rate));
  }

  if (ib) {
    setStat("ib-tested", fmtNumber(ib.tested_total));
    setStat("ib-valid", `有效覆盖 ${fmtPercent(ib.valid_coverage)}`);
    setStat("ib-proficiency", fmtPercent(ib.proficiency_rate));
    setStat("ib-gap", `抑制率 ${fmtPercent(ib.suppressed_share)}`);
    setStat("ib-highscore", fmtPercent(ib.high_score_rate));
  }
};

const renderState = rows => {
  if (!ensurePlotly("equityState")) return;
  const programs = rows.reduce((acc, row) => {
    acc[row.APIB_IND] = row;
    return acc;
  }, {});

  const categories = ["测试人数(千)", "达标率(%)", "高分率(%)", "抑制率(%)"];
  const traces = ["AP", "IB"]
    .filter(program => programs[program])
    .map(program => {
      const item = programs[program];
      return {
        x: categories,
        y: [
          item.tested_total / 1000,
          item.proficiency_rate * 100,
          item.high_score_rate * 100,
          (item.suppressed_share ?? 0) * 100
        ],
        type: "bar",
        name: program,
        marker: { color: program === "AP" ? palette.ap : palette.ib }
      };
    });

  Plotly.newPlot(
    "equityState",
    traces,
    siteLayout({
      barmode: "group",
      yaxis: { title: "数值 (千 / %)", gridcolor: "rgba(0,0,0,0.08)", zerolinecolor: "rgba(0,0,0,0.08)" },
      legend: { orientation: "h", y: -0.25 }
    }),
    { displayModeBar: false, responsive: true }
  );
};

const renderLevel = rows => {
  if (!ensurePlotly("equityLevel")) return;
  const order = ["level1_cnt", "level2_cnt", "level3_cnt", "level4_cnt", "level5_cnt", "level6_cnt", "level7_cnt"];
  const grouped = rows.reduce((acc, row) => {
    if (!acc[row.APIB_IND]) acc[row.APIB_IND] = {};
    acc[row.APIB_IND][row.level] = row.share * 100;
    return acc;
  }, {});

  const friendly = name => `Level ${name.replace("level", "").replace("_cnt", "")}`;

  const traces = ["AP", "IB"]
    .filter(program => grouped[program])
    .map(program => ({
      x: order.map(friendly),
      y: order.map(level => grouped[program][level] ?? 0),
      type: "bar",
      name: program,
      marker: { color: program === "AP" ? palette.ap : palette.ib }
    }));

  Plotly.newPlot(
    "equityLevel",
    traces,
    siteLayout({
      barmode: "stack",
      yaxis: { title: "占比 (%)", range: [0, 100], gridcolor: "rgba(0,0,0,0.08)" },
      legend: { orientation: "h", y: -0.25 }
    }),
    { displayModeBar: false, responsive: true }
  );
};

const renderDemographic = rows => {
  if (!ensurePlotly("equityDemo")) return;
  const sorted = [...rows].sort((a, b) => b.tested_student_cnt - a.tested_student_cnt).slice(0, 40);
  const traces = ["AP", "IB"]
    .map(program => {
      const subset = sorted.filter(row => row.APIB_IND === program);
      if (!subset.length) return null;
      return {
        type: "scatter",
        mode: "markers",
        name: program,
        x: subset.map(row => row.participation_share * 100),
        y: subset.map(row => row.proficiency_rate * 100),
        text: subset.map(row => `${row.SUBGROUP_NAME} · ${program}<br>测试人数：${fmtNumber(row.tested_student_cnt)}`),
        hovertemplate: "%{text}<br>参与份额：%{x:.1f}%<br>达标率：%{y:.1f}%<extra></extra>",
        marker: {
          size: subset.map(row => Math.max(8, Math.sqrt(row.tested_student_cnt) * 0.15)),
          color: program === "AP" ? palette.ap : palette.ib,
          opacity: 0.85,
          line: { width: 1, color: "#0b0f17" }
        }
      };
    })
    .filter(Boolean);

  Plotly.newPlot(
    "equityDemo",
    traces,
    siteLayout({
      xaxis: { title: "参与份额 (%)", gridcolor: "rgba(0,0,0,0.08)" },
      yaxis: { title: "达标率 (%)", gridcolor: "rgba(0,0,0,0.08)" },
      legend: { orientation: "h", y: -0.25 }
    }),
    { displayModeBar: false, responsive: true }
  );
};

const renderGap = gap => {
  if (!ensurePlotly("equityGap")) return;
  const positives = gap?.positive?.slice(0, 5) ?? [];
  const negatives = gap?.negative?.slice(0, 5) ?? [];
  const combined = [...positives, ...negatives];
  if (!combined.length) {
    document.getElementById("equityGap").innerHTML = "<p class=\"plot-note\">暂无 gap 数据</p>";
    return;
  }

  const labels = combined.map(item => `${item.APIB_IND} · ${item.SUBGROUP_NAME}`);
  const values = combined.map(item => item.gap_vs_all * 100);

  Plotly.newPlot(
    "equityGap",
    [
      {
        type: "bar",
        orientation: "h",
        y: labels,
        x: values,
        marker: { color: combined.map(item => (item.gap_vs_all >= 0 ? palette.ap : palette.ib)) },
        hovertemplate: "%{y}<br>差距：%{x:.1f}pp<extra></extra>"
      }
    ],
    siteLayout({
      margin: { t: 40, r: 40, b: 30, l: 160 },
      xaxis: { title: "差距 (pp)", zerolinecolor: "rgba(0,0,0,0.1)" }
    }),
    { displayModeBar: false, responsive: true }
  );
};

const renderNrc = rows => {
  if (!ensurePlotly("equityNrc")) return;
  const categories = Array.from(new Set(rows.slice().sort((a, b) => a.NRC_CODE - b.NRC_CODE).map(row => row.NRC_DESC)));
  const grouped = rows.reduce((acc, row) => {
    if (!acc[row.APIB_IND]) acc[row.APIB_IND] = {};
    acc[row.APIB_IND][row.NRC_DESC] = row.proficiency_rate * 100;
    return acc;
  }, {});

  const traces = ["AP", "IB"]
    .filter(program => grouped[program])
    .map(program => ({
      type: "scatterpolar",
      r: categories.map(cat => grouped[program][cat] ?? 0),
      theta: categories,
      fill: "toself",
      fillcolor: program === "AP" ? "rgba(88,166,255,0.15)" : "rgba(255,166,87,0.15)",
      name: program,
      line: { color: program === "AP" ? palette.ap : palette.ib, width: 2.4 }
    }));

  Plotly.newPlot(
    "equityNrc",
    traces,
    siteLayout({
      margin: { t: 30, r: 30, b: 30, l: 30 },
      polar: {
        bgcolor: "#ffffff",
        radialaxis: { range: [0, 100], tickfont: { color: "#5c6270" }, gridcolor: "rgba(0,0,0,0.1)" },
        angularaxis: { tickfont: { color: "#5c6270" }, gridcolor: "rgba(0,0,0,0.05)" }
      },
      legend: { orientation: "h", y: -0.2 }
    }),
    { displayModeBar: false, responsive: true }
  );
};

const buildCountyColumn = (title, entries = []) => {
  const rows = entries
    ?.slice(0, 5)
    ?.map(entry => `<li><span>${entry.COUNTY_NAME}</span><span>${fmtPercent(entry.proficiency_rate)}</span></li>`)
    ?.join("") ?? "<li>暂无数据</li>";
  return `<div class="county-column"><h4>${title}</h4><ul>${rows}</ul></div>`;
};

const renderCounty = league => {
  const container = document.getElementById("equityCounty");
  if (!container) return;
  container.innerHTML = `
    <div class="county-league">
      ${buildCountyColumn("AP · Top", league?.ap?.top)}
      ${buildCountyColumn("AP · Bottom", league?.ap?.bottom)}
      ${buildCountyColumn("IB · Top", league?.ib?.top)}
      ${buildCountyColumn("IB · Bottom", league?.ib?.bottom)}
    </div>
  `;
};

const renderSubjectHeat = rows => {
  if (!ensurePlotly("equitySubject")) return;
  const subjects = Array.from(new Set(rows.map(row => row.SUBJECT_AREA))).sort();
  const programs = ["AP", "IB"];
  const matrix = subjects.map(subject =>
    programs.map(program => {
      const match = rows.find(row => row.SUBJECT_AREA === subject && row.APIB_IND === program);
      return match ? match.proficiency_rate * 100 : null;
    })
  );

  Plotly.newPlot(
    "equitySubject",
    [
      {
        type: "heatmap",
        z: matrix,
        x: programs,
        y: subjects,
        colorscale: [
          [0, "#eef3ff"],
          [0.5, "#8ab6ff"],
          [1, "#0d2e5f"]
        ],
        colorbar: { title: "达标率 (%)" }
      }
    ],
    siteLayout({ margin: { t: 30, r: 20, b: 60, l: 150 } }),
    { displayModeBar: false, responsive: true }
  );
};

const renderResourceScatter = rows => {
  if (!ensurePlotly("equityResource")) return;
  const filtered = rows
    .filter(row => row.tested_student_cnt >= 25)
    .sort((a, b) => b.tested_student_cnt - a.tested_student_cnt)
    .slice(0, 400);

  const classes = Array.from(new Set(filtered.map(row => row.resource_class))).filter(Boolean);
  const paletteSeq = idx => {
    const colors = [palette.ap, palette.ib, palette.accent, palette.warning, palette.neutral];
    return colors[idx % colors.length];
  };

  const traces = classes.map((cls, idx) => {
    const subset = filtered.filter(row => row.resource_class === cls);
    return {
      type: "scatter",
      mode: "markers",
      name: cls,
      x: subset.map(row => row.proficiency_rate * 100),
      y: subset.map(row => row.high_score_rate * 100),
      text: subset.map(row => `${row.aggregation_name}<br>${row.APIB_IND} · ${row.NRC_DESC}<br>测试人数：${fmtNumber(row.tested_student_cnt)}`),
      hovertemplate: "%{text}<br>达标率：%{x:.1f}% · 高分率：%{y:.1f}%<extra></extra>",
      marker: {
        size: subset.map(row => Math.max(6, Math.sqrt(row.tested_student_cnt) * 0.1)),
        color: paletteSeq(idx),
        opacity: 0.85,
        line: { width: 1, color: "#0b0f17" }
      }
    };
  });

  Plotly.newPlot(
    "equityResource",
    traces,
    siteLayout({
      xaxis: { title: "达标率 (%)", range: [0, 100], gridcolor: "rgba(0,0,0,0.08)" },
      yaxis: { title: "高分率 (%)", range: [0, 100], gridcolor: "rgba(0,0,0,0.08)" },
      legend: { orientation: "h", y: -0.25 }
    }),
    { displayModeBar: false, responsive: true }
  );
};

const renderSuppression = rows => {
  const container = document.getElementById("equitySuppression");
  if (!container) return;
  const sorted = [...rows].sort((a, b) => (b.suppressed_share_records ?? 0) - (a.suppressed_share_records ?? 0));
  const items = sorted.slice(0, 8);
  if (!items.length) {
    container.textContent = "暂无抑制热点数据。";
    return;
  }
  container.innerHTML = `
    <ul class="suppression-list">
      ${items
        .map(
          item => `
            <li>
              <div>
                <strong>${item.APIB_IND} · ${item.SUBGROUP_NAME}</strong>
                <p>记录数：${fmtNumber(item.record_count)}</p>
              </div>
              <span>${fmtPercent(item.suppressed_share_records ?? 0)}</span>
            </li>
          `
        )
        .join("")}
    </ul>
  `;
};

const renderDeck = specs => {
  renderStats(specs?.statewide_overview?.data);
  renderState(specs?.statewide_overview?.data ?? []);
  renderLevel(specs?.state_level_distribution?.data ?? []);
  renderDemographic(specs?.demographic_participation?.data ?? []);
  renderGap(specs?.demographic_gap);
  renderNrc(specs?.nrc_summary?.data ?? []);
  renderCounty(specs?.county_league);
  renderSubjectHeat(specs?.subject_summary?.data ?? []);
  renderResourceScatter(specs?.resource_scatter?.data ?? []);
  renderSuppression(specs?.suppression_hotspots?.data ?? []);
};

const initEquityPage = async () => {
  const hasStats = Object.values(statMap).some(Boolean);
  if (!hasStats) return;
  try {
    const response = await fetch(DATA_URL);
    if (!response.ok) {
      throw new Error(`加载失败：${response.status}`);
    }
    const raw = await response.text();
    const sanitized = raw.replace(/\bNaN\b/g, "null");
    const specs = JSON.parse(sanitized);
    renderDeck(specs);
  } catch (error) {
    console.error("加载公平性数据失败", error);
    const plots = document.querySelectorAll(".plot-shell");
    plots.forEach(plot => {
      plot.innerHTML = "<p class=\"plot-note\">数据加载失败，请稍后重试。</p>";
    });
    const suppression = document.getElementById("equitySuppression");
    if (suppression) suppression.textContent = "数据加载失败。";
  }
};

if (document.readyState === "loading") {
  document.addEventListener("DOMContentLoaded", initEquityPage, { once: true });
} else {
  initEquityPage();
}

