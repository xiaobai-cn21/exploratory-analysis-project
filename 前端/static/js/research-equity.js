import { createMidnightLayout } from "./theme.js";

const DATA_URL = "/static/data/fairness/chart_specs.json";

const palette = {
	ap: "#58a6ff",
	ib: "#ffa657",
	accent: "#42dd8a",
	warning: "#f08c67",
	neutral: "#8da0c5"
};

const statFields = {
	apTested: document.querySelector('[data-field="ap-tested"]'),
	apValid: document.querySelector('[data-field="ap-valid"]'),
	apProficiency: document.querySelector('[data-field="ap-proficiency"]'),
	apGap: document.querySelector('[data-field="ap-gap"]'),
	apHighscore: document.querySelector('[data-field="ap-highscore"]'),
	ibTested: document.querySelector('[data-field="ib-tested"]'),
	ibValid: document.querySelector('[data-field="ib-valid"]'),
	ibProficiency: document.querySelector('[data-field="ib-proficiency"]'),
	ibGap: document.querySelector('[data-field="ib-gap"]'),
	ibHighscore: document.querySelector('[data-field="ib-highscore"]')
};

const formatNumber = (value) => (typeof value === "number" ? value.toLocaleString("en-US") : "—");
const formatPercent = (value, digits = 1) =>
	typeof value === "number" ? `${(value * 100).toFixed(digits)}%` : "—";

const handlePlotlyMissing = (containerId) => {
	const container = document.getElementById(containerId);
	if (container) {
		container.innerHTML = "<p class=\"plot-note\">Plotly 未加载，暂无法渲染。</p>";
	}
};

const setStatValue = (node, value) => {
	if (!node) {
		return;
	}
	node.textContent = value;
};

const renderStats = (rows = []) => {
	const mapped = rows.reduce((acc, row) => {
		acc[row.APIB_IND] = row;
		return acc;
	}, {});

	const ap = mapped.AP;
	const ib = mapped.IB;

	if (ap) {
		setStatValue(statFields.apTested, formatNumber(ap.tested_total));
		setStatValue(statFields.apValid, `有效覆盖 ${formatPercent(ap.valid_coverage)}`);
		setStatValue(statFields.apProficiency, formatPercent(ap.proficiency_rate));
		setStatValue(statFields.apGap, `抑制率 ${formatPercent(ap.suppressed_share)}`);
		setStatValue(statFields.apHighscore, formatPercent(ap.high_score_rate));
	}

	if (ib) {
		setStatValue(statFields.ibTested, formatNumber(ib.tested_total));
		setStatValue(statFields.ibValid, `有效覆盖 ${formatPercent(ib.valid_coverage)}`);
		setStatValue(statFields.ibProficiency, formatPercent(ib.proficiency_rate));
		setStatValue(statFields.ibGap, `抑制率 ${formatPercent(ib.suppressed_share)}`);
		setStatValue(statFields.ibHighscore, formatPercent(ib.high_score_rate));
	}
};

const renderState = (rows = []) => {
	if (!window.Plotly) {
		handlePlotlyMissing("equity-state");
		return;
	}

	const programs = rows.reduce((acc, row) => {
		acc[row.APIB_IND] = row;
		return acc;
	}, {});

	const categories = ["测试人数(千)", "达标率(%)", "高分率(%)", "抑制率(%)"];

	const data = ["AP", "IB"]
		.filter((program) => programs[program])
		.map((program) => {
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

	const layout = createMidnightLayout({
		barmode: "group",
		yaxis: {
			title: "数值 (千 / pp)",
			gridcolor: "rgba(255,255,255,0.08)"
		},
		legend: { orientation: "h", y: -0.25 }
	});

	Plotly.newPlot("equity-state", data, layout, { displayModeBar: false, responsive: true });
};

const friendlyLevel = (level) => {
	if (!level) {
		return level;
	}
	const number = level.replace("level", "").replace("_cnt", "");
	return `Level ${number}`;
};

const renderLevel = (rows = []) => {
	if (!window.Plotly) {
		handlePlotlyMissing("equity-level");
		return;
	}

	const levelOrder = ["level1_cnt", "level2_cnt", "level3_cnt", "level4_cnt", "level5_cnt", "level6_cnt", "level7_cnt"];
	const grouped = rows.reduce((acc, row) => {
		if (!acc[row.APIB_IND]) {
			acc[row.APIB_IND] = {};
		}
		acc[row.APIB_IND][row.level] = row.share * 100;
		return acc;
	}, {});

	const traces = ["AP", "IB"]
		.filter((program) => grouped[program])
		.map((program) => ({
			x: levelOrder.map((level) => friendlyLevel(level)),
			y: levelOrder.map((level) => grouped[program][level] ?? 0),
			type: "bar",
			name: program,
			marker: { color: program === "AP" ? palette.ap : palette.ib }
		}));

	const layout = createMidnightLayout({
		barmode: "stack",
		yaxis: {
			title: "占比 (%)",
			range: [0, 100]
		},
		legend: { orientation: "h", y: -0.25 }
	});

	Plotly.newPlot("equity-level", traces, layout, { displayModeBar: false, responsive: true });
};

const renderDemographic = (rows = []) => {
	if (!window.Plotly) {
		handlePlotlyMissing("equity-demo");
		return;
	}

	const sorted = [...rows].sort((a, b) => b.tested_student_cnt - a.tested_student_cnt).slice(0, 40);
	const programs = ["AP", "IB"];

	const traces = programs
		.map((program) => {
			const subset = sorted.filter((row) => row.APIB_IND === program);
			if (!subset.length) {
				return null;
			}
			return {
				type: "scatter",
				mode: "markers",
				name: program,
				x: subset.map((row) => row.participation_share * 100),
				y: subset.map((row) => row.proficiency_rate * 100),
				text: subset.map(
					(row) =>
						`${row.SUBGROUP_NAME} · ${program}<br>测试人数：${formatNumber(row.tested_student_cnt)}`
				),
				hovertemplate: "%{text}<br>参与份额：%{x:.1f}%<br>达标率：%{y:.1f}%<extra></extra>",
				marker: {
					size: subset.map((row) => Math.max(8, Math.sqrt(row.tested_student_cnt) * 0.15)),
					color: program === "AP" ? palette.ap : palette.ib,
					opacity: 0.85,
					line: { width: 1, color: "#0b0f17" }
				}
			};
		})
		.filter(Boolean);

	const layout = createMidnightLayout({
		xaxis: { title: "参与份额 (%)" },
		yaxis: { title: "达标率 (%)" },
		legend: { orientation: "h", y: -0.25 }
	});

	Plotly.newPlot("equity-demo", traces, layout, { displayModeBar: false, responsive: true });
};

const renderGap = (gap = {}) => {
	if (!window.Plotly) {
		handlePlotlyMissing("equity-gap");
		return;
	}

	const positives = gap.positive?.slice(0, 5) ?? [];
	const negatives = gap.negative?.slice(0, 5) ?? [];
	const combined = [...positives, ...negatives];

	if (!combined.length) {
		handlePlotlyMissing("equity-gap");
		return;
	}

	const labels = combined.map((item) => `${item.APIB_IND} · ${item.SUBGROUP_NAME}`);
	const values = combined.map((item) => item.gap_vs_all * 100);

	const trace = {
		type: "bar",
		orientation: "h",
		y: labels,
		x: values,
		marker: {
			// 使用与其他图表一致的配色：蓝色表示正向差距，橙色表示负向差距
			color: combined.map((item) => (item.gap_vs_all >= 0 ? palette.ap : palette.ib))
		},
		hovertemplate: "%{y}<br>差距：%{x:.1f}pp<extra></extra>"
	};

	const layout = createMidnightLayout({
		margin: { t: 30, r: 20, b: 30, l: 180 },
		xaxis: { title: "Gap vs All (pp)", zerolinecolor: "rgba(255,255,255,0.15)" }
	});

	Plotly.newPlot("equity-gap", [trace], layout, { displayModeBar: false, responsive: true });
};

const renderNrc = (rows = []) => {
	if (!window.Plotly) {
		handlePlotlyMissing("equity-nrc");
		return;
	}

	const categories = Array.from(
		new Set(
			rows
				.slice()
				.sort((a, b) => a.NRC_CODE - b.NRC_CODE)
				.map((row) => row.NRC_DESC)
		)
	);

	const grouped = rows.reduce((acc, row) => {
		if (!acc[row.APIB_IND]) {
			acc[row.APIB_IND] = {};
		}
		acc[row.APIB_IND][row.NRC_DESC] = row.proficiency_rate * 100;
		return acc;
	}, {});

	const traces = ["AP", "IB"]
		.filter((program) => grouped[program])
		.map((program) => ({
			type: "scatterpolar",
			r: categories.map((cat) => grouped[program][cat] ?? 0),
			theta: categories,
			fill: "toself",
			fillcolor:
				program === "AP"
					? "rgba(88, 166, 255, 0.15)"
					: "rgba(255, 166, 87, 0.15)",
			name: program,
			line: {
				color: program === "AP" ? palette.ap : palette.ib,
				width: 2.5
			},
			marker: {
				size: 8,
				color: program === "AP" ? palette.ap : palette.ib,
				line: {
					color: "#0b0f17",
					width: 1.5
				}
			},
			hovertemplate: `<b>%{fullData.name}</b><br>%{theta}: %{r:.1f}%<extra></extra>`
		}));

	const layout = createMidnightLayout({
		polar: {
			bgcolor: "rgba(19, 26, 36, 0.5)",
			radialaxis: {
				range: [0, 100],
				gridcolor: "rgba(255,255,255,0.08)",
				gridwidth: 1,
				showline: true,
				linecolor: "rgba(255,255,255,0.12)",
				linewidth: 1,
				tickfont: { 
					color: "#8da0c5",
					size: 11
				},
				ticksuffix: "%",
				angle: 90,
				dtick: 20,
				tickangle: 0
			},
			angularaxis: {
				tickfont: { 
					color: "#d0d8ed",
					size: 10
				},
				gridcolor: "rgba(255,255,255,0.05)",
				linecolor: "rgba(255,255,255,0.12)"
			}
		},
		showlegend: true,
		legend: {
			orientation: "h",
			y: -0.2,
			x: 0.5,
			xanchor: "center",
			font: { size: 12 }
		},
		margin: { t: 50, r: 50, b: 50, l: 50 }
	});

	const config = {
		displayModeBar: false,
		responsive: true
	};

	// 先绘制初始状态（r 值为 0）
	const initialTraces = traces.map((trace) => ({
		...trace,
		r: trace.r.map(() => 0)
	}));

	Plotly.newPlot("equity-nrc", initialTraces, layout, config).then(() => {
		// 使用 animate 方法添加动画效果
		Plotly.animate("equity-nrc", {
			data: traces,
			layout: layout,
			transition: {
				duration: 800,
				easing: "cubic-in-out"
			}
		});
	});
};

const buildCountyColumn = (title, entries = []) => {
	const rows = entries
		.slice(0, 5)
		.map(
			(entry) => `
				<li>
					<span>${entry.COUNTY_NAME}</span>
					<span>${formatPercent(entry.proficiency_rate)}</span>
				</li>
			`
		)
		.join("");
	return `
		<div class="county-column">
			<h4>${title}</h4>
			<ul>${rows}</ul>
		</div>
	`;
};

const renderCounty = (league = {}) => {
	const container = document.getElementById("equity-county");
	if (!container) {
		return;
	}

	container.innerHTML = `
		<div class="county-league">
			${buildCountyColumn("AP · Top", league.ap?.top)}
			${buildCountyColumn("AP · Bottom", league.ap?.bottom)}
			${buildCountyColumn("IB · Top", league.ib?.top)}
			${buildCountyColumn("IB · Bottom", league.ib?.bottom)}
		</div>
	`;
};

const renderSubjectHeat = (rows = []) => {
	if (!window.Plotly) {
		handlePlotlyMissing("equity-subject");
		return;
	}

	const subjects = Array.from(new Set(rows.map((row) => row.SUBJECT_AREA))).sort();
	const programs = ["AP", "IB"];

	const matrix = subjects.map((subject) =>
		programs.map((program) => {
			const match = rows.find((row) => row.SUBJECT_AREA === subject && row.APIB_IND === program);
			return match ? match.proficiency_rate * 100 : null;
		})
	);

	const heatmap = [
		{
			type: "heatmap",
			z: matrix,
			x: programs,
			y: subjects,
			colorscale: [
				[0, "#081120"],
				[0.5, "#1f4d77"],
				[1, "#58a6ff"]
			],
			colorbar: { title: "% proficient" }
		}
	];

	const layout = createMidnightLayout({
		margin: { t: 30, r: 20, b: 60, l: 150 }
	});

	Plotly.newPlot("equity-subject", heatmap, layout, { displayModeBar: false, responsive: true });
};

const renderResourceScatter = (rows = []) => {
	if (!window.Plotly) {
		handlePlotlyMissing("equity-resource");
		return;
	}

	const filtered = rows
		.filter((row) => row.tested_student_cnt >= 25)
		.sort((a, b) => b.tested_student_cnt - a.tested_student_cnt)
		.slice(0, 400);

	const classes = Array.from(new Set(filtered.map((row) => row.resource_class))).filter(Boolean);

	const traces = classes.map((cls, idx) => {
		const subset = filtered.filter((row) => row.resource_class === cls);
		return {
			type: "scatter",
			mode: "markers",
			name: cls,
			x: subset.map((row) => row.proficiency_rate * 100),
			y: subset.map((row) => row.high_score_rate * 100),
			text: subset.map(
				(row) =>
					`${row.aggregation_name}<br>${row.APIB_IND} · ${row.NRC_DESC}<br>测试人数：${formatNumber(
						row.tested_student_cnt
					)}`
			),
			hovertemplate: "%{text}<br>达标率：%{x:.1f}% · 高分率：%{y:.1f}%<extra></extra>",
			marker: {
				size: subset.map((row) => Math.max(6, Math.sqrt(row.tested_student_cnt) * 0.1)),
				color: paletteSequence(idx),
				opacity: 0.85,
				line: { width: 1, color: "#0b0f17" }
			}
		};
	});

	const layout = createMidnightLayout({
		xaxis: { title: "达标率 (%)", range: [0, 100] },
		yaxis: { title: "高分率 (%)", range: [0, 100] },
		legend: { orientation: "h", y: -0.25 }
	});

	Plotly.newPlot("equity-resource", traces, layout, { displayModeBar: false, responsive: true });
};

const paletteSequence = (index) => {
	const colors = [palette.ap, palette.ib, palette.accent, palette.warning, palette.neutral];
	return colors[index % colors.length];
};

const renderSuppression = (rows = []) => {
	const container = document.getElementById("equity-suppression");
	if (!container) {
		return;
	}

	const sorted = [...rows].sort((a, b) => (b.suppressed_share_records ?? 0) - (a.suppressed_share_records ?? 0));
	const items = sorted.slice(0, 8);

	const list = items
		.map(
			(item) => `
				<li>
					<div>
						<strong>${item.APIB_IND} · ${item.SUBGROUP_NAME}</strong>
						<p>记录数：${formatNumber(item.record_count)}</p>
					</div>
					<span class="suppression-chip">${formatPercent(item.suppressed_share_records ?? 0)}</span>
				</li>
			`
		)
		.join("");

	container.innerHTML = `<ul class="suppression-list">${list}</ul>`;
};

const renderDeck = (specs) => {
	renderStats(specs?.statewide_overview?.data);
	renderState(specs?.statewide_overview?.data);
	renderLevel(specs?.state_level_distribution?.data);
	renderDemographic(specs?.demographic_participation?.data);
	renderGap(specs?.demographic_gap);
	renderNrc(specs?.nrc_summary?.data);
	renderCounty(specs?.county_league);
	renderSubjectHeat(specs?.subject_summary?.data);
	renderResourceScatter(specs?.resource_scatter?.data);
	renderSuppression(specs?.suppression_hotspots?.data || []);
};

const init = async () => {
	try {
		const response = await fetch(DATA_URL);
		if (!response.ok) {
			throw new Error(`无法加载数据：${response.status}`);
		}
		const raw = await response.text();
		const sanitized = raw.replace(/\bNaN\b/g, "null");
		const specs = JSON.parse(sanitized);
		renderDeck(specs);
	} catch (error) {
		console.error("加载公平性 EDA 数据失败", error);
		const cards = document.querySelectorAll(".plotly-canvas");
		cards.forEach((card) => {
			card.innerHTML = "<p class=\"plot-note\">数据加载失败，请稍后重试。</p>";
		});
	}
};

if (document.readyState === "loading") {
	document.addEventListener("DOMContentLoaded", init, { once: true });
} else {
	init();
}


