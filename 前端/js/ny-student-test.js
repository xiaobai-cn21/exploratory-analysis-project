import { createMidnightLayout } from "./theme.js";

(() => {
	const gradeSelect = document.getElementById("grade-select");
	const trendChart = document.getElementById("trend-chart");
	const boroughTableBody = document.getElementById("borough-table-body");
	const insightList = document.getElementById("insight-list");
	const scrollUpdates = document.getElementById("scroll-updates");
	const updatesSection = document.getElementById("updates");

	const fields = {
		growthNote: document.querySelector('[data-field="growth-note"]'),
		elaScore: document.querySelector('[data-field="ela-score"]'),
		elaChange: document.querySelector('[data-field="ela-change"]'),
		mathScore: document.querySelector('[data-field="math-score"]'),
		mathChange: document.querySelector('[data-field="math-change"]'),
		participation: document.querySelector('[data-field="participation"]'),
		participationChange: document.querySelector('[data-field="participation-change"]'),
		elaProficiencyLabel: document.querySelector('[data-field="ela-proficiency-label"]'),
		elaProficiency: document.querySelector('[data-field="ela-proficiency"]'),
		mathProficiencyLabel: document.querySelector('[data-field="math-proficiency-label"]'),
		mathProficiency: document.querySelector('[data-field="math-proficiency"]'),
		trendTitle: document.querySelector('[data-field="trend-title"]')
	};

	const gradeData = {
		grade3: {
			label: "Grade 3 projected proficiency",
			growthNote: "Phonics-heavy classrooms add +3.4pp momentum citywide.",
			elaScore: "2.89 / 4",
			mathScore: "2.74 / 4",
			elaChange: "较 2024 量尺分 +0.18",
			mathChange: "较 2024 量尺分 +0.12",
			participation: "94%",
			participationChange: "+1.6 pp",
			elaProficiency: 68,
			mathProficiency: 61,
			trend: [
				{ year: 2021, score: 55 },
				{ year: 2022, score: 59 },
				{ year: 2023, score: 63 },
				{ year: 2024, score: 66 },
				{ year: 2025, score: 68 }
			],
			boroughs: [
				{ name: "Queens", ela: "67%", math: "64%", momentum: "Dual language lift" },
				{ name: "Manhattan", ela: "71%", math: "66%", momentum: "Reader's workshop" },
				{ name: "Staten Island", ela: "69%", math: "62%", momentum: "Family opt-in surge" }
			],
			insights: [
				"Daily decodable routines cut foundational skill gaps by 40% in D2.",
				"Schools pairing read-alouds with oracy rubrics report the biggest ELL gains.",
				"Move running records online to shorten teacher prep by ~90 minutes weekly."
			]
		},
		grade5: {
			label: "Grade 5 projected proficiency",
			growthNote: "STEM lab rotations add +2.1pp math lift where fully staffed.",
			elaScore: "3.02 / 4",
			mathScore: "2.88 / 4",
			elaChange: "+0.11 vs 2024 scale score",
			mathChange: "+0.19 vs 2024 scale score",
			participation: "92%",
			participationChange: "+0.7 pp",
			elaProficiency: 66,
			mathProficiency: 63,
			trend: [
				{ year: 2021, score: 58 },
				{ year: 2022, score: 60 },
				{ year: 2023, score: 62 },
				{ year: 2024, score: 64 },
				{ year: 2025, score: 66 }
			],
			boroughs: [
				{ name: "Brooklyn", ela: "65%", math: "61%", momentum: "Writing studio pilots" },
				{ name: "Queens", ela: "67%", math: "66%", momentum: "ST Math labs" },
				{ name: "Bronx", ela: "60%", math: "57%", momentum: "Saturday math clubs" }
			],
			insights: [
				"Grade 5 teams using backwards planning from middle-school syllabi saw smoother transitions.",
				"Math studio cycles with student talk trackers correlated with +6pp on multi-step items.",
				"ELA teachers flag the need for more nonfiction text sets tied to social studies."
			]
		},
		grade8: {
			label: "Grade 8 projected proficiency",
			growthNote: "Algebra readiness bootcamps add +4.5pp for priority cohorts.",
			elaScore: "3.18 / 4",
			mathScore: "2.97 / 4",
			elaChange: "+0.09 vs 2024 scale score",
			mathChange: "+0.21 vs 2024 scale score",
			participation: "89%",
			participationChange: "+1.1 pp",
			elaProficiency: 70,
			mathProficiency: 65,
			trend: [
				{ year: 2021, score: 60 },
				{ year: 2022, score: 63 },
				{ year: 2023, score: 64 },
				{ year: 2024, score: 67 },
				{ year: 2025, score: 70 }
			],
			boroughs: [
				{ name: "Manhattan", ela: "74%", math: "71%", momentum: "Regents-aligned tasks" },
				{ name: "Queens", ela: "70%", math: "66%", momentum: "Co-taught algebra" },
				{ name: "Brooklyn", ela: "68%", math: "64%", momentum: "Peer tutoring pods" }
			],
			insights: [
				"Teams adopting Regents-style constructed response scoring each Friday close gaps faster.",
				"Student-led data notebooks keep 8th graders invested in their own growth forecasts.",
				"Partnering with CUNY tutors during advisory lifts confidence before test week."
			]
		}
	};

	const formatGradeLabel = (gradeKey) => `Grade ${gradeKey.replace("grade", "")}`;

	const gradeColors = {
		grade3: "#58a6ff",
		grade5: "#ffa657",
		grade8: "#42dd8a"
	};

	const gradeKeys = Object.keys(gradeData);
	const gradeLabels = gradeKeys.map((key) => formatGradeLabel(key));
	const elaSeries = gradeKeys.map((key) => gradeData[key].elaProficiency);
	const mathSeries = gradeKeys.map((key) => gradeData[key].mathProficiency);
	const years = gradeData.grade3.trend.map((point) => point.year);
	const mergedBoroughs = gradeKeys.flatMap((key) =>
		gradeData[key].boroughs.map((borough) => ({
			gradeKey: key,
			gradeLabel: formatGradeLabel(key),
			...borough
		}))
	);

	const supportSlices = [
		{ label: "Extended time", value: 18 },
		{ label: "Small-group tutoring", value: 32 },
		{ label: "Family workshops", value: 22 },
		{ label: "Tech readiness checks", value: 15 },
		{ label: "Coaching cycles", value: 13 }
	];

	const proficiencyStack = [
		{ grade: "Grade 3", emerging: 18, onTrack: 52, excelling: 30 },
		{ grade: "Grade 5", emerging: 20, onTrack: 50, excelling: 30 },
		{ grade: "Grade 8", emerging: 15, onTrack: 49, excelling: 36 }
	];

	const benchmarkScores = {
		grade3: [2.4, 2.6, 2.9, 3.0, 2.7, 2.8, 3.1, 2.5],
		grade5: [2.7, 2.9, 3.2, 3.1, 2.8, 3.0, 3.3, 2.9],
		grade8: [3.0, 3.2, 3.4, 3.1, 3.3, 3.5, 3.2, 3.4]
	};

	const readinessFunnel = [
		{ stage: "Enrolled", value: 52000 },
		{ stage: "Opted-In", value: 48600 },
		{ stage: "Tested", value: 46800 },
		{ stage: "Proficient", value: 31200 },
		{ stage: "Mastery", value: 12800 }
	];

	const focusAreas = {
		categories: ["Foundational reading", "Student discourse", "Writing craft", "Math reasoning", "Data talks"],
		grade3: [82, 68, 60, 55, 70],
		grade5: [70, 74, 72, 68, 66],
		grade8: [58, 72, 78, 82, 74]
	};

	const supportHeatmap = {
		boroughs: ["Bronx", "Brooklyn", "Manhattan", "Queens", "Staten Island"],
		supports: ["Extended time", "Tutoring", "Workshops", "Tech checks", "Coaching"],
		values: [
			[24, 35, 28, 22, 18],
			[20, 32, 26, 18, 15],
			[15, 25, 30, 20, 24],
			[28, 34, 24, 26, 20],
			[18, 27, 22, 16, 19]
		]
	};

	const glidepath = {
		years,
		projected: [55, 59, 63, 66, 69],
		target: [56, 61, 65, 68, 72]
	};

	const plotDefinitions = [
		{
			containerId: "plot-grade-blend",
			codeBlockId: "code-grade-blend",
			render: () => {
				if (!window.Plotly) {
					return;
				}
				const data = [
					{
						x: gradeLabels,
						y: elaSeries,
						type: "bar",
						name: "ELA",
						marker: { color: gradeColors.grade3 }
					},
					{
						x: gradeLabels,
						y: mathSeries,
						type: "bar",
						name: "Math",
						marker: { color: gradeColors.grade5 }
					}
				];

				const layout = createMidnightLayout({
					barmode: "group",
					legend: { orientation: "h", y: -0.2 },
					yaxis: {
						title: "% Proficient",
						gridcolor: "rgba(255,255,255,0.08)",
						zerolinecolor: "rgba(255,255,255,0.08)"
					},
					xaxis: {
						tickfont: { size: 12 }
					}
				});

				Plotly.newPlot("plot-grade-blend", data, layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const grades = ['Grade 3', 'Grade 5', 'Grade 8'];
const ela = [68, 66, 70];
const math = [61, 63, 65];

const gradeBlendData = [
  { x: grades, y: ela, type: 'bar', name: 'ELA', marker: { color: '#58a6ff' } },
  { x: grades, y: math, type: 'bar', name: 'Math', marker: { color: '#ffa657' } }
];

const gradeBlendLayout = {
  barmode: 'group',
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#f5f7fb' },
  yaxis: { title: '% Proficient', gridcolor: 'rgba(255,255,255,0.1)' },
  legend: { orientation: 'h' }
};

Plotly.newPlot('plot-grade-blend', gradeBlendData, gradeBlendLayout);
			`.trim()
		},
		{
			containerId: "plot-growth-lines",
			codeBlockId: "code-growth-lines",
			render: () => {
				if (!window.Plotly) {
					return;
				}

				const traces = gradeKeys.map((key) => ({
					x: years,
					y: gradeData[key].trend.map((point) => point.score),
					mode: "lines+markers",
					name: formatGradeLabel(key),
					line: { width: 3, color: gradeColors[key] },
					marker: { size: 8 }
				}));

				const layout = createMidnightLayout({
					yaxis: {
						title: "% Proficient",
						gridcolor: "rgba(255,255,255,0.08)"
					},
					xaxis: {
						title: "School year",
						gridcolor: "rgba(255,255,255,0.04)",
						tickvals: years
					},
					legend: { orientation: "h", y: -0.25 }
				});

				Plotly.newPlot("plot-growth-lines", traces, layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const years = [2021, 2022, 2023, 2024, 2025];
const grade3 = [55, 59, 63, 66, 68];
const grade5 = [58, 60, 62, 64, 66];
const grade8 = [60, 63, 64, 67, 70];

const growthLines = [
  { x: years, y: grade3, mode: 'lines+markers', name: 'Grade 3' },
  { x: years, y: grade5, mode: 'lines+markers', name: 'Grade 5' },
  { x: years, y: grade8, mode: 'lines+markers', name: 'Grade 8' }
];

const growthLayout = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#f5f7fb' },
  yaxis: { title: '% Proficient', gridcolor: 'rgba(255,255,255,0.1)' },
  xaxis: { title: 'School year' }
};

Plotly.newPlot('plot-growth-lines', growthLines, growthLayout);
			`.trim()
		},
		{
			containerId: "plot-borough-scatter",
			codeBlockId: "code-borough-scatter",
			render: () => {
				if (!window.Plotly) {
					return;
				}

				const trace = {
					x: mergedBoroughs.map((point) => Number(point.math.replace("%", ""))),
					y: mergedBoroughs.map((point) => Number(point.ela.replace("%", ""))),
					mode: "markers",
					text: mergedBoroughs.map(
						(point) => `${point.gradeLabel} · ${point.name}<br>${point.momentum}`
					),
					hovertemplate: "%{text}<br>Math: %{x}% · ELA: %{y}%<extra></extra>",
					marker: {
						size: 14,
						color: mergedBoroughs.map((point) => gradeColors[point.gradeKey] || "#8da0c5"),
						line: { width: 1, color: "#0b0f17" }
					}
				};

				const layout = createMidnightLayout({
					xaxis: {
						title: "Math proficiency (%)",
						gridcolor: "rgba(255,255,255,0.05)"
					},
					yaxis: {
						title: "ELA proficiency (%)",
						gridcolor: "rgba(255,255,255,0.05)"
					}
				});

				Plotly.newPlot("plot-borough-scatter", [trace], layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const ela = [67, 71, 69, 65, 67, 60, 74, 70, 68];
const math = [64, 66, 62, 61, 66, 57, 71, 66, 64];
const labels = [
  'Grade 3 · Queens', 'Grade 3 · Manhattan', 'Grade 3 · Staten Island',
  'Grade 5 · Brooklyn', 'Grade 5 · Queens', 'Grade 5 · Bronx',
  'Grade 8 · Manhattan', 'Grade 8 · Queens', 'Grade 8 · Brooklyn'
];

const boroughScatter = {
  x: math,
  y: ela,
  text: labels,
  mode: 'markers',
  marker: { size: 14, color: '#58a6ff' }
};

const scatterLayout = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#f5f7fb' },
  xaxis: { title: 'Math proficiency (%)' },
  yaxis: { title: 'ELA proficiency (%)' }
};

Plotly.newPlot('plot-borough-scatter', [boroughScatter], scatterLayout);
			`.trim()
		},
		{
			containerId: "plot-support-donut",
			codeBlockId: "code-support-donut",
			render: () => {
				if (!window.Plotly) {
					return;
				}

				const labels = supportSlices.map((slice) => slice.label);
				const values = supportSlices.map((slice) => slice.value);

				const data = [
					{
						values,
						labels,
						type: "pie",
						hole: 0.6,
						marker: {
							colors: [
								"#58a6ff",
								"#ffa657",
								"#42dd8a",
								"#f08c67",
								"#8da0c5"
							]
						},
						textinfo: "label+percent",
						hoverinfo: "label+value"
					}
				];

				const layout = createMidnightLayout({
					showlegend: false,
					annotations: [
						{
							text: "Supports",
							showarrow: false,
							font: { color: "#f5f7fb", size: 14 }
						}
					]
				});

				Plotly.newPlot("plot-support-donut", data, layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const labels = [
  'Extended time', 'Small-group tutoring', 'Family workshops',
  'Tech readiness checks', 'Coaching cycles'
];
const values = [18, 32, 22, 15, 13];

const donut = [{
  values,
  labels,
  type: 'pie',
  hole: 0.6,
  textinfo: 'label+percent'
}];

const donutLayout = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#f5f7fb' },
  showlegend: false
};

Plotly.newPlot('plot-support-donut', donut, donutLayout);
			`.trim()
		},
		{
			containerId: "plot-proficiency-stack",
			codeBlockId: "code-proficiency-stack",
			render: () => {
				if (!window.Plotly) {
					return;
				}

				const traces = [
					{
						x: proficiencyStack.map((row) => row.grade),
						y: proficiencyStack.map((row) => row.emerging),
						name: "Emerging",
						type: "bar",
						marker: { color: "#2a4562" }
					},
					{
						x: proficiencyStack.map((row) => row.grade),
						y: proficiencyStack.map((row) => row.onTrack),
						name: "On track",
						type: "bar",
						marker: { color: "#58a6ff" }
					},
					{
						x: proficiencyStack.map((row) => row.grade),
						y: proficiencyStack.map((row) => row.excelling),
						name: "Excelling",
						type: "bar",
						marker: { color: "#42dd8a" }
					}
				];

				const layout = createMidnightLayout({
					barmode: "stack",
					yaxis: {
						title: "% of students",
						range: [0, 100],
						gridcolor: "rgba(255,255,255,0.08)"
					},
					legend: { orientation: "h", y: -0.2 }
				});

				Plotly.newPlot("plot-proficiency-stack", traces, layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const grades = ['Grade 3', 'Grade 5', 'Grade 8'];
const emerging = [18, 20, 15];
const onTrack = [52, 50, 49];
const excelling = [30, 30, 36];

const stackData = [
  { x: grades, y: emerging, type: 'bar', name: 'Emerging' },
  { x: grades, y: onTrack, type: 'bar', name: 'On track' },
  { x: grades, y: excelling, type: 'bar', name: 'Excelling' }
];

const stackLayout = {
  barmode: 'stack',
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#f5f7fb' },
  yaxis: { title: '% of students', range: [0, 100] }
};

Plotly.newPlot('plot-proficiency-stack', stackData, stackLayout);
			`.trim()
		},
		{
			containerId: "plot-benchmark-box",
			codeBlockId: "code-benchmark-box",
			render: () => {
				if (!window.Plotly) {
					return;
				}

				const traces = gradeKeys.map((key) => ({
					type: "box",
					name: formatGradeLabel(key),
					y: benchmarkScores[key],
					boxmean: true,
					line: { color: gradeColors[key] }
				}));

				const layout = createMidnightLayout({
					yaxis: {
						title: "Scale score (1-4)",
						gridcolor: "rgba(255,255,255,0.08)"
					}
				});

				Plotly.newPlot("plot-benchmark-box", traces, layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const grade3 = [2.4, 2.6, 2.9, 3.0, 2.7, 2.8, 3.1, 2.5];
const grade5 = [2.7, 2.9, 3.2, 3.1, 2.8, 3.0, 3.3, 2.9];
const grade8 = [3.0, 3.2, 3.4, 3.1, 3.3, 3.5, 3.2, 3.4];

const boxTraces = [
  { y: grade3, type: 'box', name: 'Grade 3', boxmean: true },
  { y: grade5, type: 'box', name: 'Grade 5', boxmean: true },
  { y: grade8, type: 'box', name: 'Grade 8', boxmean: true }
];

const boxLayout = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#f5f7fb' },
  yaxis: { title: 'Scale score (1-4)' }
};

Plotly.newPlot('plot-benchmark-box', boxTraces, boxLayout);
			`.trim()
		},
		{
			containerId: "plot-mastery-funnel",
			codeBlockId: "code-mastery-funnel",
			render: () => {
				if (!window.Plotly) {
					return;
				}

				const data = [
					{
						type: "funnel",
						y: readinessFunnel.map((row) => row.stage),
						x: readinessFunnel.map((row) => row.value),
						textinfo: "value+percent previous",
						marker: { color: ["#58a6ff", "#4f8dd7", "#3c6fb0", "#2b548a", "#1a3963"] }
					}
				];

				const layout = createMidnightLayout({
					margin: { t: 30, r: 30, l: 60, b: 30 }
				});

				Plotly.newPlot("plot-mastery-funnel", data, layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const stages = ['Enrolled', 'Opted-In', 'Tested', 'Proficient', 'Mastery'];
const values = [52000, 48600, 46800, 31200, 12800];

const funnel = [{
  type: 'funnel',
  y: stages,
  x: values,
  textinfo: 'value+percent previous'
}];

const funnelLayout = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#f5f7fb' }
};

Plotly.newPlot('plot-mastery-funnel', funnel, funnelLayout);
			`.trim()
		},
		{
			containerId: "plot-focus-radar",
			codeBlockId: "code-focus-radar",
			render: () => {
				if (!window.Plotly) {
					return;
				}

				const traces = gradeKeys.map((key) => ({
					type: "scatterpolar",
					r: focusAreas[key],
					theta: focusAreas.categories,
					fill: "toself",
					name: formatGradeLabel(key),
					line: { color: gradeColors[key] }
				}));

				const layout = createMidnightLayout({
					polar: {
						bgcolor: "rgba(0,0,0,0)",
						radialaxis: {
							tickfont: { color: "#9fb3d9" },
							gridcolor: "rgba(255,255,255,0.08)",
							range: [0, 100]
						},
						angularaxis: {
							tickfont: { color: "#f5f7fb" }
						}
					},
					showlegend: true,
					legend: { orientation: "h", y: -0.2 }
				});

				Plotly.newPlot("plot-focus-radar", traces, layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const categories = ['Foundational reading', 'Student discourse', 'Writing craft', 'Math reasoning', 'Data talks'];
const grade3 = [82, 68, 60, 55, 70];
const grade5 = [70, 74, 72, 68, 66];
const grade8 = [58, 72, 78, 82, 74];

const radarData = [
  { type: 'scatterpolar', r: grade3, theta: categories, fill: 'toself', name: 'Grade 3' },
  { type: 'scatterpolar', r: grade5, theta: categories, fill: 'toself', name: 'Grade 5' },
  { type: 'scatterpolar', r: grade8, theta: categories, fill: 'toself', name: 'Grade 8' }
];

const radarLayout = {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#f5f7fb' },
  polar: { radialaxis: { range: [0, 100] } }
};

Plotly.newPlot('plot-focus-radar', radarData, radarLayout);
			`.trim()
		},
		{
			containerId: "plot-support-heatmap",
			codeBlockId: "code-support-heatmap",
			render: () => {
				if (!window.Plotly) {
					return;
				}

				const data = [
					{
						z: supportHeatmap.values,
						x: supportHeatmap.supports,
						y: supportHeatmap.boroughs,
						type: "heatmap",
						colorscale: [
							[0, "#081120"],
							[0.33, "#17365c"],
							[0.66, "#2f68a4"],
							[1, "#58a6ff"]
						],
						colorbar: { title: "% uptake" }
					}
				];

				const layout = createMidnightLayout({
					xaxis: { title: "Support type" },
					yaxis: { title: "Borough" }
				});

				Plotly.newPlot("plot-support-heatmap", data, layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const boroughs = ['Bronx', 'Brooklyn', 'Manhattan', 'Queens', 'Staten Island'];
const supports = ['Extended time', 'Tutoring', 'Workshops', 'Tech checks', 'Coaching'];
const matrix = [
  [24, 35, 28, 22, 18],
  [20, 32, 26, 18, 15],
  [15, 25, 30, 20, 24],
  [28, 34, 24, 26, 20],
  [18, 27, 22, 16, 19]
];

const heatmap = [{
  type: 'heatmap',
  z: matrix,
  x: supports,
  y: boroughs,
  colorscale: 'Blues'
}];

Plotly.newPlot('plot-support-heatmap', heatmap, { paper_bgcolor: 'rgba(0,0,0,0)', font: { color: '#f5f7fb' } });
			`.trim()
		},
		{
			containerId: "plot-growth-area",
			codeBlockId: "code-growth-area",
			render: () => {
				if (!window.Plotly) {
					return;
				}

				const traces = [
					{
						x: glidepath.years,
						y: glidepath.target,
						type: "scatter",
						mode: "lines",
						name: "Target",
						line: { color: "#ffa657", width: 3 },
						fill: "tonexty"
					},
					{
						x: glidepath.years,
						y: glidepath.projected,
						type: "scatter",
						mode: "lines",
						name: "Projected",
						line: { color: "#58a6ff", width: 3 },
						fill: "tozeroy"
					}
				];

				// Ensure projected is plotted first for fill layering
				const orderedTraces = [traces[1], traces[0]];

				const layout = createMidnightLayout({
					xaxis: { title: "School year" },
					yaxis: { title: "% proficient", range: [50, 75], gridcolor: "rgba(255,255,255,0.08)" },
					legend: { orientation: "h", y: -0.2 }
				});

				Plotly.newPlot("plot-growth-area", orderedTraces, layout, {
					displayModeBar: false,
					responsive: true
				});
			},
			code: `
const years = [2021, 2022, 2023, 2024, 2025];
const projected = [55, 59, 63, 66, 69];
const target = [56, 61, 65, 68, 72];

const area = [
  { x: years, y: projected, type: 'scatter', mode: 'lines', name: 'Projected', fill: 'tozeroy' },
  { x: years, y: target, type: 'scatter', mode: 'lines', name: 'Target', fill: 'tonexty' }
];

Plotly.newPlot('plot-growth-area', area, {
  paper_bgcolor: 'rgba(0,0,0,0)',
  plot_bgcolor: 'rgba(0,0,0,0)',
  font: { color: '#f5f7fb' },
  xaxis: { title: 'School year' },
  yaxis: { title: '% proficient' }
});
			`.trim()
		}
	];

	const renderPlotlyDeck = () => {
		if (!window.Plotly) {
			console.warn("Plotly failed to load.");
			return;
		}

		plotDefinitions.forEach((definition) => {
			definition.render();
			const codeBlock = document.getElementById(definition.codeBlockId);
			if (codeBlock) {
				codeBlock.textContent = definition.code;
			}
		});
	};

	const attachCopyHandlers = () => {
		const buttons = document.querySelectorAll(".copy-code");
		buttons.forEach((button) => {
			button.addEventListener("click", async () => {
				const targetId = button.dataset.copyTarget;
				const codeNode = document.getElementById(targetId);
				if (!codeNode) {
					return;
				}

				const text = codeNode.textContent;
				let copied = false;

				if (navigator.clipboard?.writeText) {
					try {
						await navigator.clipboard.writeText(text);
						copied = true;
					} catch (error) {
						console.warn("Clipboard API unavailable", error);
					}
				}

				if (!copied) {
					const range = document.createRange();
					range.selectNodeContents(codeNode);
					const selection = window.getSelection();
					selection.removeAllRanges();
					selection.addRange(range);
					try {
						document.execCommand("copy");
						copied = true;
					} catch (error) {
						console.warn("Fallback copy failed", error);
					} finally {
						selection.removeAllRanges();
					}
				}

				if (copied) {
					const originalText = button.textContent;
					button.textContent = "已复制";
					setTimeout(() => {
						button.textContent = originalText;
					}, 2000);
				}
			});
		});
	};

	const renderTrendChart = (trend) => {
		trendChart.innerHTML = "";
		const maxScore = Math.max(...trend.map((point) => point.score));

		trend.forEach((point) => {
			const bar = document.createElement("div");
			bar.className = "trend-bar";
			const barHeight = (point.score / maxScore) * 100;
			bar.style.height = `${barHeight}%`;
			bar.dataset.score = point.score;

			const label = document.createElement("span");
			label.textContent = point.year;
			bar.appendChild(label);

			trendChart.appendChild(bar);
		});
	};

	const renderBoroughs = (boroughs) => {
		boroughTableBody.innerHTML = "";
		boroughs.forEach((borough) => {
			const row = document.createElement("tr");
			row.innerHTML = `
				<td>${borough.name}</td>
				<td>${borough.ela}</td>
				<td>${borough.math}</td>
				<td>${borough.momentum}</td>
			`;
			boroughTableBody.appendChild(row);
		});
	};

	const renderInsights = (insights) => {
		insightList.innerHTML = "";
		insights.forEach((text) => {
			const li = document.createElement("li");
			li.textContent = text;
			insightList.appendChild(li);
		});
	};

	const renderGrade = (gradeKey) => {
		const data = gradeData[gradeKey];
		if (!data) {
			return;
		}

		fields.growthNote.textContent = data.growthNote;
		fields.trendTitle.textContent = data.label;

		fields.elaScore.textContent = data.elaScore;
		fields.elaChange.textContent = data.elaChange;
		fields.mathScore.textContent = data.mathScore;
		fields.mathChange.textContent = data.mathChange;
		fields.participation.textContent = data.participation;
		fields.participationChange.textContent = data.participationChange;

		fields.elaProficiencyLabel.textContent = `${data.elaProficiency}% proficient`;
		fields.mathProficiencyLabel.textContent = `${data.mathProficiency}% proficient`;
		fields.elaProficiency.style.width = `${data.elaProficiency}%`;
		fields.mathProficiency.style.width = `${data.mathProficiency}%`;

		renderTrendChart(data.trend);
		renderBoroughs(data.boroughs);
		renderInsights(data.insights);
	};

	gradeSelect.addEventListener("change", (event) => {
		renderGrade(event.target.value);
	});

	scrollUpdates.addEventListener("click", () => {
		updatesSection.scrollIntoView({ behavior: "smooth" });
	});

	renderGrade(gradeSelect.value);
	renderPlotlyDeck();
	attachCopyHandlers();
})();
