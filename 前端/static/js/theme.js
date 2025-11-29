const getFontFamily = () => {
	const computed = getComputedStyle(document.documentElement).getPropertyValue("--font-base");
	return computed ? computed.replace(/["']/g, "").trim() : "Open Sans, Segoe UI, sans-serif";
};

export const createMidnightLayout = (custom = {}) => ({
	paper_bgcolor: "rgba(0,0,0,0)",
	plot_bgcolor: "rgba(0,0,0,0)",
	font: {
		color: "#f5f7fb",
		family: getFontFamily()
	},
	margin: { t: 40, r: 20, b: 50, l: 45 },
	...custom
});

