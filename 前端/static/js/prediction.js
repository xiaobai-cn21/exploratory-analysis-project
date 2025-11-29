const form = document.getElementById("predictionForm");
const resultDiv = document.getElementById("predictionResult");
const chartDiv = document.getElementById("predictionChart");
const mapContainer = document.getElementById("nyStateMap");
const mapSvg = document.getElementById("nyStateMapSvg");
const mapTooltip = document.getElementById("nyStateMapTooltip");

const numericFields = ["aggregation_index", "NRC_CODE", "COUNTY_CODE", "NYC_IND", "tested_student_cnt"];

// 初始化纽约州地图
(function initNYStateMap() {
  if (typeof d3 === 'undefined' || typeof topojson === 'undefined' || !mapSvg) return;

  const topojsonUrl = '/static/data/json/new_york_counties.json';
  const svg = d3.select(mapSvg);
  const tooltip = d3.select(mapTooltip);
  const countyCodeInput = document.getElementById("COUNTY_CODE");
  
  let nyCounties = [];
  let projection;
  let pathGenerator;
  let g;
  let colorScale;
  let selectedCountyCode = null;
  let highlightedCountyCode = null;
  let countyPaths = null;
  let isProficient = null;
  const colorInterpolator = d3.interpolateRgb("#1e3a5f", "#58a6ff");

  function getSize() {
    if (!mapContainer) return { width: 600, height: 400 };
    const rect = mapContainer.getBoundingClientRect();
    return {
      width: Math.max(400, Math.floor(rect.width)),
      height: 400
    };
  }

  function updateSize() {
    const size = getSize();
    svg.attr("width", size.width).attr("height", size.height);
    
    if (projection && nyCounties && nyCounties.objects) {
      const features = topojson.feature(nyCounties, nyCounties.objects.cb_2015_new_york_county_20m);
      projection.fitSize([size.width, size.height], features);
      if (pathGenerator) {
        renderMap();
      }
    }
  }

  // 高亮指定县
  function highlightCounty(countyCode, isProficientResult = null) {
    if (!countyPaths) return;
    
    highlightedCountyCode = countyCode;
    isProficient = isProficientResult;
    
    const features = countyPaths.data();
    
    countyPaths
      .attr("fill", (d, i) => {
        const code = parseInt(d.properties.COUNTYFP) || 0;
        if (code === countyCode) {
          return isProficientResult !== null 
            ? (isProficientResult ? '#4ade80' : '#f87171')
            : '#ffa657';
        }
        const ratio = i / features.length;
        return colorInterpolator(ratio);
      })
      .attr("stroke", d => {
        const code = parseInt(d.properties.COUNTYFP) || 0;
        return code === countyCode ? '#ffffff' : "rgba(255, 255, 255, 0.3)";
      })
      .attr("stroke-width", d => {
        const code = parseInt(d.properties.COUNTYFP) || 0;
        return code === countyCode ? 3 : 0.8;
      })
      .attr("opacity", d => {
        const code = parseInt(d.properties.COUNTYFP) || 0;
        return code === countyCode ? 1 : 0.85;
      });
  }
  
  // 清除高亮
  function clearMapHighlight() {
    if (!countyPaths) return;
    
    highlightedCountyCode = null;
    isProficient = null;
    
    const colorInterpolator = d3.interpolateRgb("#1e3a5f", "#58a6ff");
    const features = countyPaths.data();
    
    countyPaths
      .attr("fill", (d, i) => {
        const ratio = i / features.length;
        return colorInterpolator(ratio);
      })
      .attr("stroke", "rgba(255, 255, 255, 0.3)")
      .attr("stroke-width", 0.8)
      .attr("opacity", 0.85);
  }
  
  // 显示地图通知
  function showMapNotification(message) {
    const notification = document.createElement("div");
    notification.style.cssText = `
      position: fixed;
      top: 20px;
      right: 20px;
      background: var(--card, #131a24);
      color: var(--text, #f5f7fb);
      padding: 1rem 1.5rem;
      border-radius: 0.5rem;
      border: 1px solid var(--accent, #58a6ff);
      box-shadow: 0 4px 12px rgba(0, 0, 0, 0.3);
      z-index: 10000;
      font-size: 0.9rem;
      animation: slideIn 0.3s ease;
    `;
    notification.textContent = message;
    document.body.appendChild(notification);
    
    setTimeout(() => {
      notification.style.animation = "slideOut 0.3s ease";
      setTimeout(() => notification.remove(), 300);
    }, 2000);
  }
  
  function renderMap() {
    if (!g || !pathGenerator || !colorScale) return;

    const size = getSize();
    svg.attr("width", size.width).attr("height", size.height);

    const features = topojson.feature(nyCounties, nyCounties.objects.cb_2015_new_york_county_20m).features;

    // 存储县代码到特征的映射
    const countyCodeMap = new Map();
    features.forEach(d => {
      const code = parseInt(d.properties.COUNTYFP) || 0;
      countyCodeMap.set(code, d);
    });
    
    countyPaths = g.selectAll("path")
      .data(features)
      .join("path")
      .attr("d", pathGenerator)
      .attr("data-county-code", d => parseInt(d.properties.COUNTYFP) || 0)
      .attr("fill", (d, i) => {
        const code = parseInt(d.properties.COUNTYFP) || 0;
        // 如果是选中的县，使用特殊颜色
        if (highlightedCountyCode === code) {
          return isProficient !== null ? (isProficient ? '#4ade80' : '#f87171') : '#ffa657';
        }
        // 使用索引创建平滑的渐变色
        const ratio = i / features.length;
        return colorInterpolator(ratio);
      })
      .attr("stroke", d => {
        const code = parseInt(d.properties.COUNTYFP) || 0;
        if (highlightedCountyCode === code) {
          return '#ffffff';
        }
        return "rgba(255, 255, 255, 0.3)";
      })
      .attr("stroke-width", d => {
        const code = parseInt(d.properties.COUNTYFP) || 0;
        return highlightedCountyCode === code ? 3 : 0.8;
      })
      .attr("opacity", d => {
        const code = parseInt(d.properties.COUNTYFP) || 0;
        return highlightedCountyCode === code ? 1 : 0.85;
      })
      .style("cursor", "pointer")
      .on("click", function(event, d) {
        event.stopPropagation();
        const code = parseInt(d.properties.COUNTYFP) || 0;
        selectedCountyCode = code;
        
        // 更新表单中的县代码
        if (countyCodeInput) {
          countyCodeInput.value = code;
          // 触发 change 事件
          countyCodeInput.dispatchEvent(new Event('change', { bubbles: true }));
        }
        
        // 高亮选中的县
        highlightCounty(code);
        
        // 显示提示信息
        const countyName = d.properties.NAME || "Unknown";
        showMapNotification(`${countyName} County (代码: ${code}) 已选中`);
      })
      .on("mouseenter", function(event, d) {
        if (highlightedCountyCode !== null) return; // 如果已有高亮，不处理悬停
        
        d3.select(this)
          .attr("opacity", 1)
          .attr("stroke-width", 2.5)
          .attr("stroke", "#58a6ff")
          .attr("fill", "#58a6ff");
        
        const countyName = d.properties.NAME || "Unknown";
        const countyCode = d.properties.COUNTYFP || "N/A";
        tooltip
          .style("display", "block")
          .style("opacity", 0)
          .html(`
            <div style="padding: 0.5rem;">
              <strong style="color: #58a6ff; font-size: 1.1em;">${countyName} County</strong><br/>
              <span style="color: #8da0c5; font-size: 0.9em;">县代码: ${countyCode}</span><br/>
              <span style="color: #8da0c5; font-size: 0.8em; font-style: italic;">点击选择此县</span>
            </div>
          `)
          .style("left", (event.pageX + 15) + "px")
          .style("top", (event.pageY - 10) + "px")
          .transition()
          .duration(200)
          .style("opacity", 1);
      })
      .on("mousemove", function(event) {
        tooltip
          .style("left", (event.pageX + 15) + "px")
          .style("top", (event.pageY - 10) + "px");
      })
      .on("mouseleave", function(event, d) {
        if (highlightedCountyCode !== null) return; // 如果已有高亮，不处理离开
        
        const code = parseInt(d.properties.COUNTYFP) || 0;
        const isHighlighted = highlightedCountyCode === code;
        
        if (!isHighlighted) {
          d3.select(this)
            .attr("opacity", 0.85)
            .attr("stroke-width", 0.8)
            .attr("stroke", "rgba(255, 255, 255, 0.3)")
            .attr("fill", (d, i) => {
              const ratio = i / features.length;
              return colorInterpolator(ratio);
            });
        }
        
        tooltip
          .transition()
          .duration(200)
          .style("opacity", 0)
          .on("end", () => tooltip.style("display", "none"));
      });
    
    // 暴露函数供外部调用
    window.highlightCountyOnMap = highlightCounty;
    window.clearMapHighlight = clearMapHighlight;
  }

  // 加载地图数据
  d3.json(topojsonUrl).then(data => {
    nyCounties = data;
    
    const size = getSize();
    const features = topojson.feature(nyCounties, nyCounties.objects.cb_2015_new_york_county_20m);
    projection = d3.geoAlbersUsa()
      .fitSize([size.width, size.height], features);
    
    pathGenerator = d3.geoPath().projection(projection);
    
    colorScale = d3.scaleSequential(d3.interpolateBlues)
      .domain([0, 100]);

    g = svg.append("g");
    
    renderMap();

    // 响应窗口大小变化
    window.addEventListener("resize", () => {
      updateSize();
    });
  }).catch(err => {
    console.error("加载地图数据失败:", err);
    if (mapSvg) {
      mapSvg.innerHTML = `<text x="50%" y="50%" text-anchor="middle" fill="#8da0c5">地图加载失败</text>`;
    }
  });
})();

if (form) {
  form.addEventListener("submit", async (event) => {
    event.preventDefault();
    
    const formData = new FormData(form);
    const payload = {};
    
    for (const [key, value] of formData.entries()) {
      if (numericFields.includes(key)) {
        payload[key] = Number(value);
      } else {
        payload[key] = value;
      }
    }
    
    resultDiv.innerHTML = "<p>正在预测，请稍候...</p>";
    chartDiv.innerHTML = "";
    
    try {
      const response = await fetch("/api/predict_proficiency", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload),
      });
      
      if (!response.ok) {
        const errorData = await response.json();
        throw new Error(errorData.error || "预测失败");
      }
      
      const result = await response.json();
      const probability = (result.probability * 100).toFixed(2);
      const isProficient = result.is_proficient;
      const metrics = result.metrics || {};
      
      // 显示结果
      resultDiv.innerHTML = `
        <div style="text-align: center; padding: 2rem;">
          <h2 style="color: var(--accent, #58a6ff); margin-bottom: 1rem; font-size: 1.5rem;">预测结果</h2>
          <div style="font-size: 2.5rem; font-weight: bold; color: ${isProficient ? '#4ade80' : '#f87171'}; margin: 1rem 0;">
            ${isProficient ? '✅ 预计达标' : '⚠️ 预计未达标'}
          </div>
          <div style="font-size: 2rem; font-weight: bold; color: var(--accent-2, #ffa657); margin: 1rem 0;">
            概率: ${probability}%
          </div>
          <p style="color: var(--text-muted, #8da0c5); margin-top: 1rem; font-size: 0.9rem;">
            基于 MLP Classifier 模型预测<br>
            AUC = ${metrics.auc || 0.956}, Accuracy = ${metrics.accuracy || 0.89}, F1 = ${metrics.f1_score || 0.92}
          </p>
        </div>
      `;
      
      // 创建可视化图表
      createPredictionChart(result.probability, isProficient);
      
      // 高亮地图上对应的县
      const countyCode = payload.COUNTY_CODE;
      if (countyCode && window.highlightCountyOnMap) {
        window.highlightCountyOnMap(countyCode, isProficient);
      }
      
    } catch (error) {
      resultDiv.innerHTML = `<p style="color: var(--warning, #f08c67);">预测失败：${error.message}</p>`;
    }
  });
  
  // 监听县代码输入变化，更新地图高亮
  if (countyCodeInput) {
    countyCodeInput.addEventListener("change", function() {
      const code = parseInt(this.value);
      if (code && window.highlightCountyOnMap) {
        window.highlightCountyOnMap(code, null);
      }
    });
  }
}

function createPredictionChart(probability, isProficient) {
  // 清空之前的图表
  chartDiv.innerHTML = "";
  
  const width = 400;
  const height = 300;
  const margin = { top: 20, right: 20, bottom: 40, left: 60 };
  const innerWidth = width - margin.left - margin.right;
  const innerHeight = height - margin.top - margin.bottom;
  
  const svg = d3.select(chartDiv)
    .append("svg")
    .attr("width", width)
    .attr("height", height);
  
  const g = svg.append("g")
    .attr("transform", `translate(${margin.left},${margin.top})`);
  
  // 创建比例尺
  const xScale = d3.scaleLinear()
    .domain([0, 1])
    .range([0, innerWidth]);
  
  const yScale = d3.scaleBand()
    .domain(["达标概率"])
    .range([0, innerHeight])
    .padding(0.3);
  
  // 获取 CSS 变量颜色（深色主题）
  const getComputedStyle = window.getComputedStyle(document.body);
  const accentColor = getComputedStyle.getPropertyValue('--accent').trim() || '#58a6ff';
  const successColor = isProficient ? '#4ade80' : '#f87171';
  const warningColor = getComputedStyle.getPropertyValue('--warning').trim() || '#f08c67';
  const textColor = getComputedStyle.getPropertyValue('--text').trim() || '#f5f7fb';
  const textMutedColor = getComputedStyle.getPropertyValue('--text-muted').trim() || '#8da0c5';
  const borderColor = getComputedStyle.getPropertyValue('--border').trim() || 'rgba(255, 255, 255, 0.08)';
  const cardColor = getComputedStyle.getPropertyValue('--card').trim() || '#131a24';
  
  // 添加背景条
  g.append("rect")
    .attr("x", 0)
    .attr("y", 0)
    .attr("width", innerWidth)
    .attr("height", innerHeight)
    .attr("fill", cardColor)
    .attr("stroke", borderColor)
    .attr("stroke-width", 1)
    .attr("rx", 4);
  
  // 添加预测值条
  const bar = g.append("rect")
    .attr("x", 0)
    .attr("y", yScale("达标概率"))
    .attr("width", 0)
    .attr("height", yScale.bandwidth())
    .attr("fill", successColor)
    .attr("rx", 4);
  
  // 动画效果
  bar.transition()
    .duration(1000)
    .attr("width", xScale(probability));
  
  // 添加数值标签
  g.append("text")
    .attr("x", xScale(probability) + 10)
    .attr("y", yScale("达标概率") + yScale.bandwidth() / 2)
    .attr("dy", "0.35em")
    .attr("fill", textColor)
    .attr("font-size", "14px")
    .attr("font-weight", "bold")
    .text(`${(probability * 100).toFixed(2)}%`);
  
  // 添加 X 轴
  const xAxis = d3.axisBottom(xScale)
    .tickFormat(d3.format(".0%"))
    .ticks(5);
  
  g.append("g")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(xAxis)
    .selectAll("text")
    .style("font-size", "12px")
    .style("fill", textMutedColor);
  
  g.append("g")
    .attr("transform", `translate(0,${innerHeight})`)
    .call(xAxis)
    .selectAll("line, path")
    .style("stroke", borderColor);
  
  // 添加 Y 轴
  const yAxis = d3.axisLeft(yScale);
  
  g.append("g")
    .call(yAxis)
    .selectAll("text")
    .style("font-size", "12px")
    .style("fill", textColor);
  
  g.append("g")
    .call(yAxis)
    .selectAll("line, path")
    .style("stroke", borderColor);
  
  // 添加参考线（50%阈值）
  g.append("line")
    .attr("x1", xScale(0.5))
    .attr("x2", xScale(0.5))
    .attr("y1", 0)
    .attr("y2", innerHeight)
    .attr("stroke", warningColor)
    .attr("stroke-width", 2)
    .attr("stroke-dasharray", "5,5")
    .attr("opacity", 0.6);
  
  g.append("text")
    .attr("x", xScale(0.5))
    .attr("y", -5)
    .attr("text-anchor", "middle")
    .attr("fill", warningColor)
    .attr("font-size", "10px")
    .text("50% 阈值");
}

