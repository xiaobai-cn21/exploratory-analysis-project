const form = document.getElementById("predictionForm");
const resultCard = document.getElementById("resultCard");
const resetBtn = document.getElementById("resetBtn");

const numericFields = ["aggregation_index", "NRC_CODE", "COUNTY_CODE", "NYC_IND", "tested_student_cnt"];

function renderMessage(html) {
  if (resultCard) {
    resultCard.innerHTML = html;
  }
}

form?.addEventListener("submit", async (event) => {
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

  renderMessage("<p>正在调用模型，请稍候...</p>");

  try {
    const response = await fetch("/api/predict", {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });

    if (!response.ok) {
      const errorData = await response.json();
      throw new Error(errorData.error || "Prediction failed");
    }

    const result = await response.json();
    const probability = (result.probability * 100).toFixed(1);
    const verdict = result.label ? "预计达标 ✅" : "预计未达标 ⚠️";

    renderMessage(`
      <p class="eyebrow">模型输出</p>
      <h3>${verdict}</h3>
      <p>概率：<strong>${probability}%</strong></p>
      <p class="plot-note">阈值设为 50%，可根据业务需求调整。</p>
    `);
  } catch (error) {
    renderMessage(`<p class="error">预测失败：${error.message}</p>`);
  }
});

resetBtn?.addEventListener("click", () => {
  renderMessage("<p>提交表单后将显示预测概率与判定结果。</p>");
});

