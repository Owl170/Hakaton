const api = {
  async get(url) {
    const res = await fetch(url);
    if (!res.ok) {
      const err = await safeJson(res);
      throw new Error(err?.detail || `HTTP ${res.status}`);
    }
    return res.json();
  },
  async postJson(url, payload) {
    const res = await fetch(url, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify(payload),
    });
    if (!res.ok) {
      const err = await safeJson(res);
      throw new Error(err?.detail || `HTTP ${res.status}`);
    }
    return res.json();
  },
};

async function safeJson(res) {
  try {
    return await res.json();
  } catch {
    return null;
  }
}

const state = {
  analysisId: null,
  map: null,
  detectionLayer: null,
  boundariesLayer: null,
  layersReqId: 0,
  filtersReqId: 0,
  summaryReqId: 0,
};

const riskColors = {
  low: "#2da44e",
  moderate: "#c9a227",
  high: "#e67e22",
  critical: "#d73a49",
};

const riskLabels = {
  low: "Безопасный",
  moderate: "Низкий риск",
  high: "Повышенный риск",
  critical: "Опасный",
};

const featureLabels = {
  water_expansion: "Расширение воды",
  overwetting: "Переувлажнение",
  heave_mounds: "Бугры пучения",
  surface_texture_change: "Изменение текстуры",
};

function territoryLabel(value) {
  if (!value) return "Не указано";
  const low = String(value).toLowerCase();
  if (low.includes("amga") || low.includes("anga") || low.includes("амга")) return "Амга";
  if (low.includes("yunkor") || low.includes("юнкор")) return "Юнкор";
  return value;
}

function byId(id) {
  return document.getElementById(id);
}

function formatNumber(value, digits = 2) {
  const n = Number(value || 0);
  return Number.isFinite(n) ? n.toFixed(digits) : "0.00";
}

function formatPercent(probability, digits = 1) {
  const n = Number(probability || 0);
  if (!Number.isFinite(n)) return "0.0%";
  return `${(n * 100).toFixed(digits)}%`;
}

function setStatus(text, isError = false) {
  const el = byId("statusText");
  if (!el) return;
  el.textContent = text;
  el.style.color = isError ? "#b02121" : "#355174";
}

function setAnalysisMeta() {
  const meta = byId("analysisMeta");
  if (!meta) return;
  if (state.analysisId) {
    meta.textContent = `Анализ №${state.analysisId}`;
    return;
  }
  meta.textContent = "Анализ не выбран";
}

function initMap() {
  if (!byId("map")) return;
  state.map = L.map("map", { zoomControl: true }).setView([62.0, 129.9], 8);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(state.map);
}

function styleDetection(feature) {
  const risk = feature.properties.risk_level || "low";
  return {
    color: riskColors[risk] || riskColors.low,
    weight: 1.3,
    fillColor: riskColors[risk] || riskColors.low,
    fillOpacity: 0.55,
  };
}

function styleBoundary() {
  return {
    color: "#2f6fe3",
    weight: 1.8,
    fill: false,
    dashArray: "4 4",
  };
}

function renderFeatureCard(properties) {
  const card = byId("featureCard");
  if (!card) return;
  const parcelId = properties.parcel_id || "Не указано";
  const territory = territoryLabel(properties.territory);
  const year = properties.year || "Не указано";
  const riskLevel = riskLabels[properties.risk_level] || (properties.risk_level || "Не указано");
  const riskScore = formatNumber(properties.risk_score, 3);
  const featureType = featureLabels[properties.feature_type] || (properties.feature_type || "Не указано");
  const areaHa = formatNumber(properties.area_ha, 2);
  const water = formatNumber((properties.water_fraction || 0) * 100, 1);
  const wet = formatNumber((properties.wet_fraction || 0) * 100, 1);
  const heave = formatNumber((properties.heave_fraction || 0) * 100, 1);

  card.innerHTML = `
    <strong>${parcelId}</strong><br>
    Территория: ${territory}<br>
    Год: ${year}<br>
    Риск: ${riskScore} (${riskLevel})<br>
    Признак: ${featureType}<br>
    Площадь проблемной зоны: ${areaHa} га<br>
    Вода: ${water}%<br>
    Переувлажнение: ${wet}%<br>
    Бугры/текстура: ${heave}%
  `;
}

function updateFilterOptions(territories, years) {
  const territoryFilter = byId("territoryFilter");
  const yearFilter = byId("yearFilter");
  if (!territoryFilter || !yearFilter) return;

  const currentTerritory = territoryFilter.value;
  const currentYear = yearFilter.value;

  territoryFilter.innerHTML = '<option value="">Все</option>';
  [...territories].sort().forEach((value) => {
    const opt = document.createElement("option");
    opt.value = value;
    opt.textContent = territoryLabel(value);
    territoryFilter.appendChild(opt);
  });

  yearFilter.innerHTML = '<option value="">Все</option>';
  [...years]
    .map((y) => Number(y))
    .filter((y) => Number.isFinite(y))
    .sort((a, b) => a - b)
    .forEach((value) => {
      const opt = document.createElement("option");
      opt.value = String(value);
      opt.textContent = String(value);
      yearFilter.appendChild(opt);
    });

  territoryFilter.value = [...territories].includes(currentTerritory) ? currentTerritory : "";
  yearFilter.value = [...years].map(String).includes(currentYear) ? currentYear : "";
}

async function loadFilterOptions() {
  const reqId = ++state.filtersReqId;
  const [baseData, analyses] = await Promise.all([
    api.get("/map/layers"),
    api.get("/analysis/results"),
  ]);
  if (reqId !== state.filtersReqId) return;

  if (baseData.analysis_id) state.analysisId = baseData.analysis_id;
  setAnalysisMeta();

  const territories = new Set();
  const years = new Set();

  (baseData.boundaries?.features || []).forEach((f) => {
    if (f.properties?.territory) territories.add(f.properties.territory);
  });
  (baseData.available_territories || []).forEach((v) => territories.add(v));
  (baseData.features || []).forEach((f) => {
    if (f.properties?.territory) territories.add(f.properties.territory);
    if (f.properties?.year !== undefined && f.properties?.year !== null) years.add(Number(f.properties.year));
  });
  (baseData.available_years || []).forEach((year) => years.add(Number(year)));

  (analyses.items || [])
    .filter((row) => String(row.status) === "completed")
    .forEach((row) => {
      (row.years || []).forEach((year) => years.add(Number(year)));
      (row.territories || []).forEach((territory) => territories.add(territory));
    });

  updateFilterOptions(territories, years);
}

function focusMapByTerritory(boundaryFC, selectedTerritory) {
  if (!state.map || !boundaryFC) return;

  if (selectedTerritory) {
    const targetFeatures = (boundaryFC.features || []).filter(
      (f) => String(f.properties?.territory || "") === String(selectedTerritory)
    );
    if (targetFeatures.length) {
      const targetLayer = L.geoJSON({ type: "FeatureCollection", features: targetFeatures });
      const targetBounds = targetLayer.getBounds();
      if (targetBounds.isValid()) {
        state.map.fitBounds(targetBounds.pad(0.2));
        return;
      }
    }
  }

  const allLayer = L.geoJSON(boundaryFC);
  const allBounds = allLayer.getBounds();
  if (allBounds.isValid()) {
    state.map.fitBounds(allBounds.pad(0.15));
  }
}

function renderYearlyChart(summary) {
  const chart = byId("yearlyChart");
  if (!chart) return;
  const rows = summary.yearly_dynamics || [];
  if (!rows.length) {
    chart.innerHTML = "<div>Нет данных</div>";
    return;
  }

  const maxArea = Math.max(...rows.map((r) => Number(r.problem_area_ha || 0)), 1);
  chart.innerHTML = "";
  rows
    .slice()
    .sort((a, b) => Number(a.year) - Number(b.year))
    .forEach((row) => {
      const width = (Number(row.problem_area_ha || 0) / maxArea) * 100;
      const el = document.createElement("div");
      el.className = "year-row";
      el.innerHTML = `
        <div><strong>${row.year}</strong> • ${formatNumber(row.problem_area_ha, 2)} га • риск ${formatPercent(row.mean_risk_score, 1)}</div>
        <div class="bar" style="width:${Math.max(width, 2)}%;"></div>
      `;
      chart.appendChild(el);
    });
}

function setStats(summary) {
  byId("problemAreaValue").textContent = `${formatNumber(summary.total_problem_area_ha, 2)} га`;
  byId("objectsCountValue").textContent = `${summary.objects_count || 0}`;
  byId("meanRiskValue").textContent = formatPercent(summary.mean_risk_score, 1);
  renderYearlyChart(summary);
}

async function loadSummary() {
  const reqId = ++state.summaryReqId;
  const summary = await api.get("/stats/summary");
  if (reqId !== state.summaryReqId) return;
  if (summary.analysis_id) state.analysisId = summary.analysis_id;
  setAnalysisMeta();
  setStats(summary);
}

async function loadLayers() {
  const reqId = ++state.layersReqId;
  const params = new URLSearchParams();

  const territory = byId("territoryFilter")?.value || "";
  const year = byId("yearFilter")?.value || "";
  const risk = byId("riskFilter")?.value || "";
  const feature = byId("featureFilter")?.value || "";

  if (territory) params.set("territory", territory);
  if (year) params.set("year", year);
  if (risk) params.set("risk_level", risk);
  if (feature) params.set("feature_type", feature);

  const data = await api.get(`/map/layers?${params.toString()}`);
  if (reqId !== state.layersReqId) return;
  if (data.analysis_id) state.analysisId = data.analysis_id;
  setAnalysisMeta();

  if (state.boundariesLayer) state.map.removeLayer(state.boundariesLayer);
  state.boundariesLayer = L.geoJSON(data.boundaries, { style: styleBoundary }).addTo(state.map);

  if (state.detectionLayer) state.map.removeLayer(state.detectionLayer);
  state.detectionLayer = L.geoJSON(data, {
    style: styleDetection,
    onEachFeature: (featureItem, layer) => {
      layer.on("click", () => renderFeatureCard(featureItem.properties || {}));
      const riskLabel = riskLabels[featureItem.properties?.risk_level] || featureItem.properties?.risk_level || "";
      layer.bindTooltip(`${featureItem.properties?.parcel_id || "Участок"} • ${riskLabel}`);
    },
  }).addTo(state.map);

  if (!data.features || data.features.length === 0) {
    byId("featureCard").textContent = "Детекции не найдены по выбранным фильтрам.";
  }

  focusMapByTerritory(data.boundaries, territory);
}

async function refreshAll() {
  await loadFilterOptions();
  await loadLayers();
  await loadSummary();
}

async function runAnalysis() {
  const territory = byId("territoryFilter")?.value || "";
  const yearValue = byId("yearFilter")?.value || "";
  const payload = {
    territories: territory ? [territory] : null,
    years: yearValue ? [Number(yearValue)] : null,
    force_retrain: false,
  };

  setStatus("Выполняется анализ...");
  const result = await api.postJson("/analysis/run", payload);
  state.analysisId = null;
  setStatus(`Анализ №${result.analysis_id} завершён`);
  await refreshAll();
}

function bindEvents() {
  byId("runAnalysisBtn")?.addEventListener("click", async () => {
    try {
      await runAnalysis();
    } catch (e) {
      setStatus(e.message || String(e), true);
    }
  });

  byId("refreshBtn")?.addEventListener("click", async () => {
    try {
      setStatus("Обновление слоёв...");
      await refreshAll();
      setStatus("Слои обновлены");
    } catch (e) {
      setStatus(e.message || String(e), true);
    }
  });

  byId("exportBtn")?.addEventListener("click", () => {
    window.open("/export/geojson", "_blank");
  });

  ["territoryFilter", "yearFilter", "riskFilter", "featureFilter"].forEach((id) => {
    byId(id)?.addEventListener("change", async () => {
      try {
        await loadLayers();
      } catch (e) {
        setStatus(e.message || String(e), true);
      }
    });
  });
}

async function initPage() {
  if (!byId("map")) return;
  initMap();
  bindEvents();
  try {
    setStatus("Загрузка данных...");
    state.analysisId = null;
    await refreshAll();
    setStatus("Готово");
  } catch (e) {
    setStatus(e.message || String(e), true);
  }
}

window.addEventListener("DOMContentLoaded", initPage);
