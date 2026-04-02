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
            headers: {"Content-Type": "application/json"},
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

function sleep(ms) {
    return new Promise((resolve) => setTimeout(resolve, ms));
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
    low: "#3AA87A",
    moderate: "#E8A03A",
    high: "#E05C3A",
    critical: "#C94A2D",
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

function setStatus(text, isError = false, durationSec = null) {
    const el = byId("statusText");
    if (!el) return;
    el.textContent = durationSec !== null ? `${text} · ${durationSec.toFixed(1)} с` : text;
    el.style.color = isError ? "#E05C3A" : "#1A3A4A";
}

function setAnalysisMeta() {
    const meta = byId("analysisMeta");
    if (!meta) return;
    if (state.analysisId) {
        meta.textContent = `Мониторинг › Анализ №${state.analysisId}`;
        return;
    }
    meta.textContent = "Мониторинг › Анализ №—";
}

function renderFeaturePlaceholder() {
    const card = byId("featureCard");
    if (!card) return;
    card.classList.add("md-feature-placeholder");
    card.innerHTML = `
    <div class="md-hint-icon">⌖</div>
    <p>Выберите полигон на карте для просмотра метрик участка.</p>
  `;
}

function initMap() {
    if (!byId("map")) return;
    state.map = L.map("map", {zoomControl: true}).setView([62.0, 129.9], 8);
    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
        attribution: "&copy; OpenStreetMap",
    }).addTo(state.map);
}

function styleBoundary() {
    return {
        color: "#2A7FAF",
        weight: 1.2,
        fill: false,
        dashArray: "4 4",
        opacity: 0.9,
    };
}

function styleDetection(feature, outlineEnabled) {
    const risk = feature.properties.risk_level || "low";
    return {
        color: outlineEnabled ? (riskColors[risk] || riskColors.low) : "transparent",
        weight: outlineEnabled ? 1 : 0,
        fillColor: riskColors[risk] || riskColors.low,
        fillOpacity: 0.46,
    };
}

function renderFeatureCard(properties) {
    const card = byId("featureCard");
    if (!card) return;
    card.classList.remove("md-feature-placeholder");
    const parcelId = properties.parcel_id || "Не указано";
    const territory = territoryLabel(properties.territory);
    const year = properties.year || "Не указано";
    const riskLevel = riskLabels[properties.risk_level] || (properties.risk_level || "Не указано");
    const riskScore = formatPercent(properties.risk_score, 1);
    const featureType = featureLabels[properties.feature_type] || (properties.feature_type || "Не указано");
    const areaHa = formatNumber(properties.area_ha, 2);
    const water = formatNumber((properties.water_fraction || 0) * 100, 1);
    const wet = formatNumber((properties.wet_fraction || 0) * 100, 1);
    const heave = formatNumber((properties.heave_fraction || 0) * 100, 1);

    card.innerHTML = `
    <strong>${parcelId}</strong><br>
    Территория: ${territory}<br>
    Год: ${year}<br>
    Уровень риска: ${riskLevel}<br>
    Вероятность: ${riskScore}<br>
    Признак: ${featureType}<br>
    Проблемная площадь: ${areaHa} га<br>
    Вода: ${water}% · Переувлажнение: ${wet}% · Текстура: ${heave}%
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

function focusMapByTerritory(boundaryFC, selectedTerritory, detectionLayer) {
    if (!state.map || !boundaryFC) return;

    if (selectedTerritory) {
        const targetFeatures = (boundaryFC.features || []).filter(
            (f) => String(f.properties?.territory || "") === String(selectedTerritory)
        );
        if (targetFeatures.length) {
            const targetLayer = L.geoJSON({type: "FeatureCollection", features: targetFeatures});
            const targetBounds = targetLayer.getBounds();
            if (targetBounds.isValid()) {
                state.map.fitBounds(targetBounds.pad(0.18));
                return;
            }
        }
    }

    if (detectionLayer) {
        const detectionBounds = detectionLayer.getBounds();
        if (detectionBounds.isValid()) {
            state.map.fitBounds(detectionBounds.pad(0.12));
            return;
        }
    }

    const allLayer = L.geoJSON(boundaryFC);
    const allBounds = allLayer.getBounds();
    if (allBounds.isValid()) {
        state.map.fitBounds(allBounds.pad(0.12));
    }
}

function riskBandClass(scorePercent) {
    if (scorePercent < 35) return "safe";
    if (scorePercent <= 43) return "warn";
    return "danger";
}

function renderYearlyChart(summary) {
    const chart = byId("yearlyChart");
    if (!chart) return;
    const rows = (summary.yearly_dynamics || []).slice().sort((a, b) => Number(a.year) - Number(b.year));
    if (!rows.length) {
        chart.innerHTML = "<div class='year-empty'>Нет данных</div>";
        return;
    }

    const selectedYearRaw = byId("yearFilter")?.value || "";
    const selectedYear = selectedYearRaw ? Number(selectedYearRaw) : null;
    const currentYear = Number.isFinite(selectedYear) ? selectedYear : Number(rows[rows.length - 1].year);
    const maxArea = Math.max(...rows.map((r) => Number(r.problem_area_ha || 0)), 1);

    chart.innerHTML = "";
    rows.forEach((row) => {
        const riskPercent = Number(row.mean_risk_score || 0) * 100;
        const width = (Number(row.problem_area_ha || 0) / maxArea) * 100;
        const band = riskBandClass(riskPercent);
        const isCurrent = Number(row.year) === Number(currentYear);
        const item = document.createElement("div");
        item.className = `year-row-v2 ${isCurrent ? "current" : ""}`;
        item.innerHTML = `
      <div class="year-row-head">
        <span class="year-name">${isCurrent ? '<i class="current-dot"></i>' : ""}${row.year}</span>
        <span class="year-risk">${formatPercent(row.mean_risk_score, 1)}</span>
      </div>
      <div class="year-bar-track">
        <div class="year-bar-fill ${band}" style="width:${Math.max(width, 2)}%;"></div>
      </div>
      <div class="year-ha">${formatNumber(row.problem_area_ha, 2)} га</div>
    `;
        chart.appendChild(item);
    });
}

function setProblemTrend(summary) {
    const trendEl = byId("problemAreaTrend");
    if (!trendEl) return;
    const rows = (summary.yearly_dynamics || []).slice().sort((a, b) => Number(a.year) - Number(b.year));
    trendEl.classList.remove("down");
    if (rows.length < 2) {
        trendEl.textContent = "↑ +0.0% к 2024";
        return;
    }

    const last = Number(rows[rows.length - 1].problem_area_ha || 0);
    const prev = Number(rows[rows.length - 2].problem_area_ha || 0);
    const prevYear = rows[rows.length - 2].year;
    const delta = prev > 0 ? ((last - prev) / prev) * 100 : 0;
    const arrow = delta >= 0 ? "↑" : "↓";
    const sign = delta >= 0 ? "+" : "-";
    if (delta < 0) trendEl.classList.add("down");
    trendEl.textContent = `${arrow} ${sign}${Math.abs(delta).toFixed(1)}% к ${prevYear}`;
}

function setStats(summary) {
    byId("problemAreaValue").textContent = `${formatNumber(summary.total_problem_area_ha, 2)} га`;
    byId("objectsCountValue").textContent = `${summary.objects_count || 0}`;
    byId("meanRiskValue").textContent = formatPercent(summary.mean_risk_score, 1);
    setProblemTrend(summary);
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
    const showRisk = byId("toggleRisk")?.checked !== false;
    const showBoundaries = byId("toggleBoundaries")?.checked !== false;
    const showTooltips = byId("toggleTooltips")?.checked !== false;
    const showOutline = byId("toggleOutline")?.checked !== false;

    if (territory) params.set("territory", territory);
    if (year) params.set("year", year);
    if (risk) params.set("risk_level", risk);
    if (feature) params.set("feature_type", feature);

    const data = await api.get(`/map/layers?${params.toString()}`);
    if (reqId !== state.layersReqId) return;
    if (data.analysis_id) state.analysisId = data.analysis_id;
    setAnalysisMeta();

    if (state.boundariesLayer) state.map.removeLayer(state.boundariesLayer);
    if (state.detectionLayer) state.map.removeLayer(state.detectionLayer);

    state.boundariesLayer = null;
    state.detectionLayer = null;

    if (showBoundaries) {
        state.boundariesLayer = L.geoJSON(data.boundaries, {style: styleBoundary}).addTo(state.map);
    }

    if (showRisk) {
        state.detectionLayer = L.geoJSON(data, {
            style: (featureItem) => styleDetection(featureItem, showOutline),
            onEachFeature: (featureItem, layer) => {
                layer.on("click", () => renderFeatureCard(featureItem.properties || {}));
                if (showTooltips) {
                    const riskLabel = riskLabels[featureItem.properties?.risk_level] || featureItem.properties?.risk_level || "";
                    layer.bindTooltip(`${featureItem.properties?.parcel_id || "Участок"} • ${riskLabel}`);
                }
            },
        }).addTo(state.map);
    }

    if (!data.features || data.features.length === 0 || !showRisk) {
        renderFeaturePlaceholder();
    }

    focusMapByTerritory(data.boundaries, territory, state.detectionLayer);
}

async function refreshAll() {
    await loadFilterOptions();
    await loadLayers();
    await loadSummary();
}

async function waitForCompletedAnalysis(timeoutMs = 15 * 60 * 1000, pollMs = 5000) {
    const started = Date.now();
    while (Date.now() - started < timeoutMs) {
        const analyses = await api.get("/analysis/results");
        const completed = (analyses.items || [])
            .filter((row) => String(row.status) === "completed")
            .sort((a, b) => Number(b.id || 0) - Number(a.id || 0))[0];
        if (completed?.id) {
            state.analysisId = completed.id;
            return true;
        }
        await sleep(pollMs);
    }
    return false;
}

function bindEvents() {
    byId("refreshBtn")?.addEventListener("click", async () => {
        try {
            const started = performance.now();
            setStatus("Обновление слоёв...");
            await refreshAll();
            setStatus("Готово", false, (performance.now() - started) / 1000);
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
                const started = performance.now();
                await loadLayers();
                await loadSummary();
                setStatus("Готово", false, (performance.now() - started) / 1000);
            } catch (e) {
                setStatus(e.message || String(e), true);
            }
        });
    });

    ["toggleRisk", "toggleBoundaries", "toggleTooltips", "toggleOutline"].forEach((id) => {
        byId(id)?.addEventListener("change", async () => {
            try {
                const started = performance.now();
                await loadLayers();
                setStatus("Готово", false, (performance.now() - started) / 1000);
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
    renderFeaturePlaceholder();
    try {
        setStatus("Загрузка данных...");
        state.analysisId = null;
        const analyses = await api.get("/analysis/results");
        const hasCompleted = (analyses.items || []).some((row) => String(row.status) === "completed");

        if (!hasCompleted) {
            const runningRows = (analyses.items || []).filter((row) => String(row.status) === "running");
            const latestRunning = runningRows.sort((a, b) => Number(b.id || 0) - Number(a.id || 0))[0];
            if (latestRunning?.id) {
                setStatus("Ожидание завершения текущего анализа...");
                const ok = await waitForCompletedAnalysis(6 * 60 * 1000, 5000);
                if (ok) {
                    const started = performance.now();
                    await refreshAll();
                    setStatus("Готово", false, (performance.now() - started) / 1000);
                    return;
                }
            }
            setStatus("Новая база: выполняется первичный анализ...");
            await api.postJson("/analysis/run", {
                territories: null,
                years: null,
                force_retrain: false,
            });
            const ok = await waitForCompletedAnalysis(6 * 60 * 1000, 4000);
            if (!ok) {
                setStatus("Анализ запущен, дождитесь завершения и обновите страницу", true);
                return;
            }
        }

        const started = performance.now();
        await refreshAll();
        setStatus("Готово", false, (performance.now() - started) / 1000);
    } catch (e) {
        setStatus(e.message || String(e), true);
    }
}

window.addEventListener("DOMContentLoaded", initPage);
