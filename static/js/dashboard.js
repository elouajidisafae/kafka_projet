let chart = null;
let forecastChart = null;
let displayedForecasts = [];
let showForecastRange = false;

function intervalLabel(f) {
  if (!showForecastRange || f.method !== 'multiwindow') return '';
  if (f.eta_low_sec == null) return 'Range unavailable';
  const low = (f.eta_low_sec / 60).toFixed(1);
  return f.eta_high_sec == null ? `Range ${low} min or later`
    : `Range ${low}–${(f.eta_high_sec / 60).toFixed(1)} min`;
}
let activeCluster = null;
let THRESHOLDS = { warning: 1000, critical: 10000 };

// ── Helpers ──────────────────────────────────────────────────────────────────

const badge = (status) => {
  const map = {
    OK:       ['ok',   '● OK'],
    WARNING:  ['warn', '▲ WARNING'],
    CRITICAL: ['crit', '✖ CRITICAL'],
    ERROR:    ['err',  '? ERROR'],
  };
  const [cls, label] = map[status] || ['err', status];
  return `<span class="badge ${cls}">${label}</span>`;
};
const stateBadge = (state) => {
  const map = {
    'Stable':              ['state-stable', 'Stable'],
    'Empty':               ['state-empty',  'Empty'],
    'Dead':                ['state-dead',   'Dead'],
    'PreparingRebalance':  ['state-rebal',  'Rebalancing'],
    'CompletingRebalance': ['state-rebal',  'Completing'],
  };
  const [cls, label] = map[state] || ['state-unknown', state || 'Unknown'];
  return `<span class="state-badge ${cls}">${label}</span>`;
};

const fmt = (n) => n >= 0 ? Number(n).toLocaleString('en-GB') : 'N/A';

const trendIcon = (t) =>
  t === 'INCREASING' ? '<span class="trend-up">↑ increasing</span>'
  : t === 'DECREASING' ? '<span class="trend-down">↓ decreasing</span>'
  : '<span class="trend-stab">→ stable</span>';

const confBadge = (c, r2) => {
  const cls = c === 'HIGH' ? 'conf-high' : c === 'MEDIUM' ? 'conf-med' : 'conf-low';
  return `<span class="${cls}">R²=${r2} (${c})</span>`;
};

const etaBadge = (etaMin, label) => {
  if (etaMin === -1) return `<span class="eta-badge eta-crit">Already ${label}</span>`;
  if (etaMin === -2) return `<span class="eta-badge eta-ok">Not projected to reach threshold</span>`;
  if (etaMin < 10)  return `<span class="eta-badge eta-crit">${label} ~${etaMin} min</span>`;
  if (etaMin < 30)  return `<span class="eta-badge eta-warn">${label} ~${etaMin} min</span>`;
  return `<span class="eta-badge eta-ok">${label} ~${etaMin} min</span>`;
};

// ── Tabs ──────────────────────────────────────────────────────────────────────

function renderTabs(clusters) {
  const container = document.getElementById('tabs');
  const all = ['all', ...clusters];
  container.innerHTML = all.map(c => `
    <div class="tab ${c === (activeCluster || 'all') ? 'active' : ''}"
         onclick="selectCluster('${c}')">
      ${c === 'all' ? 'All clusters' : c}
    </div>
  `).join('');
}

function selectCluster(name) {
  activeCluster = name === 'all' ? null : name;
  document.querySelectorAll('.tab').forEach(t => {
    t.classList.toggle('active',
      t.textContent.trim() === (name === 'all' ? 'All clusters' : name)
    );
  });
  refresh();
}

// ── Status refresh ────────────────────────────────────────────────────────────

async function refresh() {
  const url = activeCluster
    ? `/api/status?cluster=${activeCluster}`
    : '/api/status';
  const res  = await fetch(url);
  const json = await res.json();
  const rows = json.data || [];

  if (json.thresholds) {
    THRESHOLDS.warning = json.thresholds.warning;
    THRESHOLDS.critical = json.thresholds.critical;
  }

  renderTabs(json.clusters || []);

  document.getElementById('c-total').textContent = rows.length;
  document.getElementById('c-ok').textContent    = rows.filter(r => r.status === 'OK').length;
  document.getElementById('c-warn').textContent  = rows.filter(r => r.status === 'WARNING').length;
  document.getElementById('c-crit').textContent  = rows.filter(r => r.status === 'CRITICAL').length;

    document.getElementById('tbody').innerHTML = rows.map(r => `
        <tr onclick="loadChart('${r.cluster_name}','${r.group_id}','${r.topic}')">
        <td><span class="cluster-pill">${r.cluster_name}</span></td>
        <td><strong>${r.group_id}</strong></td>
        <td>${r.topic}</td>
        <td style="text-align:right;font-variant-numeric:tabular-nums">${fmt(r.total_lag)}</td>
        <td>${badge(r.status)}</td>
        <td>${stateBadge(r.group_state || 'Unknown')}</td>
        <td style="color:#475569;font-size:.75rem">
            ${r.recorded_at?.slice(0,19).replace('T',' ') || '—'}
        </td>
        </tr>
    `).join('');

  document.getElementById('last-update').innerHTML =
    '<span id="refresh-dot"></span>Updated: '
    + new Date().toLocaleTimeString('en-GB');
}

// ── Historique chart ──────────────────────────────────────────────────────────

async function loadChart(cluster, group, topic) {
  document.getElementById('chart-title').textContent =
    `${cluster} — ${group} / ${topic}`;
  document.getElementById('chart-placeholder').style.display = 'none';

  const canvas = document.getElementById('lag-chart');
  canvas.style.display = 'block';

  const res  = await fetch(
    `/api/history?cluster=${encodeURIComponent(cluster)}`
    + `&group=${encodeURIComponent(group)}`
    + `&topic=${encodeURIComponent(topic)}&hours=2`
  );
  const json = await res.json();
  const pts  = json.points || [];

  if (chart) chart.destroy();
  chart = new Chart(canvas, {
    type: 'line',
    data: {
      labels: pts.map(p => p.recorded_at.slice(11, 19)),
      datasets: [{
        label: 'Total lag',
        data: pts.map(p => p.total_lag),
        borderColor: '#7dd3fc',
        backgroundColor: '#7dd3fc22',
        fill: true, tension: 0.3, pointRadius: 2,
      }]
    },
    options: {
      responsive: true,
      plugins: { legend: { labels: { color: '#94a3b8' } } },
      scales: {
        x: { ticks: { color: '#475569' }, grid: { color: '#1e2235' } },
        y: { ticks: { color: '#475569' }, grid: { color: '#1e2235' }, beginAtZero: true },
      }
    }
  });
}

// ── Forecast table ────────────────────────────────────────────────────────────

async function refreshForecast() {
  const res       = await fetch('/api/forecast');
  const json      = await res.json();
  const forecasts = json.forecasts || [];
  displayedForecasts = forecasts;
  showForecastRange = json.show_range === true;
  const container = document.getElementById('forecast-table');

  if (!forecasts.length) {
    container.innerHTML = '<div class="placeholder-text">No forecasts available</div>';
    return;
  }

  const rows = forecasts.map(f => {
    if (!f.enough_data) {
      return `<div class="forecast-row">
        <span class="cluster-pill">${f.cluster_name || '—'}</span>
        <span style="color:#64748b">${f.group_id || '—'} / ${f.topic || '—'}</span>
        <span style="color:#475569;font-size:.75rem">Insufficient data</span>
      </div>`;
    }
    return `<div class="forecast-row" style="cursor:pointer"
      onclick="loadForecastChart('${f.cluster_name}','${f.group_id}','${f.topic}',${f.slope_per_min},${f.current_lag})">
      <span class="cluster-pill">${f.cluster_name}</span>
      <span style="min-width:160px"><strong>${f.group_id}</strong> / ${f.topic}</span>
      <span style="min-width:80px">lag: <strong>${fmt(f.current_lag)}</strong></span>
      <span style="min-width:120px">
        ${trendIcon(f.trend)}
        (${f.slope_per_min > 0 ? '+' : ''}${f.slope_per_min}/min)
      </span>
      <span>${etaBadge(f.eta_warning_min, 'WARNING')}</span>
      <span>${etaBadge(f.eta_critical_min, 'CRITICAL')}<small style="display:block">${intervalLabel(f)}</small></span>
      <span style="color:#475569">→ 5min: ${fmt(f.predicted_lag_5min)}</span>
      <span style="color:#475569">→ 15min: ${fmt(f.predicted_lag_15min)}</span>
      <span>${confBadge(f.confidence, f.r_squared)}</span>
    </div>`;
  });

  container.innerHTML = `
    <div class="forecast-header">
      <span style="min-width:60px">Cluster</span>
      <span style="min-width:160px">Group / Topic</span>
      <span style="min-width:80px">Current lag</span>
      <span style="min-width:120px">Trend</span>
      <span>ETA Warning</span>
      <span>ETA Critical</span>
      <span>+5 min</span>
      <span>+15 min</span>
      <span>Confidence</span>
    </div>
    ${rows.join('')}
  `;
}

async function refreshHealthScore() {
  const res  = await fetch('/api/health-score');
  const json = await res.json();

  if (!json || json.score === null) return;

  const score = json.score;
  const color = json.color || '#22c55e';

  document.getElementById('score-value').textContent = score;
  document.getElementById('score-value').style.color = color;
  document.getElementById('score-grade').textContent = ({Bon: 'Good', Moyen: 'Degraded', Mauvais: 'Poor', Critique: 'Critical', 'Aucune donnee': 'No data', 'Chargement...': 'Loading...'})[json.grade] || json.grade || '—';
  document.getElementById('score-grade').style.color = color;

  const d = json.details || {};
  document.getElementById('score-detail').textContent =
    `${json.n_critical} critical  •  ${json.n_warning} warning  •  ${json.n_ok} ok`;

  const circumference = 188.5;
  const offset = circumference - (score / 100) * circumference;
  const ring = document.getElementById('score-ring');
  if (ring) {
    ring.style.stroke = color;
    ring.style.strokeDashoffset = offset;
    ring.style.transition = 'stroke-dashoffset 0.8s ease, stroke 0.3s';
  }

  const clusterDiv = document.getElementById('cluster-scores');
  if (clusterDiv && json.by_cluster) {
    clusterDiv.innerHTML = json.by_cluster.map(c => `
      <div style="text-align:center;min-width:80px">
        <div style="font-size:1.4rem;font-weight:700;color:${_clusterScoreColor(c.score)}">${c.score}</div>
        <div style="font-size:.7rem;color:#64748b;text-transform:uppercase;
                    letter-spacing:.5px">${c.cluster_name}</div>
        <div style="font-size:.7rem;color:#475569">${c.n_critical}C ${c.n_warning}W ${c.n_ok}OK</div>
      </div>
    `).join('<div style="width:1px;background:#2d3148"></div>');
  }
}

function _clusterScoreColor(score) {
  if (score >= 90) return '#22c55e';
  if (score >= 70) return '#4ade80';
  if (score >= 50) return '#facc15';
  if (score >= 30) return '#fb923c';
  return '#f87171';
}

// ── Forecast chart ────────────────────────────────────────────────────────────

async function loadForecastChart(cluster, group, topic, slopePerMin, currentLag) {
  const selected = displayedForecasts.find(f => f.cluster_name === cluster && f.group_id === group && f.topic === topic);
  const prediction = selected?.prediction_band || [];
  const band = showForecastRange ? prediction : [];
  document.getElementById('forecast-chart-section').style.display = 'block';
  document.getElementById('forecast-chart-title').textContent =
    `Forecast — ${cluster} / ${group} / ${topic}`;
  document.getElementById('forecast-chart-placeholder').style.display = 'none';

  const res  = await fetch(
    `/api/history?cluster=${encodeURIComponent(cluster)}`
    + `&group=${encodeURIComponent(group)}`
    + `&topic=${encodeURIComponent(topic)}&hours=1`
  );
  const json = await res.json();
  const pts  = json.points || [];
  if (!pts.length) return;

  const WARNING  = THRESHOLDS.warning;
  const CRITICAL = THRESHOLDS.critical;

  const obsLabels   = pts.map(p => p.recorded_at.slice(11, 19));
  const obsData     = pts.map(p => p.total_lag);
  const slopePerSec = slopePerMin / 60;
  const lastLag     = obsData[obsData.length - 1];
  const predLabels  = [];
  const predData    = [];

  for (let i = 1; i <= 18; i++) {
    const seconds = i * 50;
    predLabels.push(`+${Math.round(seconds / 60)}min`);
    predData.push(prediction.length ? prediction[i].mean : Math.round(lastLag + slopePerSec * seconds));
  }

  const allLabels = [...obsLabels, ...predLabels];
  const allObs    = [...obsData,   ...Array(predLabels.length).fill(null)];
  const allPred   = [...Array(obsLabels.length - 1).fill(null), prediction.length ? prediction[0].mean : lastLag, ...predData];
  const maxY      = Math.max(...obsData, ...predData, ...band.map(p => p.upper), CRITICAL) * 1.05;
  const bands = band.length ? [
    {label: '90% prediction lower bound', data: [...Array(obsLabels.length - 1).fill(null), ...band.map(p => p.lower)],
     borderWidth: 0, pointRadius: 0, fill: false},
    {label: '90% prediction band (nominal)', data: [...Array(obsLabels.length - 1).fill(null), ...band.map(p => p.upper)],
     borderWidth: 0, pointRadius: 0, backgroundColor: '#EF9F2733', fill: '-1'},
  ] : [];

  if (forecastChart) forecastChart.destroy();
  forecastChart = new Chart(document.getElementById('forecast-canvas'), {
    type: 'line',
    data: {
      labels: allLabels,
      datasets: [
        ...bands,
        {
          label: 'Observed lag',
          data: allObs,
          borderColor: '#378ADD',
          backgroundColor: '#378ADD22',
          fill: true, tension: 0.3, pointRadius: 2, spanGaps: false,
        },
        {
          label: 'Forecast',
          data: allPred,
          borderColor: '#EF9F27',
          borderDash: [6, 3],
          backgroundColor: '#EF9F2711',
          fill: true, tension: 0.2, pointRadius: 2, spanGaps: false,
        },
        {
          label: 'WARNING',
          data: allLabels.map(() => WARNING),
          borderColor: '#BA7517',
          borderWidth: 1.5, borderDash: [4, 4], pointRadius: 0, fill: false,
        },
        {
          label: 'CRITICAL',
          data: allLabels.map(() => CRITICAL),
          borderColor: '#E24B4A',
          borderWidth: 1.5, borderDash: [4, 4], pointRadius: 0, fill: false,
        },
      ]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: { legend: { display: band.length > 0 } },
      scales: {
        x: {
          ticks: { color: '#64748b', maxTicksLimit: 10, font: { size: 11 } },
          grid:  { color: '#1e223530' }
        },
        y: {
          min: 0,
          max: Math.round(maxY),
          ticks: { color: '#64748b', font: { size: 11 } },
          grid:  { color: '#1e223530' }
        }
      },
      animation: { duration: 400 }
    }
  });
}

async function refreshRecommendations() {
  const container = document.getElementById('recommendations-container');
  const btn = document.querySelector('button[onclick="refreshRecommendations()"]');
  
  if (btn) {
    btn.disabled = true;
    btn.textContent = 'Analyzing...';
    btn.style.opacity = '0.7';
  }

  try {
    const res  = await fetch('/api/recommendations');
    const json = await res.json();
    const recs = json.recommendations || [];

    if (!recs.length) {
      container.innerHTML = '<div class="placeholder-text">Clusters are healthy. No urgent recommendations.</div>';
      return;
    }

    container.innerHTML = recs.map(r => `
      <div class="suggestion-card severity-${({HIGH: 'critical', MEDIUM: 'warning', LOW: 'info'})[r.priority]}">
        <div class="suggestion-header">
          <div class="suggestion-title">${r.title}</div>
          <div class="suggestion-tag">${r.priority} | ${r.type}</div>
        </div>
        <div class="suggestion-body">
          ${r.advice}
        </div>
        <div class="suggestion-action">
          <strong>Action:</strong> ${r.action}
        </div>
        <div style="margin-top:10px; font-size:0.65rem; color:#475569">
          ${r.cluster_name} / ${r.group_id} / ${r.topic}
        </div>
        <pre class="suggestion-metrics">${JSON.stringify(r.metrics, null, 2)}</pre>
      </div>
    `).join('');
  } catch (err) {
    container.innerHTML = '<div class="placeholder-text" style="color:#f87171">Error analyzing cluster state.</div>';
  } finally {
    if (btn) {
      btn.disabled = false;
      btn.textContent = 'Analyze Now';
      btn.style.opacity = '1';
    }
  }
}

// ── Init ─────────────────────────────────────────────────────────────────────

refresh();
refreshForecast();
refreshHealthScore();
refreshRecommendations();
setInterval(refresh,           REFRESH_INTERVAL);
setInterval(refreshForecast,   FORECAST_INTERVAL);
setInterval(refreshHealthScore, 10000);
