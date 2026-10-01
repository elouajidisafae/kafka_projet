const CLUSTER_COLORS = {
  'dev':        '#1D9E75',
  'staging':    '#BA7517',
  'production': '#E24B4A',
};

const fmt = (n) => n >= 0
  ? Math.round(n).toLocaleString('en-GB')
  : 'N/A';

let timelineChart = null;

async function loadStats() {
  const res  = await fetch('/api/stats');
  const data = await res.json();

  renderClusterSummary(data.cluster_summary || []);
  renderTimeline(data.lag_timeline || []);
  renderTopTopics(data.top_topics || []);
  renderTopGroups(data.top_groups || []);
  renderMeta(data.meta || {});

  document.getElementById('last-update').textContent =
    'Updated: ' + new Date().toLocaleTimeString('en-GB');
}

function renderClusterSummary(clusters) {
  const container = document.getElementById('cluster-summary');
  container.innerHTML = `<div class="cluster-summary-grid">` +
    clusters.map(c => {
      const cls = c.nb_critical > 0 ? 'has-critical'
                : c.nb_warning  > 0 ? 'has-warning'
                : 'all-ok';
      return `
        <div class="cluster-stat-card ${cls}">
          <div class="cluster-stat-name">${c.cluster_name}</div>
          <div class="cluster-stat-metrics">
            <div class="stat-metric">
              <span class="val total">${fmt(c.total_lag)}</span>
              <span class="lbl">Total lag</span>
            </div>
            <div class="stat-metric">
              <span class="val crit">${c.nb_critical}</span>
              <span class="lbl">Critical</span>
            </div>
            <div class="stat-metric">
              <span class="val warn">${c.nb_warning}</span>
              <span class="lbl">Warning</span>
            </div>
            <div class="stat-metric">
              <span class="val ok">${c.nb_ok}</span>
              <span class="lbl">OK</span>
            </div>
            <div class="stat-metric">
              <span class="val total">${fmt(c.max_lag)}</span>
              <span class="lbl">Max lag</span>
            </div>
            <div class="stat-metric">
              <span class="val total">${fmt(c.avg_lag)}</span>
              <span class="lbl">Average lag</span>
            </div>
          </div>
        </div>`;
    }).join('') + `</div>`;
}

function renderTimeline(points) {
  const clusterMap = {};
  const buckets    = new Set();

  for (const p of points) {
    if (!clusterMap[p.cluster_name]) clusterMap[p.cluster_name] = {};
    clusterMap[p.cluster_name][p.bucket] = p.total_lag;
    buckets.add(p.bucket);
  }

  const labels   = [...buckets].sort();
  const datasets = Object.entries(clusterMap).map(([name, vals]) => ({
    label:           name,
    data:            labels.map(b => vals[b] ?? null),
    borderColor:     CLUSTER_COLORS[name] || 'var(--primary)',
    backgroundColor: (CLUSTER_COLORS[name] || 'var(--primary)') + '22',
    fill:            true,
    tension:         0.3,
    pointRadius:     1,
    spanGaps:        true,
  }));

  if (timelineChart) timelineChart.destroy();
  timelineChart = new Chart(
    document.getElementById('timeline-chart'), {
      type: 'line',
      data: { labels, datasets },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { labels: { color: getComputedStyle(document.documentElement).getPropertyValue('--text-muted').trim() || '#94a3b8', boxWidth: 12 } }
        },
        scales: {
          x: {
            ticks: { color: getComputedStyle(document.documentElement).getPropertyValue('--text-muted').trim() || '#64748b', maxTicksLimit: 12, font: { size: 10 } },
            grid:  { color: getComputedStyle(document.documentElement).getPropertyValue('--border').trim() + '30' || '#1e223530' }
          },
          y: {
            ticks: { color: getComputedStyle(document.documentElement).getPropertyValue('--text-muted').trim() || '#64748b', font: { size: 10 } },
            grid:  { color: getComputedStyle(document.documentElement).getPropertyValue('--border').trim() + '30' || '#1e223530' },
            beginAtZero: true,
          }
        }
      }
    }
  );
}

function renderTopTopics(topics) {
  const maxLag = topics[0]?.total_lag || 1;
  document.getElementById('top-topics').innerHTML = topics.map((t, i) => `
    <div class="top-row">
      <span class="top-rank">#${i + 1}</span>
      <div style="flex:1">
        <div class="top-name">${t.topic}</div>
        <div class="top-sub">${t.nb_groups} group(s)</div>
        <div class="lag-bar-wrap" style="margin-top:4px">
          <div class="lag-bar" style="width:${Math.round(t.total_lag / maxLag * 100)}%"></div>
        </div>
      </div>
      <span class="top-lag">${fmt(t.total_lag)}</span>
    </div>
  `).join('');
}

function renderTopGroups(groups) {
  const maxLag = groups[0]?.total_lag || 1;
  const statusColor = { CRITICAL: '#f87171', WARNING: '#facc15', OK: '#22c55e' };

  document.getElementById('top-groups').innerHTML = groups.map((g, i) => `
    <div class="top-row">
      <span class="top-rank">#${i + 1}</span>
      <div style="flex:1">
        <div class="top-name">${g.group_id}</div>
        <div class="top-sub">
          <span style="color:#7dd3fc">${g.cluster_name}</span>
          — ${g.nb_topics} topic(s)
          — <span class="badge ${g.worst_status.toLowerCase()}">${g.worst_status}</span>
        </div>
        <div class="lag-bar-wrap" style="margin-top:4px">
          <div class="lag-bar" style="width:${Math.round(g.total_lag / maxLag * 100)}%"></div>
        </div>
      </div>
      <span class="top-lag">${fmt(g.total_lag)}</span>
    </div>
  `).join('');
}

function renderMeta(meta) {
  const fmt_ts = (ts) => ts ? ts.slice(0, 19).replace('T', ' ') + ' UTC' : 'N/A';
  document.getElementById('meta-info').innerHTML = `
    <div class="meta-grid">
      <div class="meta-item">
        <div class="lbl">Total SQLite records</div>
        <div class="val">${(meta.total_records || 0).toLocaleString('en-GB')}</div>
      </div>
      <div class="meta-item">
        <div class="lbl">Earliest measurement</div>
        <div class="val">${fmt_ts(meta.oldest_record)}</div>
      </div>
      <div class="meta-item">
        <div class="lbl">Latest measurement</div>
        <div class="val">${fmt_ts(meta.newest_record)}</div>
      </div>
      <div class="meta-item">
        <div class="lbl">Generated at</div>
        <div class="val">${fmt_ts(meta.generated_at)}</div>
      </div>
    </div>
  `;
}

loadStats();
setInterval(loadStats, 30000);