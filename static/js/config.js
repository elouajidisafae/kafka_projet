async function loadConfig() {
  const res  = await fetch('/api/config');
  const json = await res.json();

  document.getElementById('warning-threshold').value  = json.alerts.warning_threshold;
  document.getElementById('critical-threshold').value = json.alerts.critical_threshold;
  document.getElementById('refresh-interval').value   = json.monitor.refresh_interval;
  document.getElementById('retention-days').value     = json.monitor.history_retention_days;
  document.getElementById('exclude-topics').value     = (json.exclude_topics || []).join(', ');
  document.getElementById('exclude-groups').value     = (json.exclude_groups || []).join(', ');

  const clustersList = document.getElementById('clusters-list');
  clustersList.innerHTML = (json.clusters || []).map(c => `
    <div class="cluster-card">
      <span class="cluster-name">${c.name}</span>
      <span class="cluster-servers">${c.bootstrap_servers}</span>
      <span class="cluster-dot"></span>
    </div>
  `).join('');
}

async function saveConfig() {
  const statusEl = document.getElementById('save-status');
  statusEl.textContent = 'Saving...';
  statusEl.className = '';

  const warning  = parseInt(document.getElementById('warning-threshold').value);
  const critical = parseInt(document.getElementById('critical-threshold').value);

  if (warning >= critical) {
    statusEl.textContent = 'Error: the WARNING threshold must be lower than the CRITICAL threshold.';
    statusEl.className = 'status-error';
    return;
  }

  const payload = {
    alerts: {
      warning_threshold:  warning,
      critical_threshold: critical,
    },
    monitor: {
      refresh_interval:       parseInt(document.getElementById('refresh-interval').value),
      history_retention_days: parseInt(document.getElementById('retention-days').value),
    },
    exclude_topics: document.getElementById('exclude-topics').value
      .split(',').map(s => s.trim()).filter(Boolean),
    exclude_groups: document.getElementById('exclude-groups').value
      .split(',').map(s => s.trim()).filter(Boolean),
  };

  const res = await fetch('/api/config', {
    method: 'POST',
    headers: { 'Content-Type': 'application/json' },
    body: JSON.stringify(payload),
  });

  const json = await res.json();

  if (json.success) {
    statusEl.textContent = 'Configuration saved successfully.';
    statusEl.className = '';
    setTimeout(() => statusEl.textContent = '', 3000);
  } else {
    statusEl.textContent = 'Error: ' + (json.error || 'unknown error');
    statusEl.className = 'status-error';
  }
}

loadConfig();