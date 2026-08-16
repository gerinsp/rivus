// Maintenance-specific presentation for the existing job-details panel.
// The main dashboard keeps rendering durable maintenance state; this file only
// adds worker/executor context and scopes the history link to the current job.
(() => {
  if (typeof renderIcebergMaintenance !== 'function') return;

  const baseRenderIcebergMaintenance = renderIcebergMaintenance;

  function maintenanceConfig(job) {
    return job?.config?.sink?.config?.table_maintenance || {};
  }

  function maintenanceExecutor(job, enabled) {
    if (!enabled) return '-';
    const value = String(maintenanceConfig(job)?.executor || '').trim().toLowerCase();
    return value === 'native' || value === 'spark' || value === 'hybrid' ? value : 'hybrid';
  }

  function replaceExactText(panel, before, after) {
    panel.querySelectorAll('div, p').forEach((el) => {
      if (el.children.length === 0 && String(el.textContent || '').trim() === before) {
        el.textContent = after;
      }
    });
  }

  function addWorkerConfig(panel, job) {
    panel.querySelector('[data-maintenance-worker-config]')?.remove();

    const runtime = job?.iceberg_maintenance;
    const cfg = maintenanceConfig(job);
    const enabled = typeof cfg.enabled === 'boolean' ? cfg.enabled : !!runtime?.enabled;
    const executor = maintenanceExecutor(job, enabled);
    const strip = document.createElement('div');
    strip.dataset.maintenanceWorkerConfig = 'true';
    strip.className = 'grid gap-3 border-b border-slate-200 bg-slate-50 px-5 py-4 sm:grid-cols-3 sm:px-6';
    strip.innerHTML = [
      maintenanceMetric('Automatic maintenance', enabled ? 'Enabled' : 'Disabled', enabled ? 'Durable worker scheduling is active' : 'No automatic tasks are scheduled'),
      maintenanceMetric('Scheduler', 'Maintenance Worker', 'CDC only emits lightweight maintenance signals'),
      maintenanceMetric('Executor', executor === '-' ? '-' : executor.charAt(0).toUpperCase() + executor.slice(1), 'Compaction policy; cleanup operations stay native'),
    ].join('');

    const header = panel.querySelector('a[href*="/ui/admin/iceberg-maintenance/runs"]')?.closest('.border-b');
    if (header) {
      header.insertAdjacentElement('afterend', strip);
    } else {
      panel.prepend(strip);
    }
  }

  function scopeHistoryLink(panel, job) {
    const link = panel.querySelector('a[href*="/ui/admin/iceberg-maintenance/runs"]');
    const jobID = String(job?.id || '').trim();
    if (!link || !jobID) return;
    link.href = `/ui/admin/iceberg-maintenance/runs/?job_id=${encodeURIComponent(jobID)}`;
    link.textContent = "View this job's maintenance runs";
  }

  renderIcebergMaintenance = function renderIcebergMaintenanceWithWorkerContext(job) {
    baseRenderIcebergMaintenance(job);

    const panel = document.getElementById('icebergMaintenancePanel');
    if (!panel || panel.classList.contains('hidden')) return;

    addWorkerConfig(panel, job);
    scopeHistoryLink(panel, job);

    replaceExactText(
      panel,
      'File inventory will appear after the Iceberg sink starts.',
      'Durable maintenance-worker state will appear after the Iceberg sink starts.'
    );
    replaceExactText(
      panel,
      'Automatic maintenance monitoring is disabled for this job.',
      'Automatic maintenance is disabled for this job. The maintenance worker will not schedule tasks.'
    );
    replaceExactText(
      panel,
      'Current active file inventory and automatic-compaction progress.',
      'Durable maintenance-worker state, active Iceberg inventory, and automatic maintenance progress.'
    );
    replaceExactText(
      panel,
      'Initial snapshot is still running. Automatic submissions are paused; file counts refresh after the snapshot completes.',
      'Initial snapshot is still running. The maintenance worker waits for the snapshot barrier; file counts refresh after the snapshot completes.'
    );
  };
})();
