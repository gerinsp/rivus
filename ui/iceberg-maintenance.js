import {
  escapeHtml,
  fmtBytes,
  fmtWholeNumber,
  formatDateTime,
  setInnerHTMLIfChanged,
} from './core/api.js';

function maintenanceConfig(job) {
  return job?.config?.sink?.config?.table_maintenance || {};
}

function maintenanceEnabled(job) {
  const cfg = maintenanceConfig(job);
  if (typeof cfg.enabled === 'boolean') return cfg.enabled;
  return !!job?.iceberg_maintenance?.enabled;
}

function maintenanceExecutor(job, enabled) {
  if (!enabled) return '-';
  const value = String(maintenanceConfig(job)?.executor || '').trim().toLowerCase();
  return value === 'native' || value === 'spark' || value === 'hybrid' ? value : 'hybrid';
}

function maintenanceStateLabel(state) {
  const labels = {
    waiting_for_snapshot: 'Waiting for snapshot',
    scanning: 'Scanning manifests',
    watching: 'Watching',
    accumulating: 'Collecting files',
    ready: 'Ready for maintenance',
    running: 'Maintenance running',
    healthy: 'Healthy',
    error: 'Inventory error',
    disabled: 'Disabled',
  };
  return labels[String(state || '').toLowerCase()] || String(state || 'Unknown');
}

function maintenanceStateClass(state) {
  switch (String(state || '').toLowerCase()) {
    case 'running':
      return 'border-blue-200 bg-blue-50 text-blue-700';
    case 'ready':
      return 'border-amber-200 bg-amber-50 text-amber-700';
    case 'error':
      return 'border-rose-200 bg-rose-50 text-rose-700';
    case 'healthy':
    case 'watching':
      return 'border-emerald-200 bg-emerald-50 text-emerald-700';
    default:
      return 'border-slate-200 bg-slate-50 text-slate-600';
  }
}

function maintenanceMetric(label, value, detail) {
  return `
    <div class="stat-card min-w-0">
      <div class="stat-label">${escapeHtml(label)}</div>
      <div class="mono mt-2 text-xl font-semibold text-slate-900">${escapeHtml(value)}</div>
      <div class="mt-1 text-xs leading-5 text-slate-500">${escapeHtml(detail || '')}</div>
    </div>
  `;
}

function workerConfigStrip(job, enabled) {
  const executor = maintenanceExecutor(job, enabled);
  const executorLabel = executor === '-' ? '-' : executor.charAt(0).toUpperCase() + executor.slice(1);
  return `
    <div class="grid gap-3 border-b border-slate-200 bg-slate-50 px-5 py-4 sm:grid-cols-3 sm:px-6">
      ${maintenanceMetric('Automatic maintenance', enabled ? 'Enabled' : 'Disabled', enabled ? 'Durable worker scheduling is active' : 'No automatic tasks are scheduled')}
      ${maintenanceMetric('Scheduler', 'Maintenance Worker', 'CDC only emits lightweight maintenance signals')}
      ${maintenanceMetric('Executor', executorLabel, 'Compaction policy; snapshot expiration and orphan cleanup stay native')}
    </div>
  `;
}

function historyLink(job) {
  const jobID = String(job?.id || '').trim();
  const href = jobID
    ? `/ui/admin/iceberg-maintenance/runs/?job_id=${encodeURIComponent(jobID)}`
    : '/ui/admin/iceberg-maintenance/runs/';
  const label = jobID ? "View this job's maintenance runs" : 'View maintenance runs';
  return `<a href="${href}" target="_blank" rel="noopener noreferrer" class="brand-outline-btn rounded-md px-3 py-1.5 text-xs font-semibold">${label}</a>`;
}

export function renderIcebergMaintenance(job) {
  const panel = document.getElementById('icebergMaintenancePanel');
  if (!panel) return;

  const sinkType = String(job?.sink_type || job?.config?.sink?.type || '').trim().toLowerCase();
  if (sinkType !== 'iceberg_native') {
    panel.classList.add('hidden');
    panel.innerHTML = '';
    return;
  }

  panel.classList.remove('hidden');
  const maintenance = job?.iceberg_maintenance;
  const enabled = maintenanceEnabled(job);

  if (!maintenance) {
    setInnerHTMLIfChanged(panel, `
      <div class="border-b border-slate-200 px-5 py-5 sm:px-6">
        <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Iceberg Maintenance</div>
        <div class="mt-2 text-sm text-slate-600">Durable maintenance-worker state will appear after the Iceberg sink starts.</div>
      </div>
      ${workerConfigStrip(job, enabled)}
    `);
    return;
  }

  if (!enabled) {
    setInnerHTMLIfChanged(panel, `
      <div class="flex flex-col gap-3 border-b border-slate-200 px-5 py-5 sm:flex-row sm:items-center sm:justify-between sm:px-6">
        <div>
          <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Iceberg Maintenance</div>
          <div class="mt-2 text-sm text-slate-600">Automatic maintenance is disabled for this job. The maintenance worker will not schedule tasks.</div>
        </div>
        <span class="rounded-full border px-3 py-1.5 text-xs font-semibold ${maintenanceStateClass('disabled')}">Disabled</span>
      </div>
      ${workerConfigStrip(job, false)}
    `);
    return;
  }

  const state = String(maintenance.state || 'watching');
  const tables = Array.isArray(maintenance.tables) ? [...maintenance.tables] : [];
  const stateOrder = { running: 0, ready: 1, error: 2, accumulating: 3, scanning: 4, waiting_for_snapshot: 5, healthy: 6 };
  tables.sort((left, right) => {
    const stateDelta = (stateOrder[left?.state] ?? 9) - (stateOrder[right?.state] ?? 9);
    if (stateDelta !== 0) return stateDelta;
    const fileDelta = Number(right?.eligible_small_files || 0) - Number(left?.eligible_small_files || 0);
    if (fileDelta !== 0) return fileDelta;
    return String(left?.identifier || '').localeCompare(String(right?.identifier || ''));
  });

  const dataThreshold = Number(maintenance.data_files_threshold || 0);
  const deleteThreshold = Number(maintenance.equality_delete_files_threshold || 0);
  const tableRows = tables.map((table) => {
    const operations = Array.isArray(table?.operations) && table.operations.length > 0
      ? `<div class="mt-1 text-[11px] text-blue-600">${escapeHtml(table.operations.join(', '))}</div>`
      : '';
    const error = table?.error
      ? `<div class="mt-1 max-w-xl break-words text-[11px] leading-4 text-rose-600">${escapeHtml(table.error)}</div>`
      : '';
    return `
      <tr class="border-b border-slate-100 align-top last:border-0">
        <td class="px-4 py-3">
          <div class="mono text-xs font-semibold text-slate-800">${escapeHtml(table?.identifier || '-')}</div>
          <div class="mt-1 text-[11px] text-slate-500">Checked ${escapeHtml(table?.checked_at ? formatDateTime(table.checked_at) : '-')}</div>
          ${error}
        </td>
        <td class="mono px-4 py-3 text-right text-xs font-semibold text-slate-800">${fmtWholeNumber(table?.active_data_files || 0)}</td>
        <td class="px-4 py-3 text-right">
          <div class="mono text-xs font-semibold text-slate-800">${fmtWholeNumber(table?.eligible_small_files || 0)}</div>
          <div class="mt-1 text-[11px] text-slate-500">${escapeHtml(fmtBytes(Number(table?.eligible_small_bytes || 0)))}</div>
        </td>
        <td class="mono px-4 py-3 text-right text-xs font-semibold text-slate-800">${fmtWholeNumber(table?.active_equality_delete_files || 0)}</td>
        <td class="px-4 py-3 text-xs text-slate-600">
          <div>Small: <span class="mono font-semibold text-slate-800">${fmtWholeNumber(table?.eligible_small_files || 0)} / ${fmtWholeNumber(dataThreshold)}</span></div>
          <div class="mt-1">Deletes: <span class="mono font-semibold text-slate-800">${fmtWholeNumber(table?.active_equality_delete_files || 0)} / ${fmtWholeNumber(deleteThreshold)}</span></div>
        </td>
        <td class="px-4 py-3">
          <span class="inline-flex rounded-full border px-2.5 py-1 text-[11px] font-semibold ${maintenanceStateClass(table?.state)}">${escapeHtml(maintenanceStateLabel(table?.state))}</span>
          ${operations}
        </td>
      </tr>
    `;
  }).join('');

  const checkedAt = maintenance.checked_at ? formatDateTime(maintenance.checked_at) : 'Waiting for first state update';
  const scanned = `${fmtWholeNumber(maintenance.tables_scanned || 0)} / ${fmtWholeNumber(maintenance.tables_total || 0)} tables tracked`;
  const errors = Number(maintenance.inventory_errors || 0);
  const inventoryNotice = state === 'waiting_for_snapshot'
    ? 'Initial snapshot is still running. The maintenance worker waits for the snapshot barrier; file counts refresh after the snapshot completes.'
    : errors > 0
      ? `${fmtWholeNumber(errors)} table inventory scan(s) failed. Other durable table state remains available below.`
      : 'Counts come from active files in the current Iceberg snapshot and durable maintenance metadata, not every object stored in S3.';
  const inventoryTone = errors > 0
    ? 'border-rose-200 bg-rose-50 text-rose-700'
    : state === 'waiting_for_snapshot'
      ? 'border-amber-200 bg-amber-50 text-amber-800'
      : 'border-slate-200 bg-slate-50 text-slate-600';

  setInnerHTMLIfChanged(panel, `
    <div class="border-b border-slate-200 px-5 py-4 sm:px-6">
      <div class="flex flex-col gap-3 lg:flex-row lg:items-start lg:justify-between">
        <div>
          <div class="text-xs font-semibold uppercase tracking-[0.18em] text-slate-500">Iceberg Maintenance</div>
          <div class="mt-2 text-sm text-slate-600">Durable maintenance-worker state, active Iceberg inventory, and automatic maintenance progress.</div>
        </div>
        <div class="flex flex-wrap items-center gap-2">
          <span class="rounded-full border px-3 py-1.5 text-xs font-semibold ${maintenanceStateClass(state)}">${escapeHtml(maintenanceStateLabel(state))}</span>
          <span class="rounded-full border border-slate-200 bg-white px-3 py-1.5 text-[11px] font-semibold text-slate-600">${escapeHtml(scanned)}</span>
          ${historyLink(job)}
        </div>
      </div>
      <div class="mt-3 text-xs text-slate-500">Last durable state update: <span class="font-semibold text-slate-700">${escapeHtml(checkedAt)}</span></div>
    </div>

    ${workerConfigStrip(job, true)}

    <div class="grid gap-3 bg-slate-50 px-5 py-4 sm:grid-cols-2 xl:grid-cols-5 sm:px-6">
      ${maintenanceMetric('Active data files', fmtWholeNumber(maintenance.active_data_files || 0), 'Currently referenced')}
      ${maintenanceMetric('Equality-delete files', fmtWholeNumber(maintenance.active_equality_delete_files || 0), 'Currently referenced')}
      ${maintenanceMetric('Eligible small files', fmtWholeNumber(maintenance.eligible_small_files || 0), `Below ${fmtBytes(Number(maintenance.small_file_size_bytes || 0))}`)}
      ${maintenanceMetric('Eligible small bytes', fmtBytes(Number(maintenance.eligible_small_bytes || 0)), `Minimum ${fmtBytes(Number(maintenance.small_files_min_total_bytes || 0))}`)}
      ${maintenanceMetric('Tables ready', fmtWholeNumber(maintenance.tables_ready || 0), `${fmtWholeNumber(maintenance.active_runs || 0)} worker lease(s) active`)}
    </div>

    <div class="mx-5 mt-4 rounded-[16px] border px-4 py-3 text-sm ${inventoryTone} sm:mx-6">${escapeHtml(inventoryNotice)}</div>

    <div class="px-5 pb-5 pt-4 sm:px-6 sm:pb-6">
      <div class="overflow-hidden rounded-[16px] border border-slate-200">
        <div class="max-h-[560px] overflow-auto">
          <table class="min-w-full text-sm">
            <thead class="sticky top-0 z-10 bg-slate-50 text-slate-600">
              <tr class="border-b border-slate-200">
                <th class="min-w-64 px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.14em]">Iceberg table</th>
                <th class="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.14em]">Data files</th>
                <th class="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.14em]">Small files</th>
                <th class="px-4 py-3 text-right text-[11px] font-semibold uppercase tracking-[0.14em]">Equality deletes</th>
                <th class="min-w-44 px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.14em]">Current / trigger</th>
                <th class="min-w-44 px-4 py-3 text-left text-[11px] font-semibold uppercase tracking-[0.14em]">State / last result</th>
              </tr>
            </thead>
            <tbody class="bg-white">
              ${tableRows || '<tr><td colspan="6" class="px-4 py-8 text-center text-sm text-slate-500">No durable Iceberg table state is available yet.</td></tr>'}
            </tbody>
          </table>
        </div>
      </div>
    </div>
  `);
}
